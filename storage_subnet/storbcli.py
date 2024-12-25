import asyncio
import uuid
from functools import wraps

import typer
from rich.console import Console
from rich.table import Table

import storage_subnet.validator.db as db
from storage_subnet.constants import DB_DIR

cli = typer.Typer(name="Storb CLI")


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@cli.command()
@coro
async def create_key(
    balance: float | None = None,
    rate_limit_per_minute: int | None = None,
    name: str | None = None,
    db_dir: str = DB_DIR,
) -> str:
    """
    Create a new API key.

    This command creates a new API key with the given balance, rate limit per minute, and name.

    Arguments:
    balance: The initial balance of the API key.
    rate_limit_per_minute: The rate limit in requests per minute for the API key.
    name: The name for the API key.
    """

    if balance is None:
        balance = float(
            input("Please enter initial balance for the key (1 credit = 1 image): ")
        )

    if rate_limit_per_minute is None:
        rate_limit_per_minute = int(input("Please enter rate limit per minute: "))

    if name is None:
        name = input("Please enter a name for the API key: ")

    api_key = str(uuid.uuid4())
    async with db.get_db_connection(db_dir=db_dir) as conn:
        await db.add_api_key(conn, api_key, balance, rate_limit_per_minute, name)

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    table.add_column(db.KEY)
    table.add_column(db.BALANCE)
    table.add_column(db.RATE_LIMIT_PER_MINUTE)
    table.add_column(db.NAME)

    row = (api_key, balance, rate_limit_per_minute, name)
    table.add_row(*map(str, row))

    console.print(table)

    return api_key


@cli.command()
@coro
async def update_key(
    key: str,
    balance: float,
    rate_limit_per_minute: int,
    name: str,
    db_dir: str = DB_DIR,
) -> None:
    """
    Update an existing API Key.

    This command updates an existing API key identified by the 'key' with the given balance, rate limit per minute, and name.

    Arguments:
    key: The API key to update.
    balance (optional): The new balance of the API key.
    rate_limit_per_minute (optional): The new rate limit in requests per minute for the API key.
    name (optional): The new name for the API key.
    """
    async with db.get_db_connection(db_dir=db_dir) as conn:
        if balance is not None:
            await db.update_api_key_balance(conn, key, balance)
        if rate_limit_per_minute is not None:
            await db.update_api_key_rate_limit(conn, key, rate_limit_per_minute)
        if name is not None:
            await db.update_api_key_name(conn, key, name)


@cli.command()
@coro
async def delete_key(key: str, db_dir: str = DB_DIR) -> None:
    """
    Delete an existing API Key.

    This command deletes an existing API key identified by the 'key'.

    Arguments:
        key: The API key to delete.
    """
    async with db.get_db_connection(db_dir=db_dir) as conn:
        await db.delete_api_key(conn, key)


@cli.command()
@coro
async def list_keys(db_dir: str = DB_DIR) -> None:
    """
    List all API keys.
    """
    async with db.get_db_connection(db_dir=db_dir) as conn:
        rows = await db.get_all_api_keys(conn)
    console = Console()

    table = Table(show_header=True, header_style="bold magenta")
    try:
        keys = rows[0].keys()
    except IndexError:
        console.print("No keys table found - try adding a key")
        return
    for key in keys:
        table.add_column(str(key))

    for row in rows:
        table.add_row(*map(str, row))

    console.print(table)


@cli.command()
@coro
async def show_key_info(key: str, db_dir: str = DB_DIR) -> None:
    """
    Show information about an API key.
    """

    async with db.get_db_connection(db_dir=db_dir) as conn:
        row = await db.get_api_key_info(conn, key)

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    async with db.get_db_connection(db_dir=db_dir) as conn:
        row = await db.get_api_key_info(conn, key)

    if row:
        for column_name in row:
            table.add_column(column_name)

        table.add_row(*[str(value) for value in row.values()])
        console.print(table)


@cli.command()
@coro
async def logs_for_key(key: str, db_dir: str = DB_DIR) -> None:
    """
    Show all logs for an API key.
    """
    from rich.console import Console
    from rich.table import Table

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    async with db.get_db_connection(db_dir=db_dir) as conn:
        logs = await db.get_all_logs_for_key(conn, key)

    if logs:
        for column_name in logs[0]:
            table.add_column(column_name)

        for log in logs:
            log = dict(log)  # noqa: PLW2901
            table.add_row(*[str(value) for value in log.values()])

        console.print(table)
    else:
        print(f"No logs found for key: {key}")


@cli.command()
@coro
async def logs_summary(db_dir: str = DB_DIR) -> None:
    """
    Summary of all logs.
    """
    async with db.get_db_connection(db_dir=db_dir) as conn:
        keys = await db.get_all_api_keys(conn)

    console = Console()

    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("key")
    summary_table.add_column("Total Requests")
    summary_table.add_column("Total Credits Used")

    global_endpoint_dict = {}

    for key in keys:
        key = dict(key)[db.KEY]  # noqa: PLW2901
        async with db.get_db_connection(db_dir=db_dir) as conn:
            logs = await db.get_all_logs_for_key(conn, key)

        total_requests = len(logs)
        total_credits_used = sum([dict(log).get("cost", 0) for log in logs])

        for log in logs:
            log = dict(log)  # noqa: PLW2901
            endpoint = log.get(db.ENDPOINT, "unknown_endpoint")
            global_endpoint_dict[endpoint] = global_endpoint_dict.get(endpoint, 0) + 1

        summary_table.add_row(key, str(total_requests), str(total_credits_used))

    console.print(summary_table)

    breakdown_table = Table(show_header=True, header_style="bold green")
    breakdown_table.add_column("Endpoint")
    breakdown_table.add_column("Count")

    for endpoint, count in global_endpoint_dict.items():
        breakdown_table.add_row(endpoint, str(count))

    console.print("Endpoint Breakdown:")
    console.print(breakdown_table)


if __name__ == "__main__":
    cli()
