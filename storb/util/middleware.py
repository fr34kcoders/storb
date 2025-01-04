from colorama import Fore, Style
from fastapi import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request

from storb.constants import MAX_UPLOAD_SIZE
from storb.util.logging import get_logger

logger = get_logger(__name__)


class LoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        """Logging middleware - pretty colors for logging"""

        logger.info(
            f"{Style.BRIGHT}{Fore.GREEN}Request{Style.RESET_ALL}: {Style.BRIGHT}{Fore.BLUE}{request.method}{Style.RESET_ALL} {request.url}"
        )
        response = await call_next(request)
        return response


class FileSizeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        # Retrieve Content-Length header
        content_length = request.headers.get("Content-Length")
        if content_length:
            if int(content_length) > MAX_UPLOAD_SIZE:
                raise HTTPException(
                    status_code=413, detail="File size exceeds the limit"
                )
        response = await call_next(request)
        return response
