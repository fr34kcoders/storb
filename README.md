<div align="center">

```
                                                     
   █████████   █████                       █████     
  ███░░░░░███ ░░███                       ░░███      
 ░███    ░░░  ███████    ██████  ████████  ░███████  
 ░░█████████ ░░░███░    ███░░███░░███░░███ ░███░░███ 
  ░░░░░░░░███  ░███    ░███ ░███ ░███ ░░░  ░███ ░███ 
  ███    ░███  ░███ ███░███ ░███ ░███      ░███ ░███ 
 ░░█████████   ░░█████ ░░██████  █████     ████████  
  ░░░░░░░░░     ░░░░░   ░░░░░░  ░░░░░     ░░░░░░░░   
                                                     
The Decentralized Object Storage Subnet
```

</div>

Storb is a decentralized object storage subnet built on the Bittensor network. It aims to be a distributed, fault-tolerant, and efficient digital storage solution.

## Features

- **Decentralization**: Utilizes a network of nodes to store data redundantly.

- **Erasure Coding**: Enhances data reliability and storage efficiency by fragmenting and distributing data across multiple nodes, allowing reconstruction even if some fragments are lost. 

- **Incentivized Storage**: Storb leverages the power of the Bittensor. The subnet rewards miners for contributing reliable and responsive storage resources, with validators ensuring data integrity. Bittensor serves as *the* ideal incentive layer for this.

For an overview of how the subnet works, [see here](docs/overview.md).

## Installation

Follow these steps to set up a miner or validator node:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/fr34kcoders/storb.git
   cd storb
   ```

2. **Set Up Virtual Environment**:

   Ensure you have [`uv` installed](https://docs.astral.sh/uv/getting-started/installation/). Once you do, run the following commands:

   ```bash
   uv venv --python 3.12
   source ./venv/bin/activate
   uv sync
   uv pip install -e .
   ```

3. **Configure and run node**:

- [**Miner**](docs/miner.md)
- [**Validator**](docs/validator.md)

## Contributing

We welcome contributions to enhance Storb. Please fork the repository and submit a pull request with your improvements.

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/fr34kcoders/storb/blob/main/LICENSE) file for details.

## Contact

For questions or support, please open an issue in this repository or contact the maintainers on the Bittensor discord server.
