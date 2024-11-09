# Uniswap
Obtaining Uniswap V3 transaction data for analysis.
Note for now this is only adapted for the WBTC/ETH pool.
It will be simple to refactor the script to obtain data any pool address.

Necessary Features:

Note: I am using a virtual environment to install these
1. web3
2. python-dotenv
3. requests

The commands are:

pip install web3
pip install python-dotenv
pip install requests

Also need an Inufra API Key. Can obtain one for free from the website.

You will need to create an env file called ".env". This is where you will store the APIKey. Follow .env.example

Uses Etherscan to obtain pool ABI: https://etherscan.io/