import requests # type: ignore
import json
import os
from web3 import Web3 # type: ignore
from dotenv import load_dotenv # type: ignore

# Load environment variables
load_dotenv()
api_key = os.getenv('api_key')
url = f'https://mainnet.infura.io/v3/{api_key}'

# Load the ABI from the JSON file
with open('pool_abi.json') as f:
    pool_abi = json.load(f)

# Create a Web3 instance
w3 = Web3(Web3.HTTPProvider(url))

# Get the latest block number
latest_block = w3.eth.block_number

# Define the Uniswap V3 pool contract address for WBTC/ETH
pool_address = w3.to_checksum_address("0xCBCdF9626bC03E24f779434178A73a0B4bad62eD")  # Use toChecksumAddress for proper formatting

def get_swap_data(start_block, end_block):
    # Define the Swap event signature (keccak hash)
    swap_event_signature = "Swap(address,address,int256,int256,uint160,uint128,int24)"
    swap_event_signature_hash = "0x" + w3.keccak(text=swap_event_signature).hex() #Need to put into hexadecimal format

    # Create the payload to filter logs
    log_payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(start_block),  # Properly format to hex
            "toBlock": hex(end_block),      # Properly format to hex
            "address": pool_address,
            "topics": [swap_event_signature_hash]
        }],
        "id": 1
    }

    # Query the logs
    log_response = requests.post(url, data=json.dumps(log_payload), headers={'content-type': 'application/json'}).json()


    transaction_data_list = [] 
    # Check for errors and parse the logs
    if 'result' in log_response:
        # Create a contract instance
        pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

        # Process and decode each log entry
        for log in log_response['result']:
            transaction_hash = log['transactionHash']

            # Decode log entry
            decoded_event = pool_contract.events.Swap().process_log(log)

            # Extract amount0 and amount1 from the decoded event
            amount0 = decoded_event['args']['amount0']
            amount1 = decoded_event['args']['amount1']

            # Get the full transaction data using the transaction hash
            try:
                transaction_data = w3.eth.get_transaction(transaction_hash)
                
                # Create a dictionary to hold transaction details
                transaction_details = {
                    "transaction_hash": transaction_hash,
                    "from": transaction_data['from'],
                    "to": transaction_data['to'],
                    "gas": transaction_data['gas'],
                    "gasPrice": transaction_data['gasPrice'],
                    "value": transaction_data['value'],
                    "block": transaction_data['blockNumber'],
                    "amount0": amount0,  # Token 0 amount (input or output)
                    "amount1": amount1   # Token 1 amount (input or output)
                }

                # Append the transaction details to the list
                transaction_data_list.append(transaction_details)
                
            except w3.exceptions.TransactionNotFound:
                print(f"Transaction {transaction_hash} not found.")
    else:
        print("Error retrieving logs:", log_response)

    return transaction_data_list

def get_liquidity_data(start_block, end_block):
    # Define the Mint event signature (keccak hash)
    mint_event_signature = "Mint(address,address,int24,int24,uint128,uint256,uint256)"
    mint_event_signature_hash = "0x" + w3.keccak(text=mint_event_signature).hex()

    # Create the payload to filter logs
    log_payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": pool_address,
            "topics": [mint_event_signature_hash]
        }],
        "id": 1
    }

    # Query the logs
    log_response = requests.post(url, data=json.dumps(log_payload), headers={'content-type': 'application/json'}).json()

    liquidity_data = []
    if 'result' in log_response and log_response['result']:
        # Create a contract instance
        pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

        # Process and decode each log entry
        for log in log_response['result']:
            transaction_hash = log['transactionHash']
            
            # Decode log entry
            try:
                decoded_event = pool_contract.events.Mint().process_log(log)

                # Get the full transaction data
                transaction_data = w3.eth.get_transaction(transaction_hash)
                liquidity_info = {
                    'transaction_hash': transaction_hash,
                    'provider': decoded_event['args']['sender'],  # Wallet of the provider
                    'amount0': decoded_event['args']['amount0'],  # Amount of token0 provided
                    'amount1': decoded_event['args']['amount1'],  # Amount of token1 provided
                    'block_number': transaction_data.blockNumber,
                    # Access the correct keys for the range
                    'range': f"{decoded_event['args']['tickLower']} - {decoded_event['args']['tickUpper']}"  # Range provided
                }
                liquidity_data.append(liquidity_info)

            except w3.exceptions.MismatchedABI as e:
                print(f"Mismatched ABI for transaction {transaction_hash}: {e}")
            except w3.exceptions.TransactionNotFound:
                print(f"Transaction {transaction_hash} not found.")
    else:
        print("No Mint events found in the specified block range.")

    return liquidity_data


swap_data = get_swap_data(latest_block-1000, latest_block)
liquidity_data = get_liquidity_data(latest_block-1000, latest_block)

print("This is the swap data from the last 1000 blocks")
print(swap_data)
print("This is the liquidity data from the last 1000 blocks")
print(liquidity_data)