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
with open('abis/pool_abi.json') as f:
    pool_abi = json.load(f)

# Create a Web3 instance
w3 = Web3(Web3.HTTPProvider(url))

# Get the latest block number
latest_block = w3.eth.block_number

# Define the Uniswap V3 pool contract address for WBTC/ETH
pool_address = w3.to_checksum_address("0xCBCdF9626bC03E24f779434178A73a0B4bad62eD")  # Use toChecksumAddress for proper formatting

#Need these to scale. Can search this up online. Contract for wbtc is: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599
wbtc_decimals = 8
eth_decimals = 18

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

    swap_data_list = [] 
    
    if 'result' in log_response and log_response['result']:
        
        pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

        # Process and decode each log entry
        for log in log_response['result']:
            transaction_hash = log['transactionHash']

            # Decode log entry
            decoded_event = pool_contract.events.Swap().process_log(log)

            # Get the full transaction data using the transaction hash
            transaction_data = w3.eth.get_transaction(transaction_hash)
            
            # Create a dictionary to hold transaction details
            transaction_details = {
                'transaction_hash': transaction_hash,
                'sender': decoded_event['args']['sender'],
                'recipient': decoded_event['args']['recipient'],
                'gas': transaction_data['gas'],
                'gasPrice': transaction_data['gasPrice'],
                'value': transaction_data['value'],
                'WBTCTokens': decoded_event['args']['amount0'],
                'ETHTokens': decoded_event['args']['amount1'],
                'WBTCTokensAdjusted': decoded_event['args']['amount0']/(10 ** wbtc_decimals),
                'ETHTokensAdjusted': decoded_event['args']['amount1']/(10 ** eth_decimals),
                'block': transaction_data['blockNumber'],
            }

            # Append the transaction details to the list
            swap_data_list.append(transaction_details)
            
    else:
        print("Error retrieving logs:", log_response)
    
    with open('outputFiles/swap_data.txt', 'w') as f:
        for entry in swap_data_list:
            json.dump(entry, f)
            f.write('\n')

    return swap_data_list

def get_provider_data(start_block, end_block):
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

    provider_data = []
    if 'result' in log_response and log_response['result']:
        # Create a contract instance
        pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

        # Process and decode each log entry
        for log in log_response['result']:
            transaction_hash = log['transactionHash']
            
            # Decode log entry
            decoded_event = pool_contract.events.Mint().process_log(log)

            # Get the full transaction data
            transaction_data = w3.eth.get_transaction(transaction_hash)
            provider_info = {
                'transaction_hash': transaction_hash,
                'provider': decoded_event['args']['sender'],
                'gas': transaction_data['gas'],
                'gasPrice': transaction_data['gasPrice'],
                'amount': decoded_event['args']['amount'],
                'WBTCTokens': decoded_event['args']['amount0'],
                'ETHTokens': decoded_event['args']['amount1'],
                'WBTCTokensAdjusted': decoded_event['args']['amount0']/(10 ** wbtc_decimals),
                'ETHTokensAdjusted': decoded_event['args']['amount1']/(10 ** eth_decimals),
                'block': transaction_data.blockNumber,
                'lowerTick': decoded_event['args']['tickLower'],
                'upperTick': decoded_event['args']['tickUpper']
            }
            provider_data.append(provider_info)

    else:
        print("No Mint events found in the specified block range.")

    with open('outputFiles/provider_data.txt', 'w') as f:
        for entry in provider_data:
            json.dump(entry, f)
            f.write('\n')

    return provider_data

def get_burn_data(start_block, end_block):

    burn_event_signature = "Burn(address,int24,int24,uint128,uint256,uint256)"
    burn_event_signature_hash = "0x" + w3.keccak(text=burn_event_signature).hex()
    
    log_payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": pool_address,
            "topics": [burn_event_signature_hash]
        }],
        "id": 1
    }
    log_response = requests.post(url, data=json.dumps(log_payload), headers={'content-type': 'application/json'}).json()
    burn_data = []
    if 'result' in log_response and log_response['result']:
        # Create a contract instance
        pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

        # Process and decode each log entry
        for log in log_response['result']:
            transaction_hash = log['transactionHash']

            decoded_event = pool_contract.events.Burn().process_log(log)

            transaction_data = w3.eth.get_transaction(transaction_hash)
            
            burn_info = {
                'transaction_hash': transaction_hash,
                'owner': decoded_event['args']['owner'],  # Wallet of the provider
                'gas': transaction_data['gas'],
                'gasPrice': transaction_data['gasPrice'],
                'amount': decoded_event['args']['amount'],
                'WBTCTokens': -decoded_event['args']['amount0'],
                'ETHTokens': -decoded_event['args']['amount1'],
                'WBTCTokensAdjusted': -decoded_event['args']['amount0']/(10 ** wbtc_decimals),
                'ETHTokensAdjusted': -decoded_event['args']['amount1']/(10 ** eth_decimals),
                'block': transaction_data.blockNumber,
                'lowerTick': decoded_event['args']['tickLower'],
                'upperTick': decoded_event['args']['tickUpper']
            }
            burn_data.append(burn_info)

    else:
        print("No Burn events found in the specified block range.")

    with open('outputFiles/burn_data.txt', 'w') as f:
        for entry in burn_data:
            json.dump(entry, f)
            f.write('\n')

    return burn_data

def get_liquidity_provider_data(start_block, end_block):
    provider_data = get_provider_data(start_block, end_block)
    burn_data = get_burn_data(start_block, end_block)
    liquidity_data = provider_data + burn_data
    liquidity_data.sort(key=lambda x: x['block'])
    with open('outputFiles/liquidity_data.txt', 'w') as f:
        for entry in liquidity_data:
            json.dump(entry, f)
            f.write('\n')
    return liquidity_data

#Can use to reduce redundancy. Not currently used.
def process_transaction(log, pool_contract, event_name):
    decoded_event = pool_contract.events[event_name]().process_log(log)
    transaction_hash = log['transactionHash']
    transaction_data = w3.eth.get_transaction(transaction_hash)
    return decoded_event, transaction_data

swap_data = get_swap_data(latest_block-1000, latest_block)
provider_data = get_provider_data(latest_block-10000, latest_block)
burn_data = get_burn_data(latest_block-10000, latest_block)
liquidity_data = get_liquidity_provider_data(latest_block-10000, latest_block)