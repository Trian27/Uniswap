import requests # type: ignore
import json
import os
from web3 import Web3 # type: ignore
from dotenv import load_dotenv # type: ignore
from datetime import datetime
import pytz # type: ignore
import pandas as pd # type: ignore
import csv

# Load environment variables
load_dotenv()
infura_api_key = os.getenv('infura_api_key')
url = f'https://mainnet.infura.io/v3/{infura_api_key}'

# Load the ABI from the JSON file
with open('abis/pool_abi.json') as f:
    pool_abi = json.load(f)

# Create a Web3 instance
w3 = Web3(Web3.HTTPProvider(url))

# Get the latest block number
latest_block = w3.eth.block_number

pool_address = w3.to_checksum_address("0x11950d141EcB863F01007AdD7D1A342041227b58")

#Need these to scale. Can search this up online. Contract for wbtc is: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599
wbtc_decimals = 8
eth_decimals = 18
gas_decimals = 18
value_decimals = 18

pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)

def get_pool_data():
    # Get the current state of the pool
    slot0 = pool_contract.functions.slot0().call()
    sqrtPriceX96 = slot0[0]
    current_tick = slot0[1] # will have to modify later to be a multiple of the tick spacing 
    
    # Get the fee tier and tick spacing from the pool contract
    fee = pool_contract.functions.fee().call()
    tick_spacing = pool_contract.functions.tickSpacing().call()
    
    # Convert fee to percentage
    fee_percent = fee / 1e4  # Fee is returned in hundredths of a bip
    
    # # Calculate price from sqrtPriceX96
    # can use later
    # price = ((sqrtPriceX96 / (2 ** 96)) ** 2) * (10 ** (wbtc_decimals - eth_decimals))
    
    # Adjust current_tick to nearest valid tick index
    adjusted_tick = current_tick - (current_tick % tick_spacing)
    
    # Define a reasonable tick range around the adjusted tick
    tick_range_multiplier = 1000
    min_tick = adjusted_tick - (tick_range_multiplier * tick_spacing)
    max_tick = adjusted_tick + (tick_range_multiplier * tick_spacing)
    
    # Initialize a list to store liquidity data
    liquidity_data = []
    
    # Loop through ticks in the specified range
    for tick in range(min_tick, max_tick + tick_spacing, tick_spacing):
        try:
            tick_data = pool_contract.functions.ticks(tick).call()
            liquidity_gross = int(tick_data[0])
            liquidity_net = int(tick_data[1])
            if liquidity_gross > 0 or liquidity_net != 0:
                liquidity_data.append({
                    'tickIdx': tick,
                    'liquidityGross': liquidity_gross,
                    'liquidityNet': liquidity_net
                })
        except Exception:
            continue
    
    # Create DataFrame with explicit data types
    df = pd.DataFrame(liquidity_data).astype({
        'tickIdx': 'int64',
        'liquidityGross': 'int64',
        'liquidityNet': 'int64'
    })
    
    if not df.empty:
        output_dir = "./outputFiles/poolData"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'liquidity_distribution_{timestamp}.csv'
        filepath = os.path.join(output_dir, filename)
        
        # Write to CSV with numeric format preserved
        df.to_csv(filepath, 
                  index=False,
                  float_format='%.0f',
                  quoting=csv.QUOTE_MINIMAL)


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
                'gasPriceAdjusted': transaction_data['gasPrice'] / (10 ** gas_decimals),
                'value': transaction_data['value'] / (10 ** value_decimals),
                'WBTCTokens': decoded_event['args']['amount0'],
                'ETHTokens': decoded_event['args']['amount1'],
                'WBTCTokensAdjusted': decoded_event['args']['amount0']/(10 ** wbtc_decimals),
                'ETHTokensAdjusted': decoded_event['args']['amount1']/(10 ** eth_decimals),
                'block': transaction_data['blockNumber'],
                'timestamp': get_block_timestamp(transaction_data['blockNumber']),
            }

            # Append the transaction details to the list
            swap_data_list.append(transaction_details)
            
    else:
        print("Error retrieving logs:", log_response)
    

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
                'gasPriceAdjusted': transaction_data['gasPrice'] / (10 ** gas_decimals),
                'amount': decoded_event['args']['amount'],
                'WBTCTokens': decoded_event['args']['amount0'],
                'ETHTokens': decoded_event['args']['amount1'],
                'WBTCTokensAdjusted': decoded_event['args']['amount0']/(10 ** wbtc_decimals),
                'ETHTokensAdjusted': decoded_event['args']['amount1']/(10 ** eth_decimals),
                'block': transaction_data.blockNumber,
                'timestamp': get_block_timestamp(transaction_data['blockNumber']),
                'lowerTick': decoded_event['args']['tickLower'],
                'upperTick': decoded_event['args']['tickUpper']
            }
            provider_data.append(provider_info)

    else:
        print("No Mint events found in the specified block range.")


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

        # Process and decode each log entry
        for log in log_response['result']:
            transaction_hash = log['transactionHash']

            decoded_event = pool_contract.events.Burn().process_log(log)

            transaction_data = w3.eth.get_transaction(transaction_hash)
            
            burn_info = {
                'transaction_hash': transaction_hash,
                'provider': decoded_event['args']['owner'], #references owner
                'gas': transaction_data['gas'],
                'gasPrice': transaction_data['gasPrice'],
                'gasPriceAdjusted': transaction_data['gasPrice'] / (10 ** gas_decimals),
                'amount': decoded_event['args']['amount'],
                'WBTCTokens': -decoded_event['args']['amount0'],
                'ETHTokens': -decoded_event['args']['amount1'],
                'WBTCTokensAdjusted': -decoded_event['args']['amount0']/(10 ** wbtc_decimals),
                'ETHTokensAdjusted': -decoded_event['args']['amount1']/(10 ** eth_decimals),
                'block': transaction_data.blockNumber,
                'timestamp': get_block_timestamp(transaction_data['blockNumber']),
                'lowerTick': decoded_event['args']['tickLower'],
                'upperTick': decoded_event['args']['tickUpper']
            }
            burn_data.append(burn_info)

    else:
        print("No Burn events found in the specified block range.")

    return burn_data

def get_liquidity_provider_data(start_block, end_block):
    provider_data = get_provider_data(start_block, end_block)
    burn_data = get_burn_data(start_block, end_block)
    liquidity_data = provider_data + burn_data
    liquidity_data.sort(key=lambda x: x['block'])

    return liquidity_data

#Can use to reduce redundancy. Not currently used.
def process_transaction(log, pool_contract, event_name):
    decoded_event = pool_contract.events[event_name]().process_log(log)
    transaction_hash = log['transactionHash']
    transaction_data = w3.eth.get_transaction(transaction_hash)
    return decoded_event, transaction_data

def get_block_timestamp(block_number):
    block = w3.eth.get_block(block_number)
    unix_timestamp = block['timestamp']
    dt_object = datetime.fromtimestamp(unix_timestamp, pytz.UTC)
    return dt_object

def write_swap_data_csv(swap_data, filename="swap_data.csv"):
    df = pd.DataFrame(swap_data)
    filepath = os.path.join("outputFiles", filename)
    df.to_csv(filepath, index=False)

def write_provider_data_csv(provider_data, filename="provider_data.csv"):
    df = pd.DataFrame(provider_data)
    filepath = os.path.join("outputFiles", filename)
    df.to_csv(filepath, index=False)

def write_burn_data_csv(burn_data, filename="burn_data.csv"):
    df = pd.DataFrame(burn_data)
    filepath = os.path.join("outputFiles", filename)
    df.to_csv(filepath, index=False)

def write_liquidity_data_csv(liquidity_data, filename="liquidity_data.csv"):
    df = pd.DataFrame(liquidity_data)
    filepath = os.path.join("outputFiles", filename)
    df.to_csv(filepath, index=False)

# #47000 is an approximate upper bound on a week's worth of ethereum blocks. Typical block takes about 13 seconds to be mined.
# #Need to break it up in chunks of around 5000 blocks though otherwise hit some sort of limits
# swap_data = []
# provider_data = []
# burn_data = []
# liquidity_data = []
# for i in range(latest_block-47000, latest_block, 1250):
#     swap_data += get_swap_data(i+1, i+1250)
#     provider_data += get_provider_data(i+1, i+1250)
#     burn_data += get_burn_data(i+1, i+1250)
#     liquidity_data += get_liquidity_provider_data(i+1, i+1250)

# write_swap_data_csv(swap_data)
# write_provider_data_csv(provider_data)
# write_burn_data_csv(burn_data)
# write_liquidity_data_csv(liquidity_data)

get_pool_data()