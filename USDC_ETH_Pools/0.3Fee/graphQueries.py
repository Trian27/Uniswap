import requests # type: ignore
import os, json
from dotenv import load_dotenv # type: ignore
from datetime import datetime
import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore
from decimal import Decimal, getcontext

getcontext().prec = 78
# Load env with debug
load_dotenv()
the_graph_api_key = os.getenv('the_graph_api_key')

# The Graph API endpoint
GRAPH_API_URL = f"https://gateway.thegraph.com/api/{the_graph_api_key}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

POOL_ADDRESS = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"

OUTPUT_DIR = os.getenv('output_csv_path_USDC_ETH_0.3_Pool')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_hourly_pool_data(pool_address):
    skip = 0
    batch_size = 1000
    timestamp = int(datetime.now().timestamp())
    all_ticks = []
    pool_data = None

    while True:
        query = """
        {
            pool(id: "%s") {
                id
                tick
                ticks(first: %d, skip: %d, orderBy: tickIdx, orderDirection: asc) {
                    tickIdx
                    liquidityNet
                }
            }
        }
        """ % (pool_address, batch_size, skip)

        try:
            response = requests.post(
                GRAPH_API_URL,
                json={'query': query},
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code == 200:
                data = response.json()['data']['pool']
                
                if pool_data is None:
                    pool_data = {
                        "id": data['id'],
                        "tick": data['tick']
                    }
                
                ticks = data['ticks']
                if not ticks:
                    break
                tick_data = []
                for tick in ticks:
                    print(tick)
                    tick_data.append({
                        "tickIdx": tick['tickIdx'],
                        "liquidityNet": tick['liquidityNet']
                    })
                all_ticks.extend(tick_data)
                skip += batch_size
                
        except Exception as e:
            print(f"Error: {e}")
            break
    
    df = pd.DataFrame(all_ticks)
    
    df['timestamp'] = timestamp
    df['current_tick'] = pool_data['tick']
    df['pool_id'] = pool_data['id']
    
    df['tickIdx'] = df['tickIdx'].astype(int)
    df['liquidityNet'] = df['liquidityNet'].apply(Decimal)
    df = df.sort_values('tickIdx')
    df['cumulative_liquidity'] = df['liquidityNet'].cumsum()
    
    filename = f"liquidity_data_{timestamp}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def main():
    get_hourly_pool_data(POOL_ADDRESS)

if __name__ == "__main__":
    main()