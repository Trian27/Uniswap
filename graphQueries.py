import requests # type: ignore
import os, json
from dotenv import load_dotenv # type: ignore
from datetime import datetime
import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore

# Load environment variables
load_dotenv()
the_graph_api_key = os.getenv('the_graph_api_key')

# The Graph API endpoint
GRAPH_API_URL = f"https://gateway.thegraph.com/api/{the_graph_api_key}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Pool address remains the same
POOL_ADDRESS = "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed"

OUTPUT_DIR = os.getenv('output_csv_path')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# def get_hourly_pool_data(pool_address, hours):

#     skip = 0
#     batch_size = 1000

#     hour_timestamps = []
#     hour_current_ticks = []
#     hour_all_tick_data = []
#     liquidity_per_hour = [{} for _ in range(hours)]
#     hours_needing_more_data = set(range(hours))

#     while hours_needing_more_data:
        
#         query = """
#         {
#             pool(id: "%s") {
#                 id
#                 poolHourData(first: %d, orderBy: periodStartUnix, orderDirection: desc) {
#                 periodStartUnix
#                 pool {
#                     tick
#                     ticks(first: %d, skip: %d, orderBy: tickIdx, orderDirection: asc) {
#                     tickIdx
#                     liquidityNet
#                     }
#                 }
#                 }
#             }
#         }
#         """ % (pool_address, hours, batch_size, skip)
#         try:
#             response = requests.post(
#                 GRAPH_API_URL,
#                 json={'query': query},
#                 headers={'Content-Type': 'application/json'}
#             )
#             if response.status_code == 200:
#                 response_json = response.json()
#                 if 'data' in response_json and 'pool' in response_json['data'] and 'poolHourData' in response_json['data']['pool']:
#                     hourly_data = response_json['data']['pool']['poolHourData']
#                     for hour_index, current_hour in enumerate(hourly_data):
#                         reversed_hour_index = hours - hour_index - 1
#                         if not hour_timestamps or current_hour['periodStartUnix'] not in hour_timestamps:
#                             hour_timestamps.append(current_hour['periodStartUnix'])
#                             hour_current_ticks.append(current_hour['pool']['tick'])
#                             print(current_hour['pool']['tick'])
#                         processed_tick_data = []
#                         proper_tick_order = list(reversed(current_hour['pool']['ticks']))
#                         if not proper_tick_order:
#                             hours_needing_more_data.discard(reversed_hour_index)
#                             continue
#                         current_liquidity = 0
#                         for processed_tick in proper_tick_order:
#                             tick_idx = processed_tick['tickIdx']
#                             if tick_idx in liquidity_per_hour[reversed_hour_index]:
#                                 liquidity_per_hour[reversed_hour_index][tick_idx] += int(processed_tick['liquidityNet'])
#                                 current_liquidity = liquidity_per_hour[reversed_hour_index][tick_idx]
#                             else:
#                                 liquidity_per_hour[reversed_hour_index][tick_idx] = int(processed_tick['liquidityNet'])
#                                 current_liquidity = int(processed_tick['liquidityNet'])
#                             processed_tick_data.append((processed_tick['tickIdx'], current_liquidity))
#                         hour_all_tick_data.append(processed_tick_data)
#                     skip += batch_size
#                 else:
#                     pass
#             else:
#                 print(f"Error {response.status_code}: {response.text}")
#                 break
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             break

#     return hour_timestamps, hour_current_ticks, hour_all_tick_data

# def plot_hourly_liquidity(hour_timestamps, hour_current_ticks, hour_all_tick_data):
#     fig, axes = plt.subplots(6, 4, figsize=(20, 30))
#     fig.suptitle('Hourly Liquidity Distribution', fontsize=16)
#     axes = axes.flatten()
    
#     for hour_idx, (timestamp, current_tick, tick_data) in enumerate(zip(hour_timestamps, hour_current_ticks, hour_all_tick_data)):
#         ax = axes[hour_idx]
#         ticks, liquidity = zip(*tick_data)
#         bars = ax.bar(ticks, liquidity, width=50, color='blue')
        
#         # Highlight only exact current tick match
#         for idx, tick in enumerate(ticks):
#             if int(tick) == int(current_tick):
#                 bars[idx].set_color('red')
#         ax.axvline(x=int(current_tick), color='red', linestyle='--', linewidth=2)
#         dt = datetime.fromtimestamp(int(timestamp))
#         time_str = dt.strftime('%Y-%m-%d %H:%M')
        
#         ax.set_title(f'Time: {time_str}\nCurrent Tick: {current_tick}')
#         ax.set_xlabel('Tick Index')
#         ax.set_ylabel('Liquidity')
#         ax.tick_params(axis='x', rotation=45)
#         ax.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    
#     plt.tight_layout()
#     save_path = os.path.join(OUTPUT_DIR, 'liquidity_distribution.png')
#     plt.savefig(save_path)
#     plt.close()
#     print(f"Chart saved to: {save_path}")

# def main():
#     hour_timestamps, hour_current_ticks, hour_all_tick_data = get_hourly_pool_data(POOL_ADDRESS, 24)
#     print(len(hour_timestamps), len(hour_current_ticks), len(hour_all_tick_data))
#     print(hour_current_ticks)
#     print(hour_all_tick_data)
#     # plot_hourly_liquidity(hour_timestamps, hour_current_ticks, hour_all_tick_data)

def get_hourly_pool_data(pool_address):
    skip = 0
    batch_size = 1000
    timestamp = int(datetime.now().timestamp())
    all_ticks = []
    pool_data = None
    cumulative_liquidity = 0

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
                
                # Store pool data only once
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
                    cumulative_liquidity += int(tick['liquidityNet'])
                    tick_data.append({
                        "tickIdx": tick['tickIdx'],
                        "liquidityNet": tick['liquidityNet'],
                        "cumulative_liquidity": cumulative_liquidity
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
    df['cumulative_liquidity'] = df['cumulative_liquidity'].astype(float)
    df['liquidityNet'] = df['liquidityNet'].astype(float)
    # Calculate cumulative liquidity
    df = df.sort_values('tickIdx')
    
    filename = f"liquidity_data_{timestamp}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def main():
    get_hourly_pool_data(POOL_ADDRESS)

if __name__ == "__main__":
    main()