import http.client
import json
import os
import re
import csv
import time
from dotenv import load_dotenv

load_dotenv()

def get_price_for_timestamp(ts):
    """
    Query Coinbase for a 1-minute candle matching the given timestamp.
    Returns the price (tries 'price' then 'close') if available.
    """
    conn = http.client.HTTPSConnection("api.coinbase.com")
    payload = ''
    headers = {'Content-Type': 'application/json'}
    path = f"/api/v3/brokerage/market/products/ETH-USD/candles?start={ts}&end={ts+60}&granularity=ONE_MINUTE"
    try:
        conn.request("GET", path, payload, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        obj = json.loads(data)
        if isinstance(obj, dict) and "candles" in obj:
            candle = obj["candles"]
            return candle[0]["open"]
        else:
            print(f"Unexpected response for timestamp {ts}: {obj}")
            return None
    except Exception as e:
        print(f"Error fetching price for timestamp {ts}: {e}")
        return None

def main():

    csv_dir = os.getenv('output_csv_path_USDC_ETH_0.05_Pool')
    if not csv_dir:
        print("Error: 'output_csv_path_USDC_ETH_0.05_Pool' not defined in .env file.")
        return

    pattern = re.compile(r'liquidity_data_(\d+)\.csv')
    timestamps = []

    try:
        files = os.listdir(csv_dir)
    except Exception as e:
        print(f"Error listing CSV directory '{csv_dir}': {e}")
        return

    for filename in files:
        match = pattern.match(filename)
        if match:
            ts = int(match.group(1))
            timestamps.append(ts)

    if not timestamps:
        print(f"No matching CSV files found in '{csv_dir}'.")
        return

    timestamps = sorted(set(timestamps))

    cex_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'eth_cex_prices.csv')

    with open(cex_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'price'])
        
        for ts in timestamps:
            price = get_price_for_timestamp(ts)
            if price is not None:
                writer.writerow([ts, price])
                print(f"Timestamp {ts}: Price {price}")
            else:
                print(f"Price not found for timestamp {ts}")
            time.sleep(0.1)

if __name__ == "__main__":
    main()