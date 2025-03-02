import os
import requests
import csv
from datetime import datetime

def get_current_eth_usd_coinbase():
    # Coinbase API endpoint for ETH/USD price
    url = "https://api.coinbase.com/v2/prices/ETH-USD/spot"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        current_price = data['data']['amount']
        # Get current Unix timestamp
        timestamp = int(datetime.now().timestamp())
        
        # Define the path to the eth_cex_prices.csv file
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'eth_cex_prices.csv')
        
        # Append the timestamp and price to the CSV file
        with open(csv_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([timestamp, current_price])
    else:
        print("Error fetching data from Coinbase API")

get_current_eth_usd_coinbase()