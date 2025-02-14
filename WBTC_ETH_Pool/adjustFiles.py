import os
import pandas as pd
from decimal import Decimal, getcontext
from dotenv import load_dotenv

# Set Decimal precision to handle very large numbers
getcontext().prec = 78  # Approximately 256 bits
load_dotenv()

def process_csv_files():
    source_dir = os.getenv('output_csv_path_WBTC_ETH_Pool')
    dest_dir = os.getenv('output_csv_adjusted_path_WBTC_ETH_POOL')
    
    os.makedirs(dest_dir, exist_ok=True)
    
    for filename in os.listdir(source_dir):
        if filename.endswith('.csv'):
            print(f"\nProcessing {filename}...")
            filepath = os.path.join(source_dir, filename)
            
            # Read CSV with string types to preserve precision
            df = pd.read_csv(filepath, dtype={
                'liquidityNet': str,
                'tickIdx': str,
                'timestamp': str,
                'current_tick': str,
                'pool_id': str
            })
            
            # Keep only needed columns
            df = df[['tickIdx', 'liquidityNet', 'timestamp', 'current_tick', 'pool_id']]
            
            # Convert to appropriate types
            df['liquidityNet'] = df['liquidityNet'].apply(Decimal)
            df['tickIdx'] = df['tickIdx'].astype(int)
            df['timestamp'] = df['timestamp'].astype(int)
            df['current_tick'] = df['current_tick'].astype(int)
            
            df = df.sort_values('tickIdx')
            
            # Calculate cumulative with Decimal
            cumulative = Decimal('0')
            cumulative_list = []
            
            for liquidity in df['liquidityNet']:
                cumulative += liquidity
                if cumulative < 0:
                    cumulative = Decimal('0')
                cumulative_list.append(str(cumulative))  # Convert to string to preserve precision
            
            df['cumulative_liquidity'] = cumulative_list
            
            # Save to CSV with string format to preserve precision
            dest_path = os.path.join(dest_dir, filename)
            df.to_csv(dest_path, index=False, float_format='%.0f')
            print(f"Saved adjusted file to: {dest_path}")

if __name__ == "__main__":
    process_csv_files()