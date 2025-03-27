import os
import re
import sys
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def csv_file_gen(csv_dir):
    """
    Yields tuples of (timestamp, filepath) for CSV files matching the pattern:
    liquidity_data_<timestamp>.csv in the given directory.
    """
    pattern = re.compile(r'liquidity_data_(\d+)\.csv')
    try:
        for filename in os.listdir(csv_dir):
            match = pattern.match(filename)
            if match:
                timestamp = int(match.group(1))
                filepath = os.path.join(csv_dir, filename)
                yield timestamp, filepath
    except Exception as e:
        print(f"Error listing directory {csv_dir}: {e}")

def check_negative_liquidity(csv_dir):
    """
    Iterates over all CSV files in the directory using csv_file_gen.
    If a CSV file contains any negative cumulative_liquidity value, it prints
    the file path and the min/max liquidityNet values (if available), then terminates.
    """
    for _, filepath in csv_file_gen(csv_dir):
        try:
            df = pd.read_csv(filepath)
            # Convert the cumulative_liquidity column to numeric, coercing errors to NaN
            df['cumulative_liquidity'] = pd.to_numeric(df['cumulative_liquidity'], errors='coerce')
            # Also convert liquidityNet column if it exists
            if 'liquidityNet' in df.columns:
                df['liquidityNet'] = pd.to_numeric(df['liquidityNet'], errors='coerce')
            if (df['cumulative_liquidity'] < 0).any():
                print(f"Error: Negative cumulative liquidity found in file: {filepath}")
                # Print net liquidity stats if the column exists
                if 'liquidityNet' in df.columns:
                    net_min = df['liquidityNet'].min()
                    net_max = df['liquidityNet'].max()
                    print(f"Net liquidity stats - Min: {net_min}, Max: {net_max}")
                else:
                    print("No liquidityNet column found for stats.")
                sys.exit(1)
        except Exception as e:
            print(f"Error reading {filepath}: {e}")

if __name__ == "__main__":
    pool_csv_dir_005 = os.getenv('output_csv_path_USDC_ETH_0.05_Pool')
    pool_csv_dir_03 = os.getenv('output_csv_path_USDC_ETH_0.3_Pool')

    # Check for negative cumulative liquidity in both pool CSV directories.
    print("Checking 0.05 Pool CSV files for negative cumulative liquidity...")
    check_negative_liquidity(pool_csv_dir_005)
    print("Checking 0.3 Pool CSV files for negative cumulative liquidity...")
    check_negative_liquidity(pool_csv_dir_03)

    print("No negative cumulative liquidity values found in any CSV file.")