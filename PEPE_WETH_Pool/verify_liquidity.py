import pandas as pd
import numpy as np
from decimal import Decimal

try:
    # Read CSV file
    df = pd.read_csv('/Users/trian/Projects/Uniswap/PEPE_WETH_Pool/outputFiles/liquidityCSV/liquidity_data_1736528402.csv')
    
    # Convert to numeric, coerce errors to NaN
    df['liquidityNet'] = pd.to_numeric(df['liquidityNet'], errors='coerce')
    
    # Sort values and get top 10 most negative
    most_negative = df.sort_values('liquidityNet').head(160)
    
    print("\nTop 160 most negative liquidityNet values:")
    print("----------------------------------------")
    for _, row in most_negative.iterrows():
        print(f"LiquidityNet: {row['liquidityNet']:.0f}")
        
except Exception as e:
    print(f"Error processing file: {e}")