import pandas as pd
import numpy as np

# Read the CSV file
df = pd.read_csv('/Users/trian/Projects/Uniswap/PEPE_WETH_Pool/outputFiles/liquidityCSV/liquidity_data_1734109202.csv')

# Sort by tickIdx ascending
df = df.sort_values('tickIdx')

# Calculate new cumulative liquidity
df['new_cumulative'] = df['liquidityNet'].cumsum()

# Compare with existing calculation
df['difference'] = df['new_cumulative'] - df['cumulative_liquidity']

# Check for negative values in both calculations
neg_original = df[df['cumulative_liquidity'] < 0]
neg_new = df[df['new_cumulative'] < 0]

print("Original calculation negative values:")
if len(neg_original) > 0:
    print(neg_original[['tickIdx', 'cumulative_liquidity']])
else:
    print("None found")

print("\nNew calculation negative values:")
if len(neg_new) > 0:
    print(neg_new[['tickIdx', 'new_cumulative']])
else:
    print("None found")

print("\nRows with significant differences:")
significant_diff = df[abs(df['difference']) > 1e-10]
if len(significant_diff) > 0:
    print(significant_diff[['tickIdx', 'cumulative_liquidity', 'new_cumulative', 'difference']])
else:
    print("No significant differences found")