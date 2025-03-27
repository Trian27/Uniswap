import os
import re
import math
import csv
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dotenv import load_dotenv
import matplotlib.ticker as ticker
from decimal import Decimal, getcontext
import mpmath as mp
from scipy.stats import wasserstein_distance

load_dotenv()

getcontext().prec = 50
mp.mp.prec = 160

def precise_tick(price):
    """
    Compute tick = log_{1.0001}(10^12 / price) with high precision.
    Uses mpmath for the logarithms.
    """
    price_dec = Decimal(price)
    ratio = (Decimal(10) ** 12) / price_dec
    # Convert ratio to an mpmath mp.mpf using a string conversion for precision
    ratio_mpf = mp.mpf(str(ratio))
    base = mp.mpf("1.0001")
    tick_mpf = mp.log(ratio_mpf) / mp.log(base)
    return float(tick_mpf)

def precise_price(tick):
    """
    Compute price = 10^12 / (1.0001^tick) with high precision.
    Uses mpmath for the exponentiation.
    """
    tick_mpf = mp.mpf(tick)
    base = mp.mpf("1.0001")
    price_mpf = (mp.mpf(10) ** 12) / (base ** tick_mpf)
    return float(price_mpf)

def read_pool_data(filepath):
    df = pd.read_csv(filepath)
    df['tickIdx'] = df['tickIdx'].astype(int)
    df['cumulative_liquidity'] = df['cumulative_liquidity'].astype(float)
    return df.sort_values('tickIdx')


def get_max_liquidity(pool_csv_files_005, pool_csv_files_03):
    max_liquidity = 0
    for _, filepath in pool_csv_files_005:
        try:
            df = pd.read_csv(filepath)
            max_liquidity = max(max_liquidity, df['cumulative_liquidity'].astype(float).max())
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    for _, filepath in pool_csv_files_03:
        try:
            df = pd.read_csv(filepath)
            max_liquidity = max(max_liquidity, df['cumulative_liquidity'].astype(float).max())
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    return max_liquidity

def gen_cex_csv(filename):
    # rows sorted by timestamp
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

def csv_file_gen(csv_dir):
    pattern = re.compile(r'liquidity_data_(\d+)\.csv')
    all_files = os.listdir(csv_dir)
    pool_csv_files = []
    for filename in all_files:
        match = pattern.match(filename)
        if match:
            timestamp = int(match.group(1))
            filepath = os.path.join(csv_dir, filename)
            pool_csv_files.append((timestamp, filepath))
    pool_csv_files.sort()

    for timestamp, filepath in pool_csv_files:
        yield timestamp, filepath


def compare_liquidity_distributions(pool_csv_files_005, pool_csv_files_03, cex_csv_dir):

    gen_cex = gen_cex_csv(cex_csv_dir)
    gen_005 = csv_file_gen(pool_csv_files_005)
    gen_03 = csv_file_gen(pool_csv_files_03)

    cex_row = next(gen_cex, None)
    pool_005 = next(gen_005, None)
    pool_03 = next(gen_03, None)

    while pool_005 is not None or pool_03 is not None:
        if pool_005 is not None:
            ts_005, filepath_005 = pool_005  # Unpack the tuple
        if pool_03 is not None:
            ts_03, filepath_03 = pool_03  # Unpack the tuple
        if cex_row is not None:
            ts_cex = int(cex_row['timestamp'])

        if pool_005 is not None and pool_03 is not None and cex_row is not None:
            if ts_cex < ts_005-15 or ts_cex < ts_03-15:
                cex_row = next(gen_cex, None)
                continue

            if ts_005 < ts_03-15:
                if ts_005 < ts_cex-15:
                    plot_liquidity_distributions(filepath_005, None, None)
                    pool_005 = next(gen_005, None)
                    continue
                else:
                    plot_liquidity_distributions(filepath_005, None, cex_row)
                    pool_005 = next(gen_005, None)
                    cex_row = next(gen_cex, None)
                    continue

            if ts_03 < ts_005-15:
                if ts_03 < ts_cex-15:
                    plot_liquidity_distributions(None, filepath_03, None)
                    pool_03 = next(gen_03, None)
                    continue
                else:
                    plot_liquidity_distributions(None, filepath_03, cex_row)
                    pool_03 = next(gen_03, None)
                    cex_row = next(gen_cex, None)
                    continue

            plot_liquidity_distributions(filepath_005, filepath_03, cex_row)
            pool_005 = next(gen_005, None)
            pool_03 = next(gen_03, None)
            cex_row = next(gen_cex, None)
            continue

        if pool_005 is not None and pool_03 is not None:

            if ts_005 < ts_03-15:
                plot_liquidity_distributions(filepath_005, None, None)
                pool_005 = next(gen_005, None)
                continue

            if ts_03 < ts_005-15:
                plot_liquidity_distributions(None, filepath_03, None)
                pool_03 = next(gen_03, None)
                continue

            plot_liquidity_distributions(filepath_005, filepath_03, None)
            pool_005 = next(gen_005, None)
            pool_03 = next(gen_03, None)
            continue

        if pool_005 is not None:
            if cex_row is not None and ts_005 >= ts_cex-15:
                
                if ts_cex < ts_005-15:
                    cex_row = next(gen_cex, None)
                    continue

                plot_liquidity_distributions(filepath_005, None, cex_row)
                pool_005 = next(gen_005, None)
                cex_row = next(gen_cex, None)
                continue

            plot_liquidity_distributions(filepath_005, None, None)
            pool_005 = next(gen_005, None)
            continue

        if pool_03 is not None:
            if cex_row is not None and ts_03 >= ts_cex-15:
                
                if ts_cex < ts_03-15:
                    cex_row = next(gen_cex, None)
                    continue

                plot_liquidity_distributions(None, filepath_03, cex_row)
                pool_005 = next(gen_03, None)
                cex_row = next(gen_cex, None)
                continue

            plot_liquidity_distributions(None, filepath_03, None)
            pool_03 = next(gen_03, None)
            continue


def plot_liquidity_distributions(pool_005_filepath, pool_03_filepath, cex_row):

    output_charts_path = os.getenv('output_charts_path_USDC_ETH_compare')

    timestamp = None

    cex_tick = None
    cex_msg = "CEX not available"
    if cex_row is not None:
        cex_tick = precise_tick(float(cex_row['price']))
        cex_msg = f"CEX Tick: {cex_tick:.2f}"
        timestamp = int(cex_row['timestamp'])
    
    df_005, tick_005, current_tick_005, timestamp_005, time_str_005 = None, None, None, None, None
    if pool_005_filepath is not None:
        df_005 = read_pool_data(pool_005_filepath)
        df_005['price'] = df_005['tickIdx'].apply(precise_price)
        df_005['plot_cumulative_liquidity'] = df_005['cumulative_liquidity'].clip(lower=0)
        total_005 = df_005['plot_cumulative_liquidity'].sum()
        df_005['plot_norm_liquidity'] = (df_005['plot_cumulative_liquidity'] / total_005) if total_005 != 0 else 0
        current_tick_005 = int(df_005['current_tick'].iloc[0])
        timestamp = int(df_005['timestamp'].iloc[0])

    df_03, tick_03, current_tick_03, timestamp_03, time_str_03 = None, None, None, None, None
    if pool_03_filepath is not None:
        df_03 = read_pool_data(pool_03_filepath)
        df_03['price'] = df_03['tickIdx'].apply(precise_price)
        df_03['plot_cumulative_liquidity'] = df_03['cumulative_liquidity'].clip(lower=0)
        total_03 = df_03['plot_cumulative_liquidity'].sum()
        df_03['plot_norm_liquidity'] = (df_03['plot_cumulative_liquidity'] / total_03) if total_03 != 0 else 0
        current_tick_03 = int(df_03['current_tick'].iloc[0])
        timestamp = int(df_03['timestamp'].iloc[0])

    if df_005 is not None and df_03 is not None:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), sharey=True)
        axes = [ax1, ax2]
    else:
        fig, ax = plt.subplots(1, 1, figsize=(10, 7))
        axes = [ax]


    def plot_pool(ax, df, pool_label, current_tick):
        tick_min = current_tick - 15000
        tick_max = current_tick + 15000
        df_focus = df[(df['tickIdx'] >= tick_min) & (df['tickIdx'] <= tick_max)]
        num_ticks = len(df_focus)
        bar_width = ((tick_max - tick_min) / num_ticks * 0.9) if num_ticks > 0 else 1
        ax.bar(df_focus['tickIdx'], df_focus['plot_cumulative_liquidity'],
               width=bar_width, color='skyblue', align='center', label=pool_label)
        ax.axvline(x=current_tick, color='red', linestyle='--', linewidth=2)
        ax.text(current_tick, df_focus['plot_cumulative_liquidity'].max() * 0.9,
                "Current Tick", color='red', rotation=90, va='top', ha='right', fontsize=8)
        if cex_tick is not None:
            ax.axvline(x=cex_tick, color='green', linestyle='-', linewidth=2)
            ax.text(cex_tick, df_focus['plot_cumulative_liquidity'].max() * 0.9,
                    "CEX Tick", color='green', rotation=90, va='top', ha='right', fontsize=8)
        ax.set_title(pool_label)
        ax.set_xlabel("Tick Index")
        ax.set_ylabel("Cumulative Liquidity")




    if df_005 is not None and df_03 is not None:
        plot_pool(axes[0], df_005, "0.05 Pool", current_tick_005)
        plot_pool(axes[1], df_03, "0.3 Pool", current_tick_03)
    elif df_005 is not None:
        plot_pool(axes[0], df_005, "0.05 Pool", current_tick_005)
        fig.text(0.5, 0.01, "0.3 Pool not available; Wasserstein metric not available", 
                 ha='center', fontsize=12)
    elif df_03 is not None:
        plot_pool(axes[0], df_03, "0.3 Pool", current_tick_03)
        fig.text(0.5, 0.01, "0.05 Pool not available; Wasserstein metric not available", 
                 ha='center', fontsize=12)
    else:
        fig.text(0.5, 0.5, "No pool data available", ha='center', fontsize=14)
        plt.close(fig)
        return

    if df_005 is not None and df_03 is not None:
        wd_tick = wasserstein_distance(
            df_005['tickIdx'], df_03['tickIdx'],
            u_weights=df_005['plot_norm_liquidity'], v_weights=df_03['plot_norm_liquidity']
        )
        wd_price = wasserstein_distance(
            df_005['price'], df_03['price'],
            u_weights=df_005['plot_norm_liquidity'], v_weights=df_03['plot_norm_liquidity']
        )
        wd_message = f"Wasserstein (ticks): {wd_tick:.2f} | Wasserstein (prices): {wd_price:.2f}"
        fig.text(0.5, 0.01, wd_message, ha='center', fontsize=12)

    fig.legend([cex_msg], loc="upper right", fontsize=10)

    combined_chart_file = os.path.join(output_charts_path, f"liquidity_combined_{timestamp}.png")
    plt.tight_layout(rect=[0, 0.05, 1, 1])
    plt.savefig(combined_chart_file)
    print(f"Combined chart saved to '{combined_chart_file}'")
    plt.close(fig)

if __name__ == "__main__":

    cex_csv_dir = os.getenv('output_csv_path_USDC_ETH_cex')
    pool_csv_dir_005 = os.getenv('output_csv_path_USDC_ETH_0.05_Pool')
    pool_csv_dir_03 = os.getenv('output_csv_path_USDC_ETH_0.3_Pool')

    compare_liquidity_distributions(pool_csv_dir_005, pool_csv_dir_03, cex_csv_dir)
    
