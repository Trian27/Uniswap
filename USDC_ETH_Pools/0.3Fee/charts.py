import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os, re
from dotenv import load_dotenv

load_dotenv()

def get_max_liquidity(csv_files):
    max_liquidity = 0
    for _, filepath in csv_files:
        try:
            df = pd.read_csv(filepath)
            max_liquidity = max(max_liquidity, df['cumulative_liquidity'].astype(float).max())
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    return max_liquidity

def plot_liquidity_distribution(csv_file_path, max_liquidity):
    output_charts_path = os.getenv('output_charts_path_USDC_ETH_0.3_Pool')

    if not output_charts_path:
        print("Error: 'output_charts_path' not found in .env file.")
        return

    bar_charts_path = os.path.join(output_charts_path, 'barCharts')
    line_charts_path = os.path.join(output_charts_path, 'lineCharts')

    os.makedirs(bar_charts_path, exist_ok=True)
    os.makedirs(line_charts_path, exist_ok=True)

    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' does not exist.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{csv_file_path}' is empty.")
        return
    except Exception as e:
        print(f"Error reading '{csv_file_path}': {e}")
        return

    try:
        df['tickIdx'] = df['tickIdx'].astype(int)
        df['cumulative_liquidity'] = df['cumulative_liquidity'].astype(float)
    except KeyError as e:
        print(f"Error: Missing expected column {e} in CSV.")
        return
    except ValueError as e:
        print(f"Error: Data type conversion issue - {e}.")
        return

    df = df.sort_values('tickIdx')

    try:
        tickIdx = df['tickIdx']
        cumulative_liquidity = df['cumulative_liquidity']
        current_tick = int(df['current_tick'].iloc[0])
        timestamp = int(df['timestamp'].iloc[0])
        pool_id = df['pool_id'].iloc[0]
    except IndexError:
        print("Error: The CSV file does not contain any data rows.")
        return
    except KeyError as e:
        print(f"Error: Missing expected column {e} in CSV.")
        return
    except ValueError as e:
        print(f"Error: Data type conversion issue - {e}.")
        return

    try:
        dt = datetime.fromtimestamp(timestamp, tz=datetime.now().astimezone().tzinfo)
        time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error converting timestamp: {e}")
        time_str = "Unknown Time"

    # ----------------- Create Bar Chart (Centered around current tick, ±50,000) -----------------
    tick_min = current_tick - 15000
    tick_max = current_tick + 15000

    df_focus = df[(df['tickIdx'] >= tick_min) & (df['tickIdx'] <= tick_max)]

    if not df_focus.empty:
        plt.figure(figsize=(14, 7))
        
        num_ticks = len(df_focus)
        if num_ticks > 0:
            bar_width = (tick_max - tick_min) / num_ticks * 0.9
        else:
            bar_width = 1
        
        plt.bar(df_focus['tickIdx'], df_focus['cumulative_liquidity'], width=bar_width, color='skyblue', align='center')
        plt.ylim(bottom=0, top=max_liquidity)

        plt.axvline(x=current_tick, color='red', linestyle='--', linewidth=2, label='Current Tick')
        plt.text(current_tick, plt.ylim()[1]*0.95, 'Current Tick', color='red', rotation=90, va='top', ha='right')

        plt.title(f'Liquidity Distribution (Bar Chart)\nTimestamp: {time_str}\nPool ID: {pool_id}')
        plt.xlabel('Tick Index')
        plt.ylabel('Cumulative Liquidity')
        plt.legend()

        plt.tight_layout()

        bar_chart_file = os.path.join(bar_charts_path, f'liquidity_bar_chart_{timestamp}.png')
        try:
            plt.savefig(bar_chart_file)
            print(f"Bar chart saved to '{bar_chart_file}'")
        except Exception as e:
            print(f"Error saving bar chart: {e}")
        finally:
            plt.close()
    else:
        print(f"No data available in the specified range (tickIdx {tick_min} to {tick_max}) for the bar chart.")

    # ----------------- Create Line Chart (Full Data) -----------------
    plt.figure(figsize=(14, 7))
    plt.plot(tickIdx, cumulative_liquidity, color='blue', linewidth=1, label='Cumulative Liquidity')
    plt.ylim(bottom=0, top=max_liquidity)

    plt.axvline(x=current_tick, color='red', linestyle='--', linewidth=2, label='Current Tick')
    plt.text(current_tick, plt.ylim()[1]*0.95, 'Current Tick', color='red', rotation=90, va='top', ha='right')

    plt.title(f'Liquidity Distribution (Line Chart)\nTimestamp: {time_str}\nPool ID: {pool_id}')
    plt.xlabel('Tick Index')
    plt.ylabel('Cumulative Liquidity')
    plt.legend()

    plt.tight_layout()

    line_chart_file = os.path.join(line_charts_path, f'liquidity_line_chart_{timestamp}.png')
    try:
        plt.savefig(line_chart_file)
        print(f"Line chart saved to '{line_chart_file}'")
    except Exception as e:
        print(f"Error saving line chart: {e}")
    finally:
        plt.close()

if __name__ == "__main__":
    csv_dir = os.getenv('output_csv_path_USDC_ETH_0.3_Pool')
    print(f"CSV Directory: {csv_dir}")
    pattern = re.compile(r'liquidity_data_(\d+)\.csv')
    csv_files = []

    try:
        all_files = os.listdir(csv_dir)
    except FileNotFoundError:
        print(f"Error: The directory '{csv_dir}' does not exist.")
        exit(1)
    for filename in all_files:
        match = pattern.match(filename)
        if match:
            timestamp = int(match.group(1))
            filepath = os.path.join(csv_dir, filename)
            csv_files.append((timestamp, filepath))
    csv_files.sort()
    if not csv_files:
        print(f"No CSV files found in '{csv_dir}'.")
        exit(1)
    max_liquidity = get_max_liquidity(csv_files)
    for timestamp, filepath in csv_files:
        print(f"Processing file: {filepath}")
        plot_liquidity_distribution(filepath, max_liquidity)