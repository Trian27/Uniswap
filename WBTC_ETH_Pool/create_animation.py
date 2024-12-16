import imageio.v2 as imageio
import os
from dotenv import load_dotenv
import re

load_dotenv()

def create_bar_chart_animation():
    # Get paths from env
    charts_dir = os.path.join(os.getenv('output_charts_path_WBTC_ETH_Pool'), 'barCharts')
    output_dir = os.getenv('output_charts_path_WBTC_ETH_Pool')

    # Get all PNG files and sort by timestamp
    pattern = re.compile(r'liquidity_bar_chart_(\d+)\.png')
    png_files = []
    
    for filename in os.listdir(charts_dir):
        match = pattern.match(filename)
        if match:
            timestamp = int(match.group(1))
            filepath = os.path.join(charts_dir, filename)
            png_files.append((timestamp, filepath))
    
    png_files.sort()  # Sort by timestamp

    if not png_files:
        print("No PNG files found")
        return

    # Read all images
    images = []
    for _, filepath in png_files:
        images.append(imageio.imread(filepath))

    # Create animation
    output_path = os.path.join(output_dir, 'liquidity_animation.gif')
    imageio.mimsave(output_path, images, duration=1)  # 1 second per frame
    print(f"Animation saved to {output_path}")

if __name__ == "__main__":
    create_bar_chart_animation()