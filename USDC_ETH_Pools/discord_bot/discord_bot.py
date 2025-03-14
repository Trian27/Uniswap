import discord 
import subprocess
import csv
import os
import logging
from dotenv import load_dotenv
import time

load_dotenv()

TOKEN = os.getenv('discord_bot_token')
CHANNEL_ID = os.getenv('discord_channel_id')
CE_CURR_PRICE_TRACKER_PATH = os.getenv('cex_curr_price_tracker_path')
GRAPH_QUERIES_0_05_PATH = os.getenv('USDC_ETH_0.05_Pool_graph_queries')
GRAPH_QUERIES_0_3_PATH = os.getenv('USDC_ETH_0.3_Pool_graph_queries')

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

csv_file = 'webhook_timestamps.csv'

if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'author', 'content'])

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('discord_bot.log', encoding='utf-8', mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

@client.event
async def on_ready():
    logger.info("Bot is now ready.")

@client.event
async def on_message(message):
    
    logger.info("Message received.")
    if str(message.channel.id) == CHANNEL_ID:
    
        timestamp = int(message.created_at.timestamp())
        author = str(message.author)
        content = message.content

        # Write a new row to the CSV file for each message.
        with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, author, content])
        
        try:
            subprocess.run(['python3', CE_CURR_PRICE_TRACKER_PATH], check=True)
            subprocess.run(['python3', GRAPH_QUERIES_0_05_PATH], check=True)
            subprocess.run(['python3', GRAPH_QUERIES_0_3_PATH], check=True)
            logger.info("Successfully triggered external scripts.")
        except Exception as e:
            logger.error(f"Error triggering external scripts: {e}")


client.run(TOKEN)