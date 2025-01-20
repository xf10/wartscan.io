import discord
import os
from datetime import datetime
import requests
from channel_stats import Stats

bot = discord.Bot()
bot.add_cog(Stats(bot))

@bot.event
async def on_ready():
    print(f"We have logged in as {bot.user}")

bot.run(os.environ['BOT_TOKEN'])
