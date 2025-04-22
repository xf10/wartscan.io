from discord.ext import tasks, commands
import requests

# HOST = "https://wart.0xf10.com"
HOST = "http://explorer:5000"


class Stats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.stats.start()

        self.height_channel = None
        self.hashrate_channel = None
        self.taccs_channel = None
        self.ttxs_channel = None
        self.supply_channel = None
        self.price_channel = None
        self.marketcap_channel = None

    def cog_unload(self):
        self.stats.cancel()

    @tasks.loop(seconds=360)
    async def stats(self):
        print("UPDATING")
        price = 0
        try:
            r = requests.get(HOST + "/api/v1/stats/height")
            height = int(r.text)
            r = requests.get(HOST + "/api/v1/stats/hashrate")
            hashrate = float(r.text)
            r = requests.get(HOST + "/api/v1/stats/totalaccounts")
            taccs = int(r.text)
            r = requests.get(HOST + "/api/v1/stats/totaltxs")
            ttxs = int(r.text)
            r = requests.get(HOST + "/api/v1/stats/totalsupply")
            ts = int(r.text)
            r = requests.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=warthog&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&precision=2",
                timeout=5).json()
            price = round(float(r["warthog"]["usd"]), 2)
        except Exception as e:
            print(e)
            return

        marketcap = round(price * ts)

        try:
            self.height_channel = await self.height_channel.edit(name=f"Height: {height:,}")
            self.hashrate_channel = await self.hashrate_channel.edit(name=f"Hashrate: {hashrate} TH/s")
            self.taccs_channel = await self.taccs_channel.edit(name=f"Accounts: {taccs:,}")
            self.ttxs_channel = await self.ttxs_channel.edit(name=f"Transactions: {ttxs:,}")
            self.supply_channel = await self.supply_channel.edit(name=f"Supply: {ts:,}")
            self.price_channel = await self.price_channel.edit(name=f"Price: ${price}")
            self.marketcap_channel = await self.marketcap_channel.edit(name=f"Marketcap: ${marketcap:,}")
        except Exception as e:
            print(e)

    @stats.before_loop
    async def before_stats(self):
        print('waiting...')
        await self.bot.wait_until_ready()

        self.height_channel = self.bot.get_channel(1184874382133297303)
        self.hashrate_channel = self.bot.get_channel(1184870032728150116)
        self.taccs_channel = self.bot.get_channel(1184870410479730708)
        self.ttxs_channel = self.bot.get_channel(1184870464905039982)
        self.supply_channel = self.bot.get_channel(1184870489513009303)
        self.price_channel = self.bot.get_channel(1184870513634459688)
        self.marketcap_channel = self.bot.get_channel(1184870534618554370)
