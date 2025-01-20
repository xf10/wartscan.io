import os
import time
import requests
import utils
from logger import logger


class Notifier:
    def __init__(self):
        self.enabled = os.environ.get('NOTIFICATIONS_ENABLED')
        self.webhook = os.environ.get('NOTIFICATIONS_WEBHOOK_URL')

    def notify_general(self, title, text):
        self.send_notification(title, "https://wartscan.io", text)

    def send_notification(self, title, link, text):
        if self.enabled != "true":
            return
        wh = {"username": "Explorer", "content": "", "embeds": [{"title": title,
                                                                 "url": link,
                                                                 "description": text,
                                                                 "color": 15258703,
                                                                 "footer": {
                                                                     "text": utils.timestamp_to_datetime(
                                                                         round(time.time()))
                                                                 }}]}
        try:
            r = requests.post(self.webhook,json=wh)
        except Exception as e:
            logger.error(f"Error sending notification to webhook: {e}")
