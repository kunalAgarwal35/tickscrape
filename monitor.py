import discord_webhook
import os
import time
import threading
from datetime import datetime
import json

config = json.load(open("config.json", "r"))


TICK_DIR = config.get("tick_dir")
VENV_DIR = config.get("venv_dir")
SCRIPT_DIR = config.get("script_dir")

WEBHOOK = config.get("alert_webhook")


def get_latest_updated_file_timestamp_from_directory(file_dir):
    """Get the latest updated file timestamp from a directory"""
    files = os.listdir(file_dir)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(file_dir, x)))
    try:
        return os.path.getmtime(os.path.join(file_dir, files[-1]))
    except IndexError:
        return time.time()


def main():
    file_dir = TICK_DIR
    last_message_timestamp = 0

    while True:
        latest_updated_file_timestamp = get_latest_updated_file_timestamp_from_directory(
            file_dir)
        if (time.time() - latest_updated_file_timestamp >= 300) and time.time() - last_message_timestamp >= 600:
            webhook = WEBHOOK
            wbhk = discord_webhook.DiscordWebhook(url=webhook, content="No new tick data for 5 minutes. Last updated at {}".format(
                datetime.fromtimestamp(latest_updated_file_timestamp).strftime('%Y-%m-%d %H:%M:%S')))

            wbhk.execute()

            # Update last message timestamp
            last_message_timestamp = time.time()

            # Restart the python script
            thread = threading.Thread(target=os.system, args=(
                VENV_DIR + " " + SCRIPT_DIR, ), name='tickdump-{}'.format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S')))

            thread.start()
            time.sleep(10)

        time.sleep(10)


if __name__ == '__main__':
    main()
