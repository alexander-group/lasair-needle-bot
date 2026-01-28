"""
Configuration variables
"""
import os

KAFKA_SERVER = "kafka.lsst.ac.uk:9092"
GROUP_ID = "nfranz-test-slack-send"
TOPIC = "lasair_1827NoahFranzLasairNEEDLEFilter"

BOT_NAME = "Lasair Needle TDE Bot"
CHANNEL = "alexander-group"
SLACK_BOT_TOKEN = os.environ.get("LASAIR_SLACK_BOT_TOKEN", None)
if SLACK_BOT_TOKEN is None:
    raise ValueError("You have not set the environment variable 'TNS_SLACK_BOT_TOKEN'")
