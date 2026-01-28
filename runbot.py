"""
Actually run the bot
"""
import os
import argparse
import logging

from lasair_needle_bot.bot import LasairNeedleBot

logger = logging.getLogger(__name__)

def main():

    FILE_DIR = os.path.dirname(os.path.realpath(__file__))

    p = argparse.ArgumentParser()
    p.add_argument(
        "--log", "-l",
        required=False,
        help="The filepath to write the log to",
        default=os.path.join(FILE_DIR, "lasair-needle-tde-slack-bot.log")
    )
    
    p.add_argument(
        "--test",
        action=argparse.BooleanOptionalAction,
        help="If true, just prints the message instead of sending it"
    )
    
    args = p.parse_args()

    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=args.log,
        encoding='utf-8',
        level=logging.DEBUG
    )

    logger.info("Initializing Slack Bot...")
    try:
        bot = LasairNeedleBot()
    except Exception as exc:
        logger.exception(exc)
        raise ValueError("Failed to initialize bot!") from exc

    logger.info("Finished initializing the bot, sending the message...")
    try:
        bot.send_slack_message(test=args.test)
    except Exception as e:
        logger.exception(e)

if __name__ == "__main__":
    main()
        
