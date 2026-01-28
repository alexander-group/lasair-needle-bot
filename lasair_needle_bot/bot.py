"""
A class for the actual Lasair bot
"""

from . import config
import json
from slack_sdk import WebClient

from lasair import lasair_consumer
import numpy as np
import pandas as pd

import logging

logger = logging.getLogger(__name__)

class LasairNeedleBot(WebClient):

    def __init__(self, **kwargs):
        self.kafka_server = config.KAFKA_SERVER
        self.group_id = config.GROUP_ID
        self.topic = config.TOPIC

        self.slack_channel = config.CHANNEL
        self.slack_uname = config.BOT_NAME
        
        super().__init__(token=config.SLACK_BOT_TOKEN, **kwargs)

    def check_kafka_stream(self, limit=np.inf):

        consumer = lasair_consumer(
            self.kafka_server,
            self.group_id,
            self.topic
        )
        
        new_msgs = []
        n = 0
        while n < limit:
            msg = consumer.poll(timeout=20)
            if msg is None:
                break
            if msg.error():
                logger.error(str(msg.error()))
                break
            jmsg = json.loads(msg.value())
            new_msgs.append(jmsg)
            n += 1
            
        return pd.DataFrame(new_msgs)

    def generate_slack_msg(self, stream_dataframe):

        out = """
*New NEEDLE TDE Classifications from Lasair*
*##########################################*
\n"""
        for i, row in stream_dataframe.iterrows():
            out += f"""
>_Name_: {row.objectId}
>\tP(TDE) = {row.p_tde}
>\tDays since Discovery = {row.days_disc}
>\tLatest {row.band} = {row.mag_latest}
>\tg-r = {row.g_minus_r}
>\tOther Notes: {row.explanation}
>\tClassified On {row.UTC}
>\t<https://lasair-ztf.lsst.ac.uk/objects/{row.objectId}/|Lasair>
>\t<https://sand.as.arizona.edu/saguaro_tom/targets/search/?name={row.objectId}|SAGUARO>
"""
        return out
    
    def send_slack_message(self, test=False):
        stream_df = self.check_kafka_stream()

        if not len(stream_df):
            logger.info(f"Found no new messages since the last listener run!")
            return
        
        msg = self.generate_slack_msg(stream_df)
        logger.info(msg)

        if test:
            return

        self.chat_postMessage(
            channel = self.slack_channel,
            text = msg,
            username = self.slack_uname
        )
        logger.info("Sent the new Lasair NEEDLE classifications successfully!")
        
        
