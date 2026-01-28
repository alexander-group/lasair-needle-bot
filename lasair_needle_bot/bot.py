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
            host_sentence = f"\tThis transient is {row.host_separation}\" from {row.host_table_name} {row.host_object_id}.\n"
            if row.host_z != 0:
                host_sentence += f">\tThis host is at a {row.host_ztype} of {row.host_z}"
                if row.host_ztype == "photo-z":
                    host_sentence += f" +/- {row.host_zerr}"
            else:
                host_sentence += ">\tThis host does not have a know redshift in Lasair Sherlock. Check SAGUARO!"

            tns_name = ""
            if not pd.isna(row.tns_name):
                tns_name = f"(TNS Name: <https://wis-tns.org/object/{row.tns_name}|{row.tns_classification} {row.tns_name}>)" 
            
            out += f"""
>_Name_: {row.objectId} {tns_name}
>\t(ra, dec) = ({row.ramean}, {row.decmean})
>{host_sentence}
>\tP(TDE) = {row.p_tde}
>\tP(SLSN) = {row.p_slsn}
>\tDays since Discovery = {row.days_disc}
>\tLatest {row.band} = {row.mag_latest}
>\tg-r = {row.g_minus_r}
>\tNEEDLE Classification from {row.timestamp}
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
        
        
