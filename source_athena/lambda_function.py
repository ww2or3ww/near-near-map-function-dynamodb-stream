import sys
import json
import os
import re

from retry import retry

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        logger.info("====== START ======")
        logger.info(event)

        index = 0
        for record in event["Records"]:
            logger.info(record)
            eventName = record["eventName"]
            if eventName == "INSERT":
                insert(record)
            if eventName == "MODIFY":
                modify(record)
            elif eventName == "REMOVE":
                remove(record)
            index += 1

    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def insert(record):
    try:
        data = getDataFronRecord(record, "NewImage")
        logger.info("--- insert ---")
        logger.info(data)
        
        id = data["type"] + "-" + data["guid"]
        id = id.replace("-", "_")
        
    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def modify(record):
    try:
        data = getDataFronRecord(record, "NewImage")
        logger.info("--- insert ---")
        logger.info(data)
        
        id = data["type"] + "-" + data["guid"]
        id = id.replace("-", "_")
        
    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def remove(record):
    try:
        data = getDataFronRecord(record, "OldImage")
        logger.info("--- remove ---")
        logger.info(data)

        id = data["type"] + "-" + data["guid"]
        id = id.replace("-", "_")

    except Exception as e:
        logger.exception(e)

def getDataFronRecord(record, type):
    try:
        data = {}
        data["type"] =      record["dynamodb"][type]["type"]["S"]
        data["guid"] =      record["dynamodb"][type]["guid"]["S"]
        data["title"] =     record["dynamodb"][type]["title"]["S"]
        data["tel"] =       record["dynamodb"][type]["tel"]["S"]
        data["address"] =   record["dynamodb"][type]["address"]["S"]
        data["homepage"] =  record["dynamodb"][type]["homepage"]["S"]
        data["facebook"] =    record["dynamodb"][type]["facebook"]["S"]
        data["instagram"] =  record["dynamodb"][type]["instagram"]["S"]
        data["twitter"] =   record["dynamodb"][type]["twitter"]["S"]
        data["media1"] =    record["dynamodb"][type]["media1"]["S"]
        data["media2"] =    record["dynamodb"][type]["media2"]["S"]
        data["media3"] =    record["dynamodb"][type]["media3"]["S"]
        data["media4"] =    record["dynamodb"][type]["media4"]["S"]
        data["media5"] =    record["dynamodb"][type]["media5"]["S"]
        if "image" in record["dynamodb"][type]:
            data["image"] =     record["dynamodb"][type]["image"]["S"]
        else:
            data["image"] = ""
        data["has_xframe_options"] = record["dynamodb"][type]["has_xframe_options"]["S"]
        data["locoguide_id"] = record["dynamodb"][type]["locoguide_id"]["S"]
        
        data["latlon"] = record["dynamodb"][type]["latlon"]["S"]
        
        if "star" in record["dynamodb"][type]:
            data["star"] = record["dynamodb"][type]["star"]["N"]

        return data
    except Exception as e:
        logger.exception(e)
        return None
