import sys
import json
import os
import re

from retry import retry
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_ES                 = ""    if("ENDPOINT_ES" not in os.environ)             else os.environ["ENDPOINT_ES"]

def lambda_handler(event, context):
    try:
        logger.info("====== START ======")
        logger.info(event)

        es = Elasticsearch(
            hosts=[{
                "host": ENDPOINT_ES,
                "port": 443
            }],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=1500
        )
        
        index = 0
        for record in event["Records"]:
            logger.info(record)
            eventName = record["eventName"]
            if eventName == "INSERT":
                insert(record, es)
            if eventName == "MODIFY":
                modify(record, es)
            elif eventName == "REMOVE":
                remove(record, es)
            index += 1

    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def insert(record, es):
    try:
        data = getDataFronRecord(record, "NewImage")
        logger.info("--- insert ---")
        logger.info(data)
        
        id = data["type"] + "-" + data["guid"]
        id = id.replace("-", "_")
        
        es.index(index="articles", id=id, body=data)
    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def modify(record, es):
    try:
        data = getDataFronRecord(record, "NewImage")
        logger.info("--- insert ---")
        logger.info(data)
        
        id = data["type"] + "-" + data["guid"]
        id = id.replace("-", "_")
        
        es.index(index="articles", id=id, body=data)
    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def remove(record, es):
    try:
        data = getDataFronRecord(record, "OldImage")
        logger.info("--- remove ---")
        logger.info(data)

        id = data["type"] + "-" + data["guid"]
        id = id.replace("-", "_")
        
        es.delete(index="articles", id=id)

    except Exception as e:
        logger.exception(e)

def getDataFronRecord(record, type):
    try:
        data = {}
        data["type"] =                  record["dynamodb"][type]["type"]["S"]
        data["guid"] =                  record["dynamodb"][type]["guid"]["S"]
        data["title"] =                 record["dynamodb"][type]["title"]["S"]
        data["tel"] =                   record["dynamodb"][type]["tel"]["S"]
        data["address"] =               record["dynamodb"][type]["address"]["S"]
        data["homepage"] =              record["dynamodb"][type]["homepage"]["S"]
        data["facebook"] =              record["dynamodb"][type]["facebook"]["S"]
        data["instagram"] =             record["dynamodb"][type]["instagram"]["S"]
        data["twitter"] =               record["dynamodb"][type]["twitter"]["S"]
        data["media1"] =                record["dynamodb"][type]["media1"]["S"]
        data["media2"] =                record["dynamodb"][type]["media2"]["S"]
        data["media3"] =                record["dynamodb"][type]["media3"]["S"]
        data["media4"] =                record["dynamodb"][type]["media4"]["S"]
        data["media5"] =                record["dynamodb"][type]["media5"]["S"]
        data["latlon"] =                record["dynamodb"][type]["latlon"]["S"]
        data["has_xframe_options"] =    record["dynamodb"][type]["has_xframe_options"]["S"]
        data["locoguide_id"] =          record["dynamodb"][type]["locoguide_id"]["S"]

        if "image" in record["dynamodb"][type]:
            data["image"] =     record["dynamodb"][type]["image"]["S"]
        else:
            data["image"] = ""

        if "star" in record["dynamodb"][type]:
            if "N" in record["dynamodb"][type]["star"]:
                data["star"] = record["dynamodb"][type]["star"]["N"]
            elif "S" in record["dynamodb"][type]["star"]:
                data["star"] = int(record["dynamodb"][type]["star"]["S"])
        if "star" not in data:
            data["star"] = 0

        return data
    except Exception as e:
        logger.exception(e)
        return None