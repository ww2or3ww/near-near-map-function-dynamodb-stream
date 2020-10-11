import sys
import json
import os
import re

from retry import retry
import boto3
from boto3.dynamodb.conditions import Key

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET_NAME          = ""    if("S3_BUCKET_NAME" not in os.environ)              else os.environ["S3_BUCKET_NAME"]
S3_PREFIX_OUT           = ""    if("S3_PREFIX_OUT" not in os.environ)               else os.environ["S3_PREFIX_OUT"]
ATHENA_DB_NAME          = ""    if("ATHENA_DB_NAME" not in os.environ)              else os.environ["ATHENA_DB_NAME"]
ATHENA_TABLE_NAME       = ""    if("ATHENA_TABLE_NAME" not in os.environ)           else os.environ["ATHENA_TABLE_NAME"]
ATHENA_OUTPUT_LOCATION  = ""    if("ATHENA_OUTPUT_LOCATION" not in os.environ)      else os.environ["ATHENA_OUTPUT_LOCATION"]

S3_RESOURCE             = boto3.resource('s3')
ATHENA                  = boto3.client("athena")

def lambda_handler(event, context):
    try:
        logger.info("====== START ======")

        for record in event["Records"]:
            logger.info(record)
            eventName = record["eventName"]
            if eventName == "INSERT":
                upload(record, True)
            if eventName == "MODIFY":
                upload(record, False)
            elif eventName == "REMOVE":
                remove(record)

    except Exception as e:
        logger.exception(e)

def upload(record, needAddPartition):
    try:
        data = getDataFronRecord(record, "NewImage")
        uploadData(data, needAddPartition)

    except Exception as e:
        logger.exception(e)

@retry(tries=3, delay=1)
def uploadData(data, needAddPartition):
    key_list = data["h3-9"].split("_")
    p_h3_9_key = os.path.join(S3_PREFIX_OUT, "p_type={0}".format(data["type"]), "p_h3_9={0}".format(key_list[0]))
    key = os.path.join(p_h3_9_key, "{0}.json".format(key_list[1]))

    # S3へデータアップロード
    s3_obj = S3_RESOURCE.Object(S3_BUCKET_NAME, key)
    s3_obj.put(Body = json.dumps(data))

    if needAddPartition:
        # パーティション設定
        location = "s3://" + S3_BUCKET_NAME + "/" + p_h3_9_key + "/"
        sql = "ALTER TABLE {0} ADD IF NOT EXISTS PARTITION (p_type = '{1}', p_h3_9 = '{2}') location '{3}';".format(ATHENA_TABLE_NAME, data["type"], key_list[0], location)
        logger.info(sql)
        ATHENA.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                "Database": ATHENA_DB_NAME
            },
            ResultConfiguration={
                "OutputLocation": ATHENA_OUTPUT_LOCATION
            }
        )

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
        data["h3-9"] =      record["dynamodb"][type]["h3-9"]["S"]
        data["h3-8"] =      record["dynamodb"][type]["h3-8"]["S"]
        data["h3-7"] =      record["dynamodb"][type]["h3-7"]["S"]
        data["h3-6"] =      record["dynamodb"][type]["h3-6"]["S"]
        data["title"] =     record["dynamodb"][type]["title"]["S"]
        data["tel"] =       record["dynamodb"][type]["tel"]["S"]
        data["latlon"] =    record["dynamodb"][type]["latlon"]["S"]
        data["address"] =   record["dynamodb"][type]["address"]["S"]
        data["homepage"] =  record["dynamodb"][type]["homepage"]["S"]
        data["facebook"] =  record["dynamodb"][type]["facebook"]["S"]
        data["instagram"] = record["dynamodb"][type]["instagram"]["S"]
        data["twitter"] =   record["dynamodb"][type]["twitter"]["S"]
        data["media1"] =    record["dynamodb"][type]["media1"]["S"]
        data["media2"] =    record["dynamodb"][type]["media2"]["S"]
        data["media3"] =    record["dynamodb"][type]["media3"]["S"]
        data["media4"] =    record["dynamodb"][type]["media4"]["S"]
        data["media5"] =    record["dynamodb"][type]["media5"]["S"]
        data["locoguide_id"] =          record["dynamodb"][type]["locoguide_id"]["S"]
        data["has_xframe_options"] =    record["dynamodb"][type]["has_xframe_options"]["S"]

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
