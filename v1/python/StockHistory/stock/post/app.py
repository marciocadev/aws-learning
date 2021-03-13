import os
import boto3
from botocore.serialize import DEFAULT_TIMESTAMP_FORMAT
from botocore.exceptions import ClientError
import urllib3
import hashlib
import json
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")


def get_year_interval(last_day: date):

    year = last_day.year

    first_day = date(year, 1, 1)
    dt_ini = int(datetime.timestamp(datetime.fromordinal(first_day.toordinal())))

    dt_fim = int(datetime.timestamp(datetime.fromordinal(last_day.toordinal())))

    dt_ini_str = first_day.strftime("%Y-%m-%d")
    dt_fim_str = last_day.strftime("%Y-%m-%d")

    return dt_ini, dt_fim, dt_ini_str, dt_fim_str


def lambda_handler(event, context):

    sqs_url = os.environ.get("SQS_URL")

    queryStringParameters = event.get("queryStringParameters")
    if queryStringParameters is not None:
        stock = queryStringParameters.get("stock")

    dt_ini, dt_fim, first_day, last_day = get_year_interval(date.today())
    
    obj = {
        "stock": stock,
        "dt_ini": dt_ini,
        "dt_fim": dt_fim,
        "first_day": first_day,
        "last_day": last_day,
    }

    sqs = boto3.client("sqs")
    response = sqs.send_message(
        QueueUrl=sqs_url,
        # MessageAttributes={
        #     'stock': {
        #         'DataType': 'String',
        #         'StringValue': stock
        #     },
        #     'dt_ini': {
        #         'DataType': 'Number',
        #         'StringValue': str(dt_ini)
        #     },
        #     'dt_fim': {
        #         'DataType': 'Number',
        #         'StringValue': str(dt_fim)
        #     }
        # },
        MessageBody=json.dumps(obj)
    )

    return {
        "statusCode": 200,
        "body": json.dumps(obj)
    }