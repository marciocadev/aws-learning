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

resource_db = boto3.resource("dynamodb")

http = urllib3.PoolManager()

class NoneException(Exception):
    """None exception"""
    
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def get_historical_data_day_by_day(
        stock: str, dt_ini: float, dt_fim: float,
        url: str, headers: dict
    ):

    querystring = {
        "period1": dt_ini,
        "period2": dt_fim,
        "symbol": stock,
        "frequency": "1d",
        "filter": "history"
    }

    response = http.request("GET", url, headers=json.loads(headers), fields=querystring)

    if len(response.data) == 0:
        raise NoneException("")

    if response.status != 200:
        logger.warning("status {}".format(str(response.status)))
        

    return json.loads(response.data.decode('utf-8'))
        

def persist_price(table_name: str, stock: str, stock_dict: dict):
    table = resource_db.Table(table_name)

    for key in stock_dict.keys():
        if type(stock_dict[key]) == float or type(stock_dict[key]) == int:
            stock_dict[key] = Decimal(str(stock_dict[key]))

    with table.batch_writer() as batch:
        dt = date.fromtimestamp(stock_dict.get("date")).strftime("%Y-%m-%d")

        hash_key = {
            "Stock": stock,
            "Date": dt
        }
        hash = hashlib.md5(json.dumps(hash_key).encode())

        batch.put_item(
            Item={
                "Hash": hash.hexdigest(),
                "Date": dt,
                "Stock": stock,
                "Item": stock_dict
            }
        )


def persist_event(table_dividend_name: str, table_split_name: str, 
                    stock: str, split_dict: dict):
    table_dividend = resource_db.Table(table_dividend_name)
    table_split = resource_db.Table(table_split_name)

    for key in split_dict.keys():
        if type(split_dict[key]) == float or type(split_dict[key]) == int:
            split_dict[key] = Decimal(str(split_dict[key]))

    dt = date.fromtimestamp(split_dict.get("date")).strftime("%Y-%m-%d")

    hash_key = {
        "Stock": stock,
        "Date": dt
    }
    hash = hashlib.md5(json.dumps(hash_key).encode())

    if split_dict.get("type") == "DIVIDEND":
        table_dividend.put_item(
            Item={
                "Hash": hash.hexdigest(),
                "Date": dt,
                "Stock": stock,
                "Item": split_dict
            }
        )
    else:
        table_split.put_item(
            Item={
                "Hash": hash.hexdigest(),
                "Date": dt,
                "Stock": stock,
                "Item": split_dict
            }
        )

def get_year_interval(last_day: date):

    year = last_day.year

    first_day = date(year, 1, 1)
    dt_ini = int(datetime.timestamp(datetime.fromordinal(first_day.toordinal())))

    dt_fim = int(datetime.timestamp(datetime.fromordinal(last_day.toordinal())))

    dt_ini_str = first_day.strftime("%Y-%m-%d")
    dt_fim_str = last_day.strftime("%Y-%m-%d")

    return dt_ini, dt_fim, dt_ini_str, dt_fim_str

def get_header_from_secret(secret_name: str, region_name: str):

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        return get_secret_value_response['SecretString']

def lambda_handler(event, context):

    table_price_name = os.environ.get("TABLE_PRICE_NAME")
    table_dividend_name = os.environ.get("TABLE_DIVIDEND_NAME")
    table_split_name = os.environ.get("TABLE_SPLIT_NAME")
    region_name = os.environ.get("REGION")
    secret_name = os.environ.get("SECRET")
    sqs_url = os.environ.get("SQS_URL")

    headers = get_header_from_secret(
        secret_name=secret_name,
        region_name=region_name
    )

    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-historical-data"

    records = event.get("Records")
    body = json.loads(records[0].get("body"))

    dt_ini = body.get("dt_ini")
    dt_fim = body.get("dt_fim")
    stock_name = body.get("stock")
    first_day = body.get("first_day")
    last_day = body.get("last_day")

    logger.info("stock {stock} first_day {dt_ini} last_day {dt_fim}".format(stock=stock_name, dt_ini=first_day, dt_fim=last_day))
    
    data = get_historical_data_day_by_day(
        stock=stock_name,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        url=url,
        headers=headers
    )

    price = data.get("prices")

    if price is None or len(price) <= 0:
        logger.info("Finalizou stock {stock}".format(stock=stock_name))
        return

    for item in price:
        persist_price(table_price_name, stock_name, item)

    events_data = data.get("eventsData")
    for event in events_data:
        persist_event(table_dividend_name, table_split_name, stock_name, event)


    last_year_day = datetime.fromtimestamp(dt_ini) - timedelta(days=1)
    dt_ini, dt_fim, first_day, last_day = get_year_interval(last_year_day)

    obj = {
        "stock": stock_name,
        "dt_ini": dt_ini,
        "dt_fim": dt_fim,
        "first_day": first_day,
        "last_day": last_day,
    }

    sqs = boto3.client("sqs")
    response = sqs.send_message(
        QueueUrl=sqs_url,
        MessageBody=json.dumps(obj)
    )
