import json
import logging
import boto3
import pandas as pd
from tabulate import tabulate

db_client = boto3.client('rds')
s3 = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# import requests


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e
    logging.info("Hello World Gopi {}", event)
    bucketname = 'ensemblevc-rawdata'
    filename = 'ipos.csv'

    #bucket_name = event['Records'][0]['s3']['bucket']['name']
    #file_key = event['Records'][0]['s3']['object']['key']
    csv_file = s3.get_object(Bucket=bucketname, Key=filename)
    logging.info("Started Reading File")
    df = pd.read_csv(csv_file, skip_blank_lines=True)
    logging.info("Completed Reading File")
    logging.info(tabulate(df, tablefmt="fancy_grid"))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }
