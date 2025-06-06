import argparse
import boto3
import datetime
import logging

client = boto3.client("logs", region_name="eu-north-1")
paginator = client.get_paginator("describe_log_streams")

parser = argparse.ArgumentParser(
    prog="cloudwatch-logstream-cleaner",
    description="Delete CloudWatch log streams older than a specified number of days",
)

parser.add_argument("-l", "--loggroup", help="CloudWatch Log Group Name", required=True)
parser.add_argument("-r", "--retention", type=int, help="Retention in days", required=True)
parser.add_argument(
    "--verbose",
    help="Enable verbose mode",
    action="store_const",
    dest="loglevel",
    default=logging.INFO,
    const=logging.DEBUG,
)

args = parser.parse_args()

# Date handling
date_now = datetime.datetime.now()
retention = args.retention
retention_datetime = date_now - datetime.timedelta(days=retention)
retention_epoch = int(retention_datetime.timestamp()) * 1000  # milliseconds

logGroupName = args.loggroup


def delete_stream(logGroupName, logStreamName):
    try:
        response = client.delete_log_stream(logGroupName=logGroupName, logStreamName=logStreamName)
        logging.debug(response)
    except Exception as e:
        logging.error("Error deleting stream: " + logStreamName)
        logging.error(e)


page_iterator = paginator.paginate(
    logGroupName=logGroupName,
)

logging.info("Deleting streams older than: " + str(retention_datetime))
logging.info("Log group name: " + logGroupName)
logging.info("Log streams older than " + str(retention) + " days will be deleted")

n = 0
for page in page_iterator:
    log_streams = page["logStreams"]
    for log_stream in log_streams:
        if log_stream["creationTime"] < retention_epoch:
            n += 1
            logging.info("Deleting stream: " + log_stream["logStreamName"])
            delete_stream(logGroupName, log_stream["logStreamName"])


logging.info("Total streams deleted: " + str(n))
