import argparse
import boto3
import datetime
import logging
import sys
import os


def parse_args():
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
    return parser.parse_args()


def delete_stream(client, log_group_name, log_stream_name):
    """Delete a single log stream and log the result."""
    try:
        response = client.delete_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        logging.debug(f"Deleted stream response: {response}")
        return True
    except Exception as e:
        logging.error(f"Error deleting stream '{log_stream_name}': {e}")
        return False


def main():
    args = parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s %(levelname)s: %(message)s")

    region = os.environ.get("AWS_REGION", "eu-north-1")
    client = boto3.client("logs", region_name=region)
    paginator = client.get_paginator("describe_log_streams")

    # Use UTC for consistency?
    date_now = datetime.datetime.now(datetime.timezone.utc)
    retention_datetime = date_now - datetime.timedelta(days=args.retention)
    retention_epoch = int(retention_datetime.timestamp() * 1000)  # milliseconds

    log_group_name = args.loggroup

    logging.info(f"Using AWS region: {region}")
    logging.info(f"Deleting streams older than: {retention_datetime.isoformat()} UTC")
    logging.info(f"Log group name: {log_group_name}")
    logging.info(f"Log streams older than {args.retention} days will be deleted")
    deleted_count = 0
    try:
        page_iterator = paginator.paginate(logGroupName=log_group_name)
        for page in page_iterator:
            for log_stream in page.get("logStreams", []):
                if log_stream.get("creationTime", 0) < retention_epoch:
                    log_stream_name = log_stream.get("logStreamName")
                    logging.info(f"Deleting stream: {log_stream_name}")
                    if delete_stream(client, log_group_name, log_stream_name):
                        deleted_count += 1
    except Exception as e:
        logging.error(f"Failed to paginate or delete log streams: {e}")
        sys.exit(1)

    logging.info(f"Total streams deleted: {deleted_count}")


if __name__ == "__main__":
    main()
