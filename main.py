import argparse
import boto3
import datetime
import logging
import sys
from botocore.exceptions import ClientError


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


def delete_stream(client, log_group_name: str, log_stream_name: str) -> bool:
    """Delete a single log stream."""
    try:
        client.delete_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        return True
    except ClientError as e:
        logging.error(f"Error deleting stream '{log_stream_name}': {e}")
        return False


def main():
    args = parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s %(levelname)s: %(message)s")

    try:
        client = boto3.client("logs")
        retention_epoch = int((datetime.datetime.now(datetime.timezone.utc) - 
                             datetime.timedelta(days=args.retention)).timestamp() * 1000)
        
        logging.info(f"Deleting streams older than {args.retention} days from {args.loggroup}")
        
        deleted_count = 0
        paginator = client.get_paginator("describe_log_streams")
        
        for page in paginator.paginate(logGroupName=args.loggroup):
            for stream in page.get("logStreams", []):
                if stream.get("creationTime", 0) < retention_epoch:
                    if delete_stream(client, args.loggroup, stream["logStreamName"]):
                        deleted_count += 1
        
        logging.info(f"Deleted {deleted_count} streams")
        
    except ClientError as e:
        logging.error(f"AWS error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
