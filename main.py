import argparse
import boto3
import datetime
import logging
import sys
import os
import time
from botocore.exceptions import ClientError, BotoCoreError


def parse_args():
    """Parse command line arguments with enhanced options."""
    parser = argparse.ArgumentParser(
        prog="cloudwatch-logstream-cleaner",
        description="Delete CloudWatch log streams older than a specified number of days",
    )
    parser.add_argument("-l", "--log-group", help="CloudWatch Log Group Name", required=True)
    parser.add_argument("-r", "--retention", type=int, help="Retention in days", required=True)
    parser.add_argument(
        "--verbose",
        help="Enable verbose mode",
        action="store_const",
        dest="loglevel",
        default=logging.INFO,
        const=logging.DEBUG,
    )
    parser.add_argument(
        "--dry-run",
        help="Simulate deletion without actually deleting streams",
        action="store_true",
    )
    parser.add_argument(
        "--region",
        help="AWS region to use (overrides AWS_REGION environment variable)",
        default=None,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of streams to process before pausing (to avoid API throttling)",
        default=100,
    )
    parser.add_argument(
        "--batch-pause",
        type=float,
        help="Seconds to pause between batches",
        default=0.5,
    )
    parser.add_argument(
        "--use-last-event",
        help="Use lastEventTimestamp instead of creationTime for age calculation",
        action="store_true",
    )
    parser.add_argument(
        "--yes",
        help="Skip confirmation prompt",
        action="store_true",
    )
    args = parser.parse_args()
    if args.retention < 0:
        parser.error("--retention must be non-negative")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    return args


def setup_logging(log_level):
    """Configure logging with a consistent format."""
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Reduce noise from boto3 and botocore
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_aws_client(service, region=None):
    """Create and return an AWS client with proper error handling."""
    try:
        if region:
            return boto3.client(service, region_name=region)
        return boto3.client(service)
    except (ClientError, BotoCoreError) as e:
        logging.error(f"Failed to create AWS {service} client: {e}")
        sys.exit(1)


def delete_stream(client, log_group_name, log_stream_name, dry_run=False, max_retries=5):
    """Delete a single log stream and log the result."""
    if dry_run:
        logging.info(f"[DRY RUN] Would delete stream: {log_stream_name}")
        return True

    for attempt in range(max_retries + 1):
        try:
            response = client.delete_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
            logging.debug(f"Deleted stream response: {response}")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "ResourceNotFoundException":
                logging.warning(f"Stream '{log_stream_name}' not found (may have been deleted already)")
                return False
            elif error_code == "ThrottlingException" and attempt < max_retries:
                wait_time = 2 ** (attempt + 1)
                logging.warning(f"AWS throttling detected. Retry {attempt + 1}/{max_retries} in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"AWS error deleting stream '{log_stream_name}': {error_code} - {e}")
                return False
        except (OSError, BotoCoreError) as e:
            logging.error(f"Unexpected error deleting stream '{log_stream_name}': {e}")
            return False

    return False


def confirm_deletion(log_group_name, retention_days, estimated_count=None):
    """Ask for user confirmation before proceeding with deletion."""
    message = f"You are about to delete log streams older than {retention_days} days "
    message += f"from log group '{log_group_name}'"

    if estimated_count is not None:
        message += f" (approximately {estimated_count} streams)"

    message += ".\nDo you want to continue? [y/N]: "

    response = input(message).strip().lower()
    return response in ("y", "yes")


def get_stream_age_timestamp(log_stream, use_last_event=False):
    """Determine the timestamp to use for age calculation. Returns None if missing."""
    if use_last_event and "lastEventTimestamp" in log_stream:
        return log_stream["lastEventTimestamp"]
    if "creationTime" in log_stream:
        return log_stream["creationTime"]
    return None


def process_log_streams(client, log_group_name, retention_epoch, args):
    """Process and delete log streams based on retention policy."""
    paginator = client.get_paginator("describe_log_streams")
    deleted_count = 0

    try:
        # Single pass: collect eligible stream names
        logging.info("Scanning for eligible streams...")
        eligible_streams = []
        for page in paginator.paginate(logGroupName=log_group_name):
            for log_stream in page.get("logStreams", []):
                timestamp = get_stream_age_timestamp(log_stream, args.use_last_event)
                if timestamp is None:
                    logging.warning(f"Skipping stream '{log_stream.get('logStreamName')}': missing timestamp")
                    continue
                if timestamp < retention_epoch:
                    log_stream_name = log_stream.get("logStreamName")
                    logging.debug(
                        f"Stream: {log_stream_name}, "
                        f"Created: {datetime.datetime.fromtimestamp(log_stream['creationTime'] / 1000, tz=datetime.timezone.utc).isoformat() if 'creationTime' in log_stream else 'N/A'}, "
                        f"Last event: {datetime.datetime.fromtimestamp(log_stream['lastEventTimestamp'] / 1000, tz=datetime.timezone.utc).isoformat() if 'lastEventTimestamp' in log_stream else 'N/A'}"
                    )
                    eligible_streams.append(log_stream_name)

        if not eligible_streams:
            logging.info("No streams found that meet the deletion criteria.")
            return 0

        # Confirm before deletion
        if not args.yes and not args.dry_run:
            if not confirm_deletion(log_group_name, args.retention, len(eligible_streams)):
                logging.info("Operation cancelled by user.")
                return 0

        # Delete collected streams
        logging.info(f"Starting deletion of {len(eligible_streams)} streams...")
        for i, stream_name in enumerate(eligible_streams, 1):
            if delete_stream(client, log_group_name, stream_name, args.dry_run):
                deleted_count += 1
            if i % args.batch_size == 0:
                logging.debug(f"Processed {i} streams, pausing for {args.batch_pause}s...")
                time.sleep(args.batch_pause)

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logging.error(f"AWS error during stream processing: {error_code} - {e}")
        if error_code == "ResourceNotFoundException":
            logging.error(f"Log group '{log_group_name}' not found")
        sys.exit(1)
    except (OSError, BotoCoreError) as e:
        logging.error(f"Failed to process log streams: {e}")
        sys.exit(1)

    return deleted_count


def main():
    args = parse_args()
    setup_logging(args.loglevel)

    # Determine region with precedence: arg > env var > default
    region = args.region or os.environ.get("AWS_REGION")
    client = get_aws_client("logs", region)

    # Calculate retention date in UTC
    date_now = datetime.datetime.now(datetime.timezone.utc)
    retention_datetime = date_now - datetime.timedelta(days=args.retention)
    retention_epoch = int(retention_datetime.timestamp() * 1000)  # milliseconds

    log_group_name = args.loggroup

    # Log execution parameters
    logging.info("CloudWatch Log Stream Cleaner")
    logging.info(f"Using AWS region: {region or 'default'}")
    logging.info(f"Log group name: {log_group_name}")
    logging.info(f"Retention threshold: {retention_datetime.isoformat()} UTC")
    logging.info(f"Log streams older than {args.retention} days will be deleted")
    logging.info(f"Using {'lastEventTimestamp' if args.use_last_event else 'creationTime'} for age calculation")

    if args.dry_run:
        logging.info("DRY RUN MODE: No streams will actually be deleted")

    # Process and delete streams
    deleted_count = process_log_streams(client, log_group_name, retention_epoch, args)

    # Log summary
    if args.dry_run:
        logging.info(f"DRY RUN COMPLETE: {deleted_count} streams would have been deleted")
    else:
        logging.info(f"Operation complete: {deleted_count} streams deleted")


if __name__ == "__main__":
    main()
