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
        logging.exception(f"Failed to create AWS {service} client: {e}")
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
            logging.exception(f"Unexpected error deleting stream '{log_stream_name}': {e}")
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


def _debug_log_stream(log_stream):
    """Emit a per-stream DEBUG line without doing datetime work when DEBUG is off."""
    if not logging.getLogger().isEnabledFor(logging.DEBUG):
        return

    def _iso(ms):
        return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc).isoformat()

    created = _iso(log_stream["creationTime"]) if "creationTime" in log_stream else "N/A"
    last_event = _iso(log_stream["lastEventTimestamp"]) if "lastEventTimestamp" in log_stream else "N/A"
    logging.debug(f"Stream: {log_stream.get('logStreamName')}, Created: {created}, Last event: {last_event}")


def find_eligible_streams(client, log_group_name, retention_epoch, use_last_event):
    """Paginate log streams and return names of those older than the retention threshold."""
    logging.info("Scanning for eligible streams...")
    paginator = client.get_paginator("describe_log_streams")
    eligible_streams = []
    for page in paginator.paginate(logGroupName=log_group_name):
        for log_stream in page.get("logStreams", []):
            timestamp = get_stream_age_timestamp(log_stream, use_last_event)
            if timestamp is None:
                logging.warning(f"Skipping stream '{log_stream.get('logStreamName')}': missing timestamp")
                continue
            if timestamp < retention_epoch:
                stream_name = log_stream.get("logStreamName")
                if not stream_name:
                    logging.warning("Skipping stream with missing logStreamName")
                    continue
                _debug_log_stream(log_stream)
                eligible_streams.append(stream_name)
    return eligible_streams


def delete_eligible_streams(client, log_group_name, stream_names, *, dry_run, batch_size, batch_pause):
    """Delete the given streams in order, pausing between batches to ease API pressure."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if batch_pause < 0:
        raise ValueError("batch_pause must be non-negative")

    logging.info(f"Starting deletion of {len(stream_names)} streams...")
    deleted_count = 0
    total = len(stream_names)
    for i, stream_name in enumerate(stream_names, 1):
        if delete_stream(client, log_group_name, stream_name, dry_run):
            deleted_count += 1
        if batch_pause and i % batch_size == 0 and i < total:
            logging.debug(f"Processed {i} streams, pausing for {batch_pause}s...")
            time.sleep(batch_pause)
    return deleted_count


def process_log_streams(
    client,
    log_group_name,
    retention_epoch,
    *,
    use_last_event,
    dry_run,
    skip_confirmation,
    retention_days,
    batch_size,
    batch_pause,
):
    """Orchestrate scan, confirmation, and deletion of expired log streams."""
    try:
        eligible_streams = find_eligible_streams(client, log_group_name, retention_epoch, use_last_event)

        if not eligible_streams:
            logging.info("No streams found that meet the deletion criteria.")
            return 0

        if not skip_confirmation and not dry_run:
            if not confirm_deletion(log_group_name, retention_days, len(eligible_streams)):
                logging.info("Operation cancelled by user.")
                return 0

        return delete_eligible_streams(
            client,
            log_group_name,
            eligible_streams,
            dry_run=dry_run,
            batch_size=batch_size,
            batch_pause=batch_pause,
        )

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logging.exception(f"AWS error during stream processing: {error_code} - {e}")
        if error_code == "ResourceNotFoundException":
            logging.error(f"Log group '{log_group_name}' not found")
        sys.exit(1)
    except (OSError, BotoCoreError) as e:
        logging.exception(f"Failed to process log streams: {e}")
        sys.exit(1)


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

    log_group_name = args.log_group

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
    deleted_count = process_log_streams(
        client,
        log_group_name,
        retention_epoch,
        use_last_event=args.use_last_event,
        dry_run=args.dry_run,
        skip_confirmation=args.yes,
        retention_days=args.retention,
        batch_size=args.batch_size,
        batch_pause=args.batch_pause,
    )

    # Log summary
    if args.dry_run:
        logging.info(f"DRY RUN COMPLETE: {deleted_count} streams would have been deleted")
    else:
        logging.info(f"Operation complete: {deleted_count} streams deleted")


if __name__ == "__main__":
    main()
