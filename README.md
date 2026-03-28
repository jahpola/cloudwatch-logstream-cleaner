# CloudWatch Log Stream Cleaner

A utility to delete CloudWatch log streams older than a specified number of days.

## Features

- Delete log streams based on creation time or last event time
- Dry run mode to simulate deletion without actually removing streams
- Confirmation prompt before deletion (can be skipped with `--yes`)
- Batch processing with configurable pause to avoid API throttling
- Detailed logging with verbose mode option
- Robust error handling for AWS API interactions

## Usage

```bash
uv run main.py -l <log-group-name> -r <retention-days> [options]
```

### Required Arguments

- `-l, --loggroup`: CloudWatch Log Group Name
- `-r, --retention`: Retention period in days

### Optional Arguments

- `--dry-run`: Simulate deletion without actually deleting streams
- `--region`: AWS region to use (overrides AWS_REGION environment variable)
- `--batch-size`: Number of streams to process before pausing (default: 100)
- `--batch-pause`: Seconds to pause between batches (default: 0.5)
- `--use-last-event`: Use lastEventTimestamp instead of creationTime for age calculation
- `--yes`: Skip confirmation prompt
- `--verbose`: Enable verbose logging

## Examples

Delete log streams older than 30 days:
```bash
uv run main.py -l /aws/lambda/my-function -r 30
```

Dry run to see what would be deleted:
```bash
uv run main.py -l /aws/lambda/my-function -r 30 --dry-run
```

Use last event time instead of creation time:
```bash
uv run main.py -l /aws/lambda/my-function -r 30 --use-last-event
```

Skip confirmation prompt:
```bash
uv run main.py -l /aws/lambda/my-function -r 30 --yes
```

## AWS Credentials

The tool uses boto3 and follows the standard AWS credential resolution:
1. Command line arguments
2. Environment variables
3. Shared credential file (~/.aws/credentials)
4. AWS config file (~/.aws/config)
5. IAM role for Amazon EC2 or ECS task role

## Requirements

- [uv](https://docs.astral.sh/uv/)
- Python 3.10+
- boto3