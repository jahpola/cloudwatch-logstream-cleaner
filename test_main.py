import unittest
from unittest.mock import patch, MagicMock
import datetime

import main


class TestCloudWatchLogStreamCleaner(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = self.mock_paginator

        # Setup mock log streams
        self.now = datetime.datetime.now(datetime.timezone.utc)
        self.now_ms = int(self.now.timestamp() * 1000)

        # Stream created 40 days ago, last event 35 days ago
        self.old_stream = {
            "logStreamName": "old-stream",
            "creationTime": self.now_ms - (40 * 24 * 60 * 60 * 1000),
            "lastEventTimestamp": self.now_ms - (35 * 24 * 60 * 60 * 1000),
        }

        # Stream created 20 days ago, last event 5 days ago
        self.mixed_stream = {
            "logStreamName": "mixed-stream",
            "creationTime": self.now_ms - (20 * 24 * 60 * 60 * 1000),
            "lastEventTimestamp": self.now_ms - (5 * 24 * 60 * 60 * 1000),
        }

        # Stream created 5 days ago, last event 2 days ago
        self.new_stream = {
            "logStreamName": "new-stream",
            "creationTime": self.now_ms - (5 * 24 * 60 * 60 * 1000),
            "lastEventTimestamp": self.now_ms - (2 * 24 * 60 * 60 * 1000),
        }

        # Mock paginator response
        self.mock_paginator.paginate.return_value = [
            {"logStreams": [self.old_stream, self.mixed_stream, self.new_stream]}
        ]

    def test_get_stream_age_timestamp_creation_time(self):
        """Test that get_stream_age_timestamp returns creationTime when use_last_event is False"""
        timestamp = main.get_stream_age_timestamp(self.old_stream, use_last_event=False)
        self.assertEqual(timestamp, self.old_stream["creationTime"])

    def test_get_stream_age_timestamp_last_event(self):
        """Test that get_stream_age_timestamp returns lastEventTimestamp when use_last_event is True"""
        timestamp = main.get_stream_age_timestamp(self.old_stream, use_last_event=True)
        self.assertEqual(timestamp, self.old_stream["lastEventTimestamp"])

    @patch("main.delete_stream")
    def test_process_log_streams_creation_time(self, mock_delete_stream):
        """Test processing streams based on creation time"""
        mock_delete_stream.return_value = True

        # Create mock args
        mock_args = MagicMock()
        mock_args.retention = 30
        mock_args.use_last_event = False
        mock_args.dry_run = False
        mock_args.yes = True
        mock_args.batch_size = 100
        mock_args.batch_pause = 0

        # Calculate retention epoch (30 days ago)
        retention_datetime = self.now - datetime.timedelta(days=30)
        retention_epoch = int(retention_datetime.timestamp() * 1000)

        # Call the function
        deleted_count = main.process_log_streams(self.mock_client, "test-log-group", retention_epoch, mock_args)

        # Only the old stream should be deleted (created 40 days ago)
        self.assertEqual(deleted_count, 1)
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "old-stream", False)

    @patch("main.delete_stream")
    def test_process_log_streams_last_event(self, mock_delete_stream):
        """Test processing streams based on last event time"""
        mock_delete_stream.return_value = True

        # Create mock args
        mock_args = MagicMock()
        mock_args.retention = 30
        mock_args.use_last_event = True
        mock_args.dry_run = False
        mock_args.yes = True
        mock_args.batch_size = 100
        mock_args.batch_pause = 0

        # Calculate retention epoch (30 days ago)
        retention_datetime = self.now - datetime.timedelta(days=30)
        retention_epoch = int(retention_datetime.timestamp() * 1000)

        # Call the function
        deleted_count = main.process_log_streams(self.mock_client, "test-log-group", retention_epoch, mock_args)

        # Only the old stream should be deleted (last event 35 days ago)
        self.assertEqual(deleted_count, 1)
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "old-stream", False)

    @patch("main.delete_stream")
    def test_process_log_streams_dry_run(self, mock_delete_stream):
        """Test dry run mode"""
        mock_delete_stream.return_value = True

        # Create mock args
        mock_args = MagicMock()
        mock_args.retention = 30
        mock_args.use_last_event = False
        mock_args.dry_run = True
        mock_args.yes = True
        mock_args.batch_size = 100
        mock_args.batch_pause = 0

        # Calculate retention epoch (30 days ago)
        retention_datetime = self.now - datetime.timedelta(days=30)
        retention_epoch = int(retention_datetime.timestamp() * 1000)

        # Call the function
        deleted_count = main.process_log_streams(self.mock_client, "test-log-group", retention_epoch, mock_args)

        # The old stream should be "deleted" in dry run mode
        self.assertEqual(deleted_count, 1)
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "old-stream", True)

    def test_delete_stream_success(self):
        """Test successful stream deletion"""
        self.mock_client.delete_log_stream.return_value = {"ResponseMetadata": {"RequestId": "test-id"}}

        result = main.delete_stream(self.mock_client, "test-log-group", "test-stream", False)

        self.assertTrue(result)
        self.mock_client.delete_log_stream.assert_called_once_with(
            logGroupName="test-log-group", logStreamName="test-stream"
        )

    def test_delete_stream_dry_run(self):
        """Test dry run mode for stream deletion"""
        result = main.delete_stream(self.mock_client, "test-log-group", "test-stream", True)

        self.assertTrue(result)
        self.mock_client.delete_log_stream.assert_not_called()


if __name__ == "__main__":
    unittest.main()
