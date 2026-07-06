import unittest
from unittest.mock import patch, MagicMock
import datetime

from botocore.exceptions import ClientError

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

        # Calculate retention epoch (30 days ago)
        retention_datetime = self.now - datetime.timedelta(days=30)
        retention_epoch = int(retention_datetime.timestamp() * 1000)

        # Call the function
        deleted_count = main.process_log_streams(
            self.mock_client,
            "test-log-group",
            retention_epoch,
            use_last_event=False,
            dry_run=False,
            skip_confirmation=True,
            retention_days=30,
            batch_size=100,
            batch_pause=0,
        )

        # Only the old stream should be deleted (created 40 days ago)
        self.assertEqual(deleted_count, 1)
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "old-stream", False)

    @patch("main.delete_stream")
    def test_process_log_streams_last_event(self, mock_delete_stream):
        """Test processing streams based on last event time"""
        mock_delete_stream.return_value = True

        # Calculate retention epoch (30 days ago)
        retention_datetime = self.now - datetime.timedelta(days=30)
        retention_epoch = int(retention_datetime.timestamp() * 1000)

        # Call the function
        deleted_count = main.process_log_streams(
            self.mock_client,
            "test-log-group",
            retention_epoch,
            use_last_event=True,
            dry_run=False,
            skip_confirmation=True,
            retention_days=30,
            batch_size=100,
            batch_pause=0,
        )

        # Only the old stream should be deleted (last event 35 days ago)
        self.assertEqual(deleted_count, 1)
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "old-stream", False)

    @patch("main.delete_stream")
    def test_process_log_streams_dry_run(self, mock_delete_stream):
        """Test dry run mode"""
        mock_delete_stream.return_value = True

        # Calculate retention epoch (30 days ago)
        retention_datetime = self.now - datetime.timedelta(days=30)
        retention_epoch = int(retention_datetime.timestamp() * 1000)

        # Call the function
        deleted_count = main.process_log_streams(
            self.mock_client,
            "test-log-group",
            retention_epoch,
            use_last_event=False,
            dry_run=True,
            skip_confirmation=True,
            retention_days=30,
            batch_size=100,
            batch_pause=0,
        )

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

    def test_get_stream_age_timestamp_missing_creation_time(self):
        """Test that get_stream_age_timestamp returns None when creationTime is missing"""
        stream = {"logStreamName": "no-timestamp-stream"}
        self.assertIsNone(main.get_stream_age_timestamp(stream, use_last_event=False))

    def test_get_stream_age_timestamp_missing_last_event_falls_back_to_none(self):
        """Test that use_last_event with no lastEventTimestamp and no creationTime returns None"""
        stream = {"logStreamName": "no-timestamp-stream"}
        self.assertIsNone(main.get_stream_age_timestamp(stream, use_last_event=True))

    def test_get_stream_age_timestamp_missing_last_event_falls_back_to_creation(self):
        """Test that use_last_event with no lastEventTimestamp falls back to creationTime"""
        stream = {"logStreamName": "s", "creationTime": 12345}
        self.assertEqual(main.get_stream_age_timestamp(stream, use_last_event=True), 12345)

    @patch("main.time.sleep")
    def test_delete_stream_throttle_retry_then_success(self, mock_sleep):
        """Test that delete_stream retries on ThrottlingException with exponential backoff"""
        throttle_error = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "DeleteLogStream"
        )
        self.mock_client.delete_log_stream.side_effect = [
            throttle_error,
            throttle_error,
            {"ResponseMetadata": {"RequestId": "ok"}},
        ]

        result = main.delete_stream(self.mock_client, "test-log-group", "test-stream", False, max_retries=5)

        self.assertTrue(result)
        self.assertEqual(self.mock_client.delete_log_stream.call_count, 3)
        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)

    @patch("main.time.sleep")
    def test_delete_stream_throttle_exhausts_retries(self, mock_sleep):
        """Test that delete_stream returns False after exhausting retries"""
        throttle_error = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "DeleteLogStream"
        )
        self.mock_client.delete_log_stream.side_effect = throttle_error

        result = main.delete_stream(self.mock_client, "test-log-group", "test-stream", False, max_retries=2)

        self.assertFalse(result)
        self.assertEqual(self.mock_client.delete_log_stream.call_count, 3)  # initial + 2 retries

    def test_delete_stream_resource_not_found(self):
        """Test that delete_stream handles ResourceNotFoundException"""
        self.mock_client.delete_log_stream.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}}, "DeleteLogStream"
        )

        result = main.delete_stream(self.mock_client, "test-log-group", "test-stream", False)
        self.assertFalse(result)

    @patch("main.delete_stream")
    @patch("main.confirm_deletion", return_value=True)
    def test_process_log_streams_confirmation_accepted(self, mock_confirm, mock_delete_stream):
        """Test that streams are deleted when user confirms"""
        mock_delete_stream.return_value = True

        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)
        deleted_count = main.process_log_streams(
            self.mock_client,
            "test-log-group",
            retention_epoch,
            use_last_event=False,
            dry_run=False,
            skip_confirmation=False,
            retention_days=30,
            batch_size=100,
            batch_pause=0,
        )

        self.assertEqual(deleted_count, 1)
        mock_confirm.assert_called_once_with("test-log-group", 30, 1)

    @patch("main.delete_stream")
    @patch("main.confirm_deletion", return_value=False)
    def test_process_log_streams_confirmation_rejected(self, mock_confirm, mock_delete_stream):
        """Test that no streams are deleted when user rejects confirmation"""
        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)
        deleted_count = main.process_log_streams(
            self.mock_client,
            "test-log-group",
            retention_epoch,
            use_last_event=False,
            dry_run=False,
            skip_confirmation=False,
            retention_days=30,
            batch_size=100,
            batch_pause=0,
        )

        self.assertEqual(deleted_count, 0)
        mock_delete_stream.assert_not_called()

    @patch("main.delete_stream")
    def test_process_log_streams_resource_not_found(self, mock_delete_stream):
        """Test that process_log_streams exits on ResourceNotFoundException for log group"""
        self.mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Log group not found"}},
            "DescribeLogStreams",
        )

        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)

        with self.assertRaises(SystemExit):
            main.process_log_streams(
                self.mock_client,
                "nonexistent-group",
                retention_epoch,
                use_last_event=False,
                dry_run=False,
                skip_confirmation=True,
                retention_days=30,
                batch_size=100,
                batch_pause=0,
            )

        mock_delete_stream.assert_not_called()

    @patch("main.delete_stream")
    def test_process_log_streams_skips_missing_timestamp(self, mock_delete_stream):
        """Test that streams with missing timestamps are skipped"""
        mock_delete_stream.return_value = True
        self.mock_paginator.paginate.return_value = [
            {"logStreams": [{"logStreamName": "no-ts-stream"}, self.old_stream]}
        ]

        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)
        deleted_count = main.process_log_streams(
            self.mock_client,
            "test-log-group",
            retention_epoch,
            use_last_event=False,
            dry_run=False,
            skip_confirmation=True,
            retention_days=30,
            batch_size=100,
            batch_pause=0,
        )

        self.assertEqual(deleted_count, 1)
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "old-stream", False)

    @patch("sys.argv", ["prog", "-l", "test", "-r", "-5"])
    def test_parse_args_negative_retention(self):
        """Test that negative retention is rejected"""
        with self.assertRaises(SystemExit):
            main.parse_args()

    @patch("sys.argv", ["prog", "-l", "test", "-r", "30", "--batch-size", "0"])
    def test_parse_args_zero_batch_size(self):
        """Test that zero batch-size is rejected"""
        with self.assertRaises(SystemExit):
            main.parse_args()

    def test_find_eligible_streams_filters_by_creation_time(self):
        """find_eligible_streams returns only streams older than the retention epoch"""
        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)
        result = main.find_eligible_streams(self.mock_client, "test-log-group", retention_epoch, use_last_event=False)
        self.assertEqual(result, ["old-stream"])

    def test_find_eligible_streams_filters_by_last_event(self):
        """find_eligible_streams uses lastEventTimestamp when use_last_event is True"""
        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)
        result = main.find_eligible_streams(self.mock_client, "test-log-group", retention_epoch, use_last_event=True)
        self.assertEqual(result, ["old-stream"])

    def test_find_eligible_streams_skips_missing_timestamp(self):
        """find_eligible_streams skips streams without a usable timestamp"""
        self.mock_paginator.paginate.return_value = [
            {"logStreams": [{"logStreamName": "no-ts-stream"}, self.old_stream]}
        ]
        retention_epoch = int((self.now - datetime.timedelta(days=30)).timestamp() * 1000)
        result = main.find_eligible_streams(self.mock_client, "test-log-group", retention_epoch, use_last_event=False)
        self.assertEqual(result, ["old-stream"])

    @patch("main.delete_stream")
    def test_delete_eligible_streams_counts_successes(self, mock_delete_stream):
        """delete_eligible_streams returns the count of successful deletions"""
        mock_delete_stream.side_effect = [True, False, True]
        count = main.delete_eligible_streams(
            self.mock_client,
            "test-log-group",
            ["a", "b", "c"],
            dry_run=False,
            batch_size=100,
            batch_pause=0,
        )
        self.assertEqual(count, 2)
        self.assertEqual(mock_delete_stream.call_count, 3)

    @patch("main.delete_stream")
    def test_delete_eligible_streams_forwards_dry_run(self, mock_delete_stream):
        """delete_eligible_streams passes dry_run through to delete_stream"""
        mock_delete_stream.return_value = True
        main.delete_eligible_streams(
            self.mock_client,
            "test-log-group",
            ["a"],
            dry_run=True,
            batch_size=100,
            batch_pause=0,
        )
        mock_delete_stream.assert_called_once_with(self.mock_client, "test-log-group", "a", True)

    @patch("main.time.sleep")
    @patch("main.delete_stream")
    def test_delete_eligible_streams_pauses_between_batches(self, mock_delete_stream, mock_sleep):
        """delete_eligible_streams sleeps once per completed batch"""
        mock_delete_stream.return_value = True
        main.delete_eligible_streams(
            self.mock_client,
            "test-log-group",
            ["a", "b", "c", "d", "e"],
            dry_run=False,
            batch_size=2,
            batch_pause=0.25,
        )
        # Batches complete at i=2 and i=4 → two pauses
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(0.25)


if __name__ == "__main__":
    unittest.main()
