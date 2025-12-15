import pytest
from unittest.mock import Mock, patch, MagicMock
import datetime
from botocore.exceptions import ClientError
import main


@pytest.fixture
def mock_client():
    return Mock()


def test_parse_args():
    with patch('sys.argv', ['main.py', '-l', 'test-group', '-r', '30']):
        args = main.parse_args()
        assert args.loggroup == 'test-group'
        assert args.retention == 30


def test_delete_stream_success(mock_client):
    mock_client.delete_log_stream.return_value = None
    
    result = main.delete_stream(mock_client, 'test-group', 'test-stream')
    
    assert result is True
    mock_client.delete_log_stream.assert_called_once_with(
        logGroupName='test-group', 
        logStreamName='test-stream'
    )


def test_delete_stream_failure(mock_client):
    mock_client.delete_log_stream.side_effect = ClientError(
        {'Error': {'Code': 'ResourceNotFoundException'}}, 'delete_log_stream'
    )
    
    result = main.delete_stream(mock_client, 'test-group', 'test-stream')
    
    assert result is False


@patch('main.boto3.client')
@patch('main.parse_args')
def test_main_success(mock_parse_args, mock_boto_client):
    # Setup mocks
    mock_args = Mock()
    mock_args.loggroup = 'test-group'
    mock_args.retention = 7
    mock_args.loglevel = 20
    mock_parse_args.return_value = mock_args
    
    mock_client = Mock()
    mock_boto_client.return_value = mock_client
    
    # Mock paginator
    mock_paginator = Mock()
    mock_client.get_paginator.return_value = mock_paginator
    
    # Create old and new streams
    old_time = int((datetime.datetime.now(datetime.timezone.utc) - 
                   datetime.timedelta(days=10)).timestamp() * 1000)
    new_time = int((datetime.datetime.now(datetime.timezone.utc) - 
                   datetime.timedelta(days=1)).timestamp() * 1000)
    
    mock_paginator.paginate.return_value = [
        {
            'logStreams': [
                {'logStreamName': 'old-stream', 'creationTime': old_time},
                {'logStreamName': 'new-stream', 'creationTime': new_time}
            ]
        }
    ]
    
    with patch('main.delete_stream', return_value=True) as mock_delete:
        main.main()
        mock_delete.assert_called_once_with(mock_client, 'test-group', 'old-stream')


@patch('main.boto3.client')
@patch('main.parse_args')
def test_main_aws_error(mock_parse_args, mock_boto_client):
    mock_args = Mock()
    mock_args.loggroup = 'test-group'
    mock_args.retention = 7
    mock_args.loglevel = 20
    mock_parse_args.return_value = mock_args
    
    mock_boto_client.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied'}}, 'logs'
    )
    
    with pytest.raises(SystemExit):
        main.main()