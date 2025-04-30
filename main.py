import argparse
import boto3
import os
import datetime


client = boto3.client('logs', region_name='eu-north-1')
paginator = client.get_paginator('describe_log_streams')

parser = argparse.ArgumentParser(
                    prog='cloudwatch-logstream-cleaner',
                    description='Delete CloudWatch log streams older than a specified number of days')

parser.add_argument('-l', '--loggroup', help='CloudWatch Log Group Name', required=True)
parser.add_argument('-r', '--retention', type=int, help='Retention in days', required=True)
args = parser.parse_args()    

# Date handling
date_now = datetime.datetime.now()
retention=args.retention
retention_datetime= date_now - datetime.timedelta(days=retention)
retention_epoch = int(retention_datetime.timestamp()) * 1000 # milliseconds

logGroupName = args.loggroup

def delete_stream(logGroupName, logStreamName):
    try:
        response = client.delete_log_stream(
            logGroupName=logGroupName,
            logStreamName=logStreamName
        )
        #print(response)
    except Exception as e:
        print("Error deleting stream: " + logStreamName)
        print(e)

page_iterator = paginator.paginate(
    logGroupName=logGroupName,
)

print("Deleting streams older than: " + str(retention_datetime))
print("Log group name: " + logGroupName)
print("Log streams older than " + str(retention) + " days will be deleted")

n=0
for page in page_iterator:
    log_streams = page['logStreams']
    for log_stream in log_streams:
        if log_stream['creationTime'] < retention_epoch:
            n+=1
            print("Deleting stream: "+ log_stream['logStreamName'])
            delete_stream(logGroupName, log_stream['logStreamName'])


print("Total streams deleted: " + str(n))

