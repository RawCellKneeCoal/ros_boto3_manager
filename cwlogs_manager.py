# cwlogs_manager by: ROSELE

import logging, os
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
level=logging.INFO, format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()


# List log groups and log streams
def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupNamePrefix': group_name,
    } if group_name else {}
    res = cwlogs.describe_log_groups(**params)
    return res['logGroups']

def list_log_group_streams(group_name, stream_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
    } if group_name else {}
    if stream_name:
        params['logStreamNamePrefix'] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res['logStreams']

# Filter log events
def filter_log_events(
    group_name, filter_pat,
    start=None, stop=None,
    region_name=None
):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
        'filterPattern': filter_pat,
    }
    if start:
        params['startTime'] = start
    if stop:
        params['endTime'] = stop
    res = cwlogs.filter_log_events(**params)
    return res['events']