# sns_manager by: ROSELE

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

# Create a topic
def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True

# list sns topics
def list_sns_topics(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)

# subscribe to a topic
def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []),
    subscriptions.get('NextToken', None)

def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Protocol': 'sms',
        'Endpoint': mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    return True

# Publish a message to a topic
def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
    'TopicArn': topic_arn,
    'Message': message,
    }
    res = sns.publish(**params)
    print(res)
    return True

# Unsubscribe to a topic
def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {
        'SubscriptionArn': subscription_arn,
    }
    res = sns.unsubscribe(**params)
    print(res)
    return True

# Delete a topic
def delete_sns_topic(topic_arn):
# This will delete the topic and all it's subscriptions.
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True