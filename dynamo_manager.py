# dynamo_manager by: ROSELE

import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
level=logging.INFO, format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()

# Create a table
def create_dynamo_table(table_name, pk, pkdef):
    ddb = boto3.resource('dynamodb')
    table = ddb.create_table(
        TableName=table_name,
        KeySchema=pk,
        AttributeDefinitions=pkdef,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table

# Use an existing table
def get_dynamo_table(table_name):
    ddb = boto3.resource('dynamodb')
    return ddb.Table(table_name)

# Create an item
def create_product(category, sku, **item):
    table = get_dynamo_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    item.update(keys)
    table.put_item(Item=item)
    return table.get_item(Key=keys)['Item']

# Update an item
def update_product(category, sku, **item):
    table = get_dynamo_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    expr = ', '.join([f'{k}=:{k}' for k in item.keys()])
    vals = {f':{k}': v for k, v in item.items()}
    table.update_item(
    Key=keys,
    UpdateExpression=f'SET {expr}',
    ExpressionAttributeValues=vals,)
    return table.get_item(Key=keys)['Item']

# Delete an item
def delete_product(category, sku):
    table = get_dynamo_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    res = table.delete_item(Key=keys)
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    else:
        log.error(f'There was an error when deleting the product: {res}')
        return False

# Batch operations
def create_dynamo_items(table_name, items, keys=None):
    table = get_dynamo_table(table_name)
    params = {
        'overwrite_by_pkeys': keys
    } if keys else {}
    with table.batch_writer(**params) as batch:
        for item in items:
            batch.put_item(Item=item)
    return True

# Search items
import operator as op
from boto3.dynamodb.conditions import Key, Attr
def query_products(key_expr, filter_expr=None):
# Query requires that you provide the key filters
    table = get_dynamo_table('products')
    params = {
        'KeyConditionExpression': key_expr,
    }
    if filter_expr:
        params['FilterExpression'] = filter_expr
        res = table.query(**params)
    return res['Items']

def scan_products(filter_expr):
# Scan does not require a key filter. It will go through
# all items in your table and return all matching items.
# Use with caution!
    table = get_dynamo_table('products')
    params = {
        'FilterExpression': filter_expr,
    }
    res = table.scan(**params)
    return res['Items']

# Delete a table
def delete_dynamo_table(table_name):
    table = get_dynamo_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True