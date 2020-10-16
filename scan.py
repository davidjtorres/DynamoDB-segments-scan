import json
import boto3
import threading
import time
import os
session = boto3.session.Session()
dynamoDbClient = session.client('dynamodb')
lambdaClient = session.client('lambda')


def lambda_handler(event, context):
    count = 0
    all_items = []

    def scan_table(segment, total_segments):
        nonlocal count
        nonlocal all_items
        global session
        global dynamoDbClient

        scanObj = {
            'TableName': 'table-name',
            'FilterExpression': '#attributex  = :valuex',
            'ExpressionAttributeValues': {
                ':valuex': {'S': 'some string value'}
            },
            'ExpressionAttributeNames': {
                '#attributex': '#attributex'
            },
            'Segment': segment,
            'TotalSegments': total_segments,
        }
        response = dynamoDbClient.scan(**scanObj)
        count = count + len(response['Items'])
        all_items += response['Items']

        # Start a new scan as long a there is a LastEvakautedKey available
        while 'LastEvaluatedKey' in response:

            scanObj['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = dynamoDbClient.scan(**scanObj)
            count = count + len(response['Items'])
            all_items += response['Items']
            time.sleep(0.02)

    def create_threads():
        thread_list = []
        total_threads = 4

        for i in range(total_threads):
            # Instantiate and store the thread
            thread = threading.Thread(
                target=scan_table, args=(i, total_threads))
            thread_list.append(thread)
        # Start threads
        for thread in thread_list:
            thread.start()
        # Block main thread until all threads are finished
        for thread in thread_list:
            thread.join()
        print('Done scanning:  %s items found' % len(all_items))

    create_threads()

    return 'Running!'
