# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with the Amazon Kinesis API to
generate a data stream. This script generates data for several of the _Windows
and Aggregation_ examples in the Amazon Kinesis Data Analytics SQL Developer Guide.
"""

import datetime
import json
import random
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class KinesisStream:

    def __init__(self, kinesis_client):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name =   "sandesh-demo-1"
        self.shard_details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    

    boto3.client('kinesis')

    def list_shards(self):
        response = self.kinesis_client.list_shards(
        StreamName=self.name,                
        MaxResults=100
        #StreamCreationTimestamp=datetime(2015, 1, 1),
        # ShardFilter={
        #     'Type': 'AFTER_SHARD_ID'|'AT_TRIM_HORIZON'|'FROM_TRIM_HORIZON'|'AT_LATEST'|'AT_TIMESTAMP'|'FROM_TIMESTAMP',
        #     'ShardId': 'string',
        #     'Timestamp': datetime(2015, 1, 1)
        # }
        )  
        print(response)
        self.shard_details = response

    def get_records(self, max_records):
            """
            Gets records from the stream. This function is a generator that first gets
            a shard iterator for the stream, then uses the shard iterator to get records
            in batches from the stream. Each batch of records is yielded back to the
            caller until the specified maximum number of records has been retrieved.

            :param max_records: The maximum number of records to retrieve.
            :return: Yields the current batch of retrieved records.
            """
            print("in get records")
            try:                
                response = None
                try:

                    response = self.kinesis_client.get_shard_iterator(
                        StreamName=self.name, ShardId=self.shard_details['Shards'][0]['ShardId'],
                        ShardIteratorType='LATEST')
                    print("Shard iterator ", response)
                except Exception:
                    print("Exception while getting shards")
                shard_iter = response['ShardIterator']
                record_count = 0
                print("Record count ", record_count)
                while record_count < max_records:
                    print("in while loop")
                    response = self.kinesis_client.get_records(
                        ShardIterator=shard_iter, Limit=10)
                    shard_iter = response['NextShardIterator']
                    records = response['Records']
                    print(f"Got {len(records)} records.")
                    logger.info("Got %s records.", len(records))

                    if len(records)==0:
                        print("breaking out of the loop")
                        break

                    record_count += len(records)
                    yield records
            except ClientError:
                print("Exception")
                logger.exception("Couldn't get records from stream %s.", self.name)
                raise

    def get_data():
        return {
            'EVENT_TIME': datetime.datetime.now().isoformat(),
            'TICKER': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
            'PRICE': round(random.random() * 100, 2)}



    def generate(stream_name, kinesis_client):
        while True:
            data = get_data()
            print(data)
            kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="partitionkey")


if __name__ == '__main__':
    #generate(STREAM_NAME, )
    stream = KinesisStream(kinesis_client=boto3.client('kinesis'))
    print(stream)
    print("start") 
    stream.list_shards()

    records = stream.get_records(10)
    for record in records:
        print(record)
