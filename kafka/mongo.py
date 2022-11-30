# importing the required modules
from json import loads
from kafka import KafkaConsumer
from pymongo import MongoClient, collection

# generating the Kafka Consumer
my_consumer = KafkaConsumer(
    'Twitter-Kafka',
    bootstrap_servers = ['localhost : 9092'],
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    group_id = 'my-group',
    value_deserializer = lambda x : loads(x.decode('utf-8'))
)
my_client = MongoClient('localhost : 27017')
my_collection = my_client.testnum.testnum
for message in my_consumer:
    message = message.value
    collection.insert_one(message)
    print(message + " added to " + my_collection)