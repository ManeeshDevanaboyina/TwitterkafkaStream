from kafka import KafkaConsumer, TopicPartition
import mysql_conn
import json

with open('config.json') as json_file:
    data = json.load(json_file)
my_sql_username=data["mysql_user"]
my_sql_password=data["mysql_password"]

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
partitions= [TopicPartition('Twitter-Kafka', p) for p in consumer.partitions_for_topic('Twitter-Kafka')]
last_offset_per_partition = consumer.end_offsets(partitions)

connection=mysql_conn.create_db_connection("localhost",my_sql_username,my_sql_password,"twitter_data_ingestion")


for message in consumer:
    values = message.value.decode('utf-8')
    print(message.value)
    query1=f"Insert into iphone_tweets values({1},{message.value})"
    mysql_conn.execute_query(connection,query1)


