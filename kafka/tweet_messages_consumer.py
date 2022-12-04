from kafka import KafkaConsumer, TopicPartition
import mysql_conn
import json


with open('config.json') as json_file:
    data = json.load(json_file)
my_sql_username=data["mysql_user"]
my_sql_password=data["mysql_password"]
connection=mysql_conn.create_db_connection("localhost",my_sql_username,my_sql_password,"twitter_data_ingestion")


consumer = KafkaConsumer('Twitter-Kafka',bootstrap_servers=['localhost:9092'],auto_offset_reset='latest')


for message in consumer:
    a= message.key
    tweet_id = a.decode("utf-8")
    b=message.value
    tweet_value=b.decode("utf-8")
    print(tweet_id)
    print(tweet_value)
    query1=f"Insert into tweet_messages values({tweet_id},'{tweet_value}')"
    mysql_conn.execute_query(connection,query1)



