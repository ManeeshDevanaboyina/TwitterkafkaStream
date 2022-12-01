from kafka import KafkaConsumer
import mysql_conn
import json

with open('config.json') as json_file:
    data = json.load(json_file)
my_sql_username=data["mysql_user"]
my_sql_password=data["mysql_password"]

consumer = KafkaConsumer('Twitter-Kafka',bootstrap_servers=['localhost:9092'])

connection=mysql_conn.create_db_connection("localhost",my_sql_username,my_sql_password,"twitter_data_ingestion")
#mysql_conn.execute_query(connection,"""Insert into iphone_tweets values(PK,"tweet data")""")
for message in consumer:
    values = message.value.decode('utf-8')
    print(message.value)

