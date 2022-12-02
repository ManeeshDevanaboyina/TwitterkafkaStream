import json
import sqlite3

from kafka import KafkaConsumer

import mysql_conn
import json


with open('config.json') as json_file:
    data = json.load(json_file)
my_sql_username=data["mysql_user"]
my_sql_password=data["mysql_password"]
connection=mysql_conn.create_db_connection("localhost",my_sql_username,my_sql_password,"twitter_data_ingestion")

if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        'Covid-Retweet',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest'
    )

for message in consumer:
    aDict = json.loads(message.value)
    a=str(aDict['end'])
    b=str(aDict['start'])
    #values = message.value.decode('utf-8')
    if((mysql_conn.read_query(connection,f"Select startDate from retweet_count"))==b):

        query1=f"Insert into retweet_count values('{a}','{b}',{aDict['tweet_count']})"
        mysql_conn.execute_query(connection,query1)


#Getting class type as String intially it was dict now coming as String because of this unable take out key in dict

