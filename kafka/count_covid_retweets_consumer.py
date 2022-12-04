import ast

from kafka import KafkaConsumer
import mysql_conn
import json

with open('config.json') as json_file:
    data = json.load(json_file)
my_sql_username=data["mysql_user"]
my_sql_password=data["mysql_password"]
connection=mysql_conn.create_db_connection("localhost",my_sql_username,my_sql_password,"twitter_data_ingestion")

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                         auto_offset_reset='latest',
                         value_deserializer=lambda m: json.loads(m))
consumer.subscribe(['Covid-Retweet'])

for message in consumer:
    try:
            aDict=ast.literal_eval(message.value)
            print(aDict)
           # aDict = json.loads(message.value)
            #aDict = message.value
            a=aDict['end']
            a=a[:10]
            print(a)
            #b=str(aDict['start'])
            c=aDict['tweet_count']
            query1=f"Insert into covid_retweets_count values('{a}',{c})"
            mysql_conn.execute_query(connection,query1)
    except:
            print("Error in data")
            continue