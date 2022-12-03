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
        'Count',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest'
    )

    for message in consumer:
        try:
            aDict = json.loads(message.value)
            a=str(aDict['end'])
            b=str(aDict['start'])
            c=aDict['tweet_count']
            """if((mysql_conn.read_query(connection,f"Select startDate from retweet_count where startDate='{b}'"))==b and (mysql_conn.read_query(connection,f"Select endDate from retweet_count where endDate='{a}'"))==a and (mysql_conn.read_query(connection,f"Select tweet_count from retweet_count where tweet_count={c}"))==c):
                print("Duplicate Data")
                continue
            else:"""
            query1=f"Insert into newyork_tweet_count values('{a}','{b}',{c})"
            mysql_conn.execute_query(connection,query1)
        except:
            continue