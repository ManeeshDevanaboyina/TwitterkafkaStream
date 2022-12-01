from kafka import KafkaConsumer
import mysql_conn

consumer = KafkaConsumer('Twitter-Kafka',bootstrap_servers=['localhost:9092'])
#hdfs_path = 'hdfs://localhost:9000/StockDatapydoop/stock_file.txt'

connection=mysql_conn.create_db_connection("localhost","root","","twitter_data_ingestion")
#mysql_conn.execute_query(connection,"""Insert into iphone_tweets values(PK,"tweet data")""")
for message in consumer:
    values = message.value.decode('utf-8')
    print(message.value)

