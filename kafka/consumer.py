from kafka import KafkaConsumer

consumer = KafkaConsumer('Twitter-Kafka',bootstrap_servers=['localhost:9092'])
#hdfs_path = 'hdfs://localhost:9000/StockDatapydoop/stock_file.txt'

for message in consumer:
    values = message.value.decode('utf-8')
    print(message.value)

