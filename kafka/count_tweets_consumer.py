import json
from kafka import KafkaConsumer

if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        'Covid-Retweet',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )

for message in consumer:
    #values = message.value.decode('utf-8')

   # print("Maneesh2",type(message.value))
    aDict = json.loads(message.value)
    print(type(aDict))

#Getting class type as String intially it was converted to dict now coming as String because of this unable take out key in dict

    #print(aDict["start"])
    #query1=f"Insert into iphone_tweets values({1},{message.value})"
    #mysql_conn.execute_query(connection,query1)