import time
import json
import tweepy
from kafka import KafkaProducer
from json import dumps
from datetime import datetime, timedelta


#bearer_token = "AAAAAAAAAAAAAAAAAAAAAAkajwEAAAAACoklzU5Y6z%2BQfSDuqJbWpOmOvPU%3D115RjOCiJ75DIP9vSbHSFxoC65K6xheIdgB0uxJJ8IxNvTmlPE"

with open('config.json') as json_file:
    data = json.load(json_file)
bearer_token=data["bearer_token"]
client = tweepy.Client(bearer_token)

# Search Recent Tweets

# This endpoint/method returns Tweets from the last seven days

response = client.search_recent_tweets("iphone OR Musk")
response1=client.search_recent_tweets("Covid")
#print(reponse1)
# The method returns a Response object, a named tuple with data, includes,
# errors, and meta fields
#print(response.meta)

# In this case, the data field of the Response returned is a list of Tweet
# objects
tweets = response.data
newtopic_tweets=response1.data
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:dumps(K).encode('utf-8'))
topic_name = 'Twitter-Kafka'
topic_name1="Covid"

# Each Tweet object has default ID and text fields
def get_twitter_data1():
    for tweet in tweets:

        producer.send(topic_name, tweet.text)
        #print(tweet.id)
        print(tweet.text)

def get_twitter_data2():
    for tweet in newtopic_tweets:

        producer.send(topic_name1, tweet.text)
        #print(tweet.id)
        print(tweet.text)


#get_twitter_data1()


# By default, this endpoint/method returns 10 results
# You can retrieve up to 100 Tweets by specifying max_results
response = client.search_recent_tweets("Tweepy", max_results=100)

def periodic_work(interval):
    while True:
        get_twitter_data1()
        get_twitter_data2()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60*1)  # get data every couple of minutes

