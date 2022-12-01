import time
import json
import tweepy
from kafka import KafkaProducer
from json import dumps
from datetime import datetime, timedelta

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=1)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

with open('config.json') as json_file:
    data = json.load(json_file)
bearer_token=data["bearer_token"]
client = tweepy.Client(bearer_token)

# Search Recent Tweets

# This endpoint/method returns Tweets from the last seven days
# By default, this endpoint/method returns 10 results
# You can retrieve up to 100 Tweets by specifying max_results
response=client.search_recent_tweets(query="iphone OR Musk",max_results=10,tweet_fields=['created_at','lang'],expansions=['author_id'])
#print(response)
#response=client.get_all_tweets_count("iphone")
response1=client.search_recent_tweets("Covid")
# The method returns a Response object, a named tuple with data, includes,
# errors, and meta fields
#print(response.meta)

# In this case, the data field of the Response returned is a list of Tweet
# objects
tweets = response.data
new_topic_tweets = response1.data
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:dumps(K).encode('utf-8'))
topic_name = 'Twitter-Kafka'
topic_name1="Covid"

# Each Tweet object has default ID and text fields
def get_twitter_data1():
    for tweet in tweets:
        if(tweet.lang=='en'):
            producer.send(topic_name,tweet.text)
            #print(tweet.id)
            print(tweet.lang)
            print(tweet.text)

def get_twitter_data2():
    for tweet in new_topic_tweets:

        producer.send(topic_name1, key=tweet.id,value=tweet.text)
        #print(str(normalize_timestamp(str(tweet.created_at))))
        #print(tweet.id)
        print(tweet.text)


#get_twitter_data1()




#For Running the program for every couple of minutes
def periodic_work(interval):
    while True:
        get_twitter_data1()
        get_twitter_data2()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60*12)  # get data every couple of minutes

