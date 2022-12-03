import time
import json
import tweepy
from kafka import KafkaProducer


with open('config.json') as json_file:
    data = json.load(json_file)
bearer_token=data["bearer_token"]
client = tweepy.Client(bearer_token)

# Search Recent Tweets
# This endpoint/method returns Tweets from the last seven days
# By default, this endpoint/method returns 10 results
# You can retrieve up to 100 Tweets by specifying max_results

response=client.search_recent_tweets(query="iphone OR Musk",max_results=10,tweet_fields=['created_at','lang'],expansions=['author_id'])
users={u['id']: u for u in response.includes['users']}
#print(response)
#response=client.get_all_tweets_count("iphone")

response1=client.search_recent_tweets(("Covid"),max_results=10,tweet_fields=['created_at','lang'])

counts=client.get_recent_tweets_count("NewYork",granularity='day')

counts_retweets=client.get_recent_tweets_count('covid -is:retweet',granularity='day')

#Extracting User_ID of the Following User
user_details=client.get_users(usernames=['twitterdev'])
#Getting User Tweets of the following User
tweets=client.get_liked_tweets(id=data['user_id'],tweet_fields=['lang'])
for tweet in tweets.data:
    print(tweet.id)
    print(tweet.lang)



# The method returns a Response object, a named tuple with data, includes,
# errors, and meta fields
#print(response.meta)

# In this case, the data field of the Response returned is a list of Tweet
# objects
tweets = response.data
new_topic_tweets = response1.data
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:json.dumps(K).encode('utf-8'))
#producer1 = KafkaProducer(bootstrap_servers=['localhost:9092'],key_serializer=str.encode,value_serializer=str.encode)
topic_name = 'Twitter-Kafka'
topic_name1="Covid"
topic_name_count="Count"
topic_name_covid_retweet="Covid-Retweet"
# Each Tweet object has default ID and text fields

def get_covid_retweet_counts():
    for count in counts_retweets.data:
        #print(type(count))
        print("Maneesh1",json.dumps(count))
        producer.send(topic_name_covid_retweet,json.dumps(count))
get_covid_retweet_counts()
def get_tweet_counts():
    for count in counts.data:
        print(count)
        print("Maneesh3",json.dumps(count))
        producer.send(topic_name_count,json.dumps(count))
get_tweet_counts()
def get_twitter_data1():

        for tweet in tweets:
            if(tweet.lang=='en'):
                user=users[tweet.author_id]
                print(user.username)
                my_bytes = tweet.encode('utf-8')
                print(my_bytes)
                producer.send(topic_name,tweet)
                #producer.send(topic_name,tweet.id)
                print(tweet.id)
                print(tweet.lang)



def get_twitter_data2():
    for tweet in new_topic_tweets:
        if(tweet.lang=='en'):

            #print(str(tweet.created_at))
            print('super')
            print(tweet.data)

            #producer1.send(topic_name1,key=tweet.id,value=tweet.message)
            #print(str(normalize_timestamp(str(tweet.created_at))))
            #print(tweet.id)
            #print(tweet.text)

#Problem in sending object as JSON

#get_twitter_data1()




#For Running the program for every couple of minutes
def periodic_work(interval):
    while True:
       # get_twitter_data1()
        #get_twitter_data2()
        #get_covid_retweet_counts()
        #get_tweet_counts()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60*1)  # get data every couple of minutes

