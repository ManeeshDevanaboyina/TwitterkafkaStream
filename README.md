# TwitterkafkaStream

Kafka Streaming Application Using Twitter Tweets

Create Twitter Developer Account for generating Access tokens and place those in JSON file for security purposes. Access that file for API authentication.

For running the above Application, Kafka should be installed and should be running on the system. And necessary Libraries have to be imported especially Tweepy Library for fetching data from the Twitter API.

Create a database called 'twitter_data_ingestion' on MySql work bench and also create tables in that database for sending different tweet data coming from Twitter API. All the Kafka consumer applications are Inserting data into different tables. So inorder to insert data execute the below queries and then data will be inserted automatically. There is a python file for establishing database connection, Reading and Writing queries. We can make use of it if you need any changes.

Execute the following queries in MySql.

CREATE TABLE twitter_data_ingestion.covid_retweets_count ( Date DATE NOT NULL, tweet_count int NOT NULL, PRIMARY KEY (Date) );

CREATE TABLE twitter_data_ingestion.nework_tweet_count ( end_date varchar(50), start_date varchar(50), tweet_count int NOT NULL, PRIMARY KEY (end_date) );

CREATE TABLE twitter_data_ingestion.tweet_messages ( tweet_id BIGINT(30) NOT NULL, tweet_messages LONGTEXT, PRIMARY KEY (tweet_id) );

CREATE TABLE twitter_data_ingestion.user_liked_tweets ( tweet_id BIGINT(30) NOT NULL, tweet_messages LONGTEXT, PRIMARY KEY (tweet_id) );

For visualization purpose in Tableau we can find an option to connect MySql in Tableau. We need to install necessary drivers for establishing connection. From the above tables we used covid_retweets_count table to form a perfect bar graph for analysis
