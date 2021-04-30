# Extracting streaming data from Twitter, pre-processing, and loading into MySQL
import credentials  # Import api/access_token keys from credentials.py
import settings  # Import related setting constants from settings.py

import re
import tweepy
import psycopg2
import mysql.connector
import pandas as pd
from textblob import TextBlob


# Streaming With Tweepy
# http://docs.tweepy.org/en/v3.4.0/streaming_how_to.html#streaming-with-tweepy


# Override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    '''
    Tweets are known as “status updates”. So the Status class in tweepy has properties describing the tweet.
    https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
    '''

    def on_status(self, status):
        '''
        Extract info from tweets
        '''

        if status.retweeted:
            # Avoid retweeted info, and only original tweets will be received
            return True
        # Extract attributes from each tweet
        id_str = status.id_str
        created_at = status.created_at
        text = deEmojify(status.text)  # Pre-processing the text
        sentiment = TextBlob(text).sentiment
        polarity = sentiment.polarity
        subjectivity = sentiment.subjectivity

        user_created_at = status.user.created_at
        user_location = deEmojify(status.user.location)
        user_description = deEmojify(status.user.description)
        user_followers_count = status.user.followers_count
        longitude = None
        latitude = None
        if status.coordinates:
            longitude = status.coordinates['coordinates'][0]
            latitude = status.coordinates['coordinates'][1]

        retweet_count = status.retweet_count
        favorite_count = status.favorite_count

        # Store all data in MySQL
        try:

            mycursor = mydb.cursor()
            sql = "INSERT INTO {} (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                settings.TABLE_NAME)
            val = (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, \
                   user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count)
            mycursor.execute(sql, val)
            mydb.commit()
            mycursor.close()
        except:
            print('failed in on_status')

    def on_error(self, status_code):
        '''
        Since Twitter API has rate limits, stop srcraping data as it exceed to the thresold.
        '''
        if status_code == 420:
            # return False to disconnect the stream
            return False


def clean_tweet(self, tweet):
    '''
    Use sumple regex statemnents to clean tweet text by removing links and special characters
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None


mydb = psycopg2.connect("dbname='live_covid_tweet'"
                        " user='postgres' "
                        "host='localhost'"
                        " password='maryjolly'"
                        " connect_timeout=1 ")


if mydb != 0:
    mycursor = mydb.cursor()
    mycursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(settings.TABLE_NAME))
    # made it zero but maybe wrong
    if mycursor.fetchone()[0] != 0:
        mycursor.execute("CREATE TABLE {} ({})".format(settings.TABLE_NAME, settings.TABLE_ATTRIBUTES))
        mydb.commit()
    mycursor.close()


auth = tweepy.OAuthHandler(credentials.api_key, credentials.api_security_key)
auth.set_access_token(credentials.access_token, credentials.access_secret_token)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(languages=["en"], track=settings.TRACK_WORDS)

mydb.close()

#
# import psycopg2
#
#
#
# try:
#     conn = psycopg2.connect("dbname='de0nfk6t8kafcg'"
#                             " user='lxylonnfjbtxcd' host='ec2-34-230-115-172.compute-1.amazonaws.com' password='5a432ed2df7cd422fe45daac0878b3f77b9558eaf0cb5ba3772a94357a1ae138' ")
#
#     print(conn)
#     conn.close()
# except:
#     print('false')


# import psycopg2
#
# try:
#     conn = psycopg2.connect("dbname='live_covid_tweet'"
#                             " user='postgres' "
#                             "host='localhost'"
#                             " password='maryjolly'"
#                             " connect_timeout=1 ")
#
#     print(conn)
#     conn.close()
#     print(conn)
# except:
#     print('false')
