import settings
import os
import psycopg2
import re
import tweepy
import pandas as pd
from textblob import TextBlob


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):

        if status.retweeted:
            return True
        id_str = status.id_str
        created_at = status.created_at
        text = deEmojify(status.text)
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

        sql_cursor = connection.cursor()
        sql = "INSERT INTO {} (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
            settings.table_name)
        val = (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, \
               user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count)
        sql_cursor.execute(sql, val)
        connection.commit()

        delete_query = '''
        DELETE FROM {0}
        WHERE id_str IN (
            SELECT id_str 
            FROM {0}
            ORDER BY created_at asc
            LIMIT 200) AND (SELECT COUNT(*) FROM covid19) > 9600;
        '''.format(settings.table_name)

        sql_cursor.execute(delete_query)

        connection.commit()
        sql_cursor.close()

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
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                |(\w+:\/\/\S+)", " ", tweet).split())


def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None


DATABASE_URL = os.environ['DATABASE_URL']

connection = psycopg2.connect(DATABASE_URL, sslmode='require')
sql_cursor = connection.cursor()

sql_cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(settings.table_name))

if sql_cursor.fetchone()[0] == False:
    sql_cursor.execute("CREATE TABLE {} ({});".format(settings.table_name, settings.table_attributes))
    connection.commit()
sql_cursor.close()

CONSUMER_KEY = os.environ['CONSUMER_KEY']
CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
ACCESS_KEY = os.environ['ACCESS_KEY']
ACCESS_SECRET = os.environ['ACCESS_SECRET']


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(languages=["en"], track=settings.data_on_word)

connection.close()
