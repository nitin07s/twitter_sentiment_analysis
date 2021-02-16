from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
from kafka import KafkaProducer
import os
import json
import logging
import configparser
from datetime import datetime
from datetime import timezone


def cleantweet(data):

    rawtweet = json.loads(data)
    logging.info(f"Cleaning tweet :{rawtweet['id']}")
    tweet = {}
    tweet["date"] = datetime.strptime(rawtweet["created_at"], '%a %b %d %H:%M:%S %z %Y')\
        .replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%Y-%m-%d %H:%M:%S')
    tweet["user"] = rawtweet["user"]["screen_name"]
    ##
    tweet["language"] = rawtweet['lang']
    tweet["id"] = rawtweet['id']
    tweet["is_retweet"] = True if "retweeted_status" in rawtweet else False
    if geo_data := rawtweet.get('includes', {}).get('places'):
        if coordinates := rawtweet['data'].get('geo', {}).get('coordinates', {}).get('coordinates'):
            lon, lat = coordinates
        else:
            lon1, lat1, lon2, lat2 = geo_data[0]['geo']['bbox']
            lon = (lon1 + lon2) / 2
            lat = (lat1 + lat2) / 2

        tweet["country_code"] = geo_data[0]["country_code"]
        tweet["place_name"] = geo_data[0]["full_name"]

        tweet["latitude"] = lat
        tweet["longitude"] = lon
    if "extended_tweet" in rawtweet:
        tweet["text"] = rawtweet["extended_tweet"]["full_text"]
    else:
        tweet["text"] = rawtweet["text"]
    return json.dumps(tweet)


class StdOutListener(StreamListener):

    def __init__(self, bootstrap_server, topic):
        self.producer = KafkaProducer(bootstrap_servers=[
                                      bootstrap_server], value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        self.topic = topic

    def on_data(self, data):

        newdata = cleantweet(data)
        logging.info(f"Sending tweet: {newdata}")
        self.producer.send(self.topic, newdata)
        return True

    def on_error(self, status):

        print(status)


def main():

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')
    SECRET_PATH = os.path.join(ROOT_DIR, 'secrets.ini')

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    bootstrap_server = config['kafka'].get('broker')
    topic = config['kafka'].get('topic')

    listner = StdOutListener(bootstrap_server, topic)

    config.read(SECRET_PATH)
    api_key = config['twitter'].get('api_key').encode()
    api_secret = config['twitter'].get('api_secret').encode()
    client_token = config['twitter'].get('client_token').encode()
    client_token_secret = config['twitter'].get('client_token_secret').encode()

    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(client_token, client_token_secret)
    api = tweepy.API(auth)
    stream = Stream(auth, listner, tweet_mode='extended')
    stream.filter(track=["#Track"], languages=["en"])


if __name__ == "__main__":
    main()
