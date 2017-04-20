# import the necessary methods from tweepy library
import json
import logging
import os
import time

from kafka import KafkaProducer, SimpleClient
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Variables that contains the user credentials to access Twitter API
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'raw_tweets')
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "ENTER YOUR ACCESS TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET", "ENTER YOUR ACCESS TOKEN SECRET")
CONSUMER_KEY = os.environ.get("CONSUMER_KEY", "ENTER YOUR API KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET", "ENTER YOUR API SECRET")

TOKENS = os.environ.get("TOKENS", "").split(",")

KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    retries=5,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def ensure_topic():
    client = SimpleClient(hosts=KAFKA_SERVERS)
    client.ensure_topic_exists(KAFKA_TOPIC)
    client.close()


# This is a basic listener that just prints received tweets to stdout.
class KafkaListener(StreamListener):
    def on_data(self, data):

        try:
            producer.send(KAFKA_TOPIC, data)
        except:
            ensure_topic()
            producer.send(KAFKA_TOPIC, data)

        # logging.info("Tweet transmitted")
        return True

    def on_error(self, status):
        logging.error('Tweet error, status: %s' % status)


def main():
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = Stream(auth, KafkaListener())
    logging.info('Twitter stream opened')
    stream.filter(track=TOKENS)
    time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    logging.info('Tracking keywords: %s' % ','.join(TOKENS))
    logging.info('Kafka servers: %s' % ','.join(KAFKA_SERVERS))
    logging.info('Start stream track')
    ensure_topic()
    main()
