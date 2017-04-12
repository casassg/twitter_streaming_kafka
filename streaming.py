# import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os
import json
import logging, time
from kafka import KafkaProducer

# Variables that contains the user credentials to access Twitter API
access_token = os.environ.get("ACCESS_TOKEN", "ENTER YOUR ACCESS TOKEN")
access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET", "ENTER YOUR ACCESS TOKEN SECRET")
consumer_key = os.environ.get("CONSUMER_KEY", "ENTER YOUR API KEY")
consumer_secret = os.environ.get("CONSUMER_SECRET", "ENTER YOUR API SECRET")

tokens = os.environ.get("TOKENS", "")

tokens = tokens.replace(" ", "").split(",")
kafka_server = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    retries=5,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# This is a basic listener that just prints received tweets to stdout.
class KafkaListener(StreamListener):
    def on_data(self, data):
        producer.send('raw_tweets', data)
        # logging.info("Tweet transmitted")
        return True

    def on_error(self, status):
        logging.error('Tweet error, status: %s' % status)


def main():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, KafkaListener())
    logging.info('Twitter stream opened')
    stream.filter(track=tokens)
    time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    logging.info('Tracking keywords: %s' % ','.join(tokens))
    logging.info('Kafka servers: %s' % ','.join(kafka_server))
    logging.info('Start stream track')
    main()
