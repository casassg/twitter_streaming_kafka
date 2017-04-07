#import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os

#Variables that contains the user credentials to access Twitter API 
access_token = os.environ.get("ACCESS_TOKEN","ENTER YOUR ACCESS TOKEN")
access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET","ENTER YOUR ACCESS TOKEN SECRET")
consumer_key = os.environ.get("CONSUMER_KEY","ENTER YOUR API KEY") 
consumer_secret = os.environ.get("CONSUMER_SECRET","ENTER YOUR API SECRET")

tokens = os.environ.get("TOKENS","")

tokens = tokens.replace(" ","").split(",")


#This is a basic listener that just prints received tweets to stdout.
class KafkaListener(StreamListener):

    def  on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, KafkaListener())



    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=tokens)
