from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from kafka import KafkaProducer


access_token= ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("twitter", data.encode('utf-8'))
        return True
    def on_error(self, status):
        print(status)


listener= StdOutListener()
stream = Stream(auth, listener)
stream.filter(track="BBNaijaLockdown")
