from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from kafka import KafkaProducer


access_token= "1057305579121713152-yydhlFYRwnXUvj9XtHzyYpOAuoTT2J"
access_token_secret = "9PolB0nXPWA6Y6j5kYdpzqIKgIyE1lA1RIxKeKXZnIBcJ"
consumer_key = "ow7stEAStsyNDqc5u51YphqDc"
consumer_secret = "2HMj5hQ0M1GPvocsgCl3skFwUvRDn8A91Pi4FGi2fHGEhqMbVk"


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
