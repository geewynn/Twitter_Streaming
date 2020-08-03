from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from kafka import KafkaProducer


access_token= "1057305579121713152-yydhlFYRwnXUvj9XtHzyYpOAuoTT2J"
access_token_secret = "9PolB0nXPWA6Y6j5kYdpzqIKgIyE1lA1RIxKeKXZnIBcJ"
consumer_key = "ow7stEAStsyNDqc5u51YphqDc"
consumer_secret = "2HMj5hQ0M1GPvocsgCl3skFwUvRDn8A91Pi4FGi2fHGEhqMbVk"

def twitterauth(consumer_key,consumer_secret,access_token,access_token_secret):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

topic_name = "twitter"

def TwitterStreamer():
        while True:
            listener = ListenerTS() 
            auth = twitterauth()
            stream = Stream(auth, listener)
            stream.filter(track=["#BBNaijaLockdown"], stall_warnings=True, languages= ["en"])

def ListenerTS(raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True