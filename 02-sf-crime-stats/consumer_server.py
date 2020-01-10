from kafka import KafkaConsumer
import json


TOPIC = "com.udacity.sf.crime.calls.v1"
GROUP_ID = "com.udacity.sf.crime.calls.consumer.v1"
BOOTSTRAP_SERVER = "localhost:9092"

class ConsumerServer():

    def __init__(self, topic, group_id, bootstrap_servers, **kwargs):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers
        )

    def consume(self):
        for message in self.consumer:
            print(json.loads(message.value))

if __name__ == "__main__":
    consumer_server = ConsumerServer(
        topic=TOPIC,
        group_id=GROUP_ID,
        bootstrap_servers=[BOOTSTRAP_SERVER]
    )

    consumer_server.consume()
