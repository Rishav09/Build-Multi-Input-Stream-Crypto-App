# Create an Application instance with Kafka configs
from quixstreams import Application
import time

app = Application(
    broker_address='127.0.0.1:31234',
    consumer_group='example'
)

# Define a topic "my_topic" with JSON serialization
topic = app.topic(name='my_topic', value_serializer='json')

event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}

# Create a Producer instance
with app.get_producer() as producer:

    # Serialize an event using the defined Topic 
    message = topic.serialize(key=event["id"], value=event)

    # Produce a message into the Kafka topic
    producer.produce(
        topic=topic.name, 
        value=message.value, 
        key=message.key
    )

    time.sleep(1)