from loguru import logger
from quixstreams import Application


def run(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_consumer_group: str,
    candle_seconds: int,
):
    """
    Transforms a stream of input trades into a stream of output candles.

    In 3 steps:
    - Ingests trades from the `kafka_input_topic`
    - Aggregates trades into candles
    - Produces candles to the `kafka_output_topic`

    Args:
        kafka_broker_address (str): Kafka broker address
        kafka_input_topic (str): Kafka input topic name
        kafka_output_topic (str): Kafka output topic name
        kafka_consumer_group (str): Kafka consumer group name
        candle_seconds (int): Candle duration in seconds
    """

    app = Application(
        broker_address=kafka_broker_address, consumer_group=kafka_consumer_group
    )

    trades_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    candles_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Step 1. Ingest trades from the input Kafka topic
    sdf = app.dataframe(topic=trades_topic)

    # Log input messages
    sdf = sdf.update(lambda message: logger.info(f'Input: {message}'))

    # Step 2. [Placeholder for candle logic using candle_seconds if needed]

    # Step 3. Output to the output topic
    sdf = sdf.to_topic(candles_topic)

    app.run()


if __name__ == '__main__':
    run(
        kafka_broker_address='localhost:31234',
        kafka_input_topic='trades',
        kafka_output_topic='candles',
        kafka_consumer_group='my-consumer-group',
        candle_seconds=60,
    )
