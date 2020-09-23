import logging
import functools
import time
import json

from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s')

class KafkaApp:

    def __init__(self, name, consumer_conf=None, producer_conf=None, producer_topic=None, consumer_batch_size=1, consumer_timeout=1800):
        self.name = name
        self.consumer_conf = consumer_conf
        self.producer_conf = producer_conf
        self.batch_size = batch_size
        self.processors = []
        self.error_handlers = []
        self.logger = logging.getLogger(name)
        self.consumer_batch_size = consumer_batch_size
        self.consumer_timeout = consumer_timeout
        self.producer_topic = producer_topic

    def _initialise_clients(self):
        self.consumer = Consumer(self.consumer_conf)
        self.producer = Consumer(self.producer_cons)

    def run(self):
        """
        Main loop. Should never exit.
        """
        self._initialise_clients()
        while True:
            try:
                self.logger.info('Checking for new messages')
                msgs = self._consume()
                for msg in msgs:
                    if msg.error() is not None:
                        # Always commit on error, so not re-process msg again
                        self.consumer.commit(msg)
                        if msg.error() == KafkaError._PARTITION_EOF:
                            # Just an EOF
                            self.logger.info(f'{msg.topic()}[{msg.partition()}] reached end of offset {msg.offset()}')
                            continue
                        else:
                            # Some other error. Raise exception and chill
                            raise Exception(msg.error())
                    for process in self.processors:
                        result = process(msg)
                        if (self.producer and self.producer_topic) is not None:
                            self.producer.publish(result)
                        else:
                            logger.info(f'No producer set')
                    self.consumer.commit(msg)
            except Exception as error:
                self.logger.error(error)
                self.logger.info(f"Recreating clients and sleeping for 10 seconds.")
                self._initialise_clients()
                time.sleep(3)

    def _consume(self):
        msgs = self.consumer.consume(
            num_messages=self.consumer_batch_size,
            timeout=self.consumer_timeout
        )
        if len(msgs) == 0:
            minutes = self.consumer_timeout/60
            self.logger.info(f'Nothing received for {minutes} minutes.')
        else:
            self.logger.info(f'Read {len(msgs)} new messages')
        return msgs

    def _produce(self, value):
        self.producer.produce(topic=self.producer_topic,
                       value=json.dumps(value.as_dict()).encode('utf-8'))
        logger.info(f'Message produced: {json.dumps(value.as_dict())}')
        self.producer.poll(0)


    def process(self, func):
        """
        Use to decorate functions that process single events
        """
        self.processors.append(func)
        return func

    def handle_error(self, func):
        """
        Use to decorate functions that should be called when an error occurs
        """
        self.error_handlers.append(func)
        return func
