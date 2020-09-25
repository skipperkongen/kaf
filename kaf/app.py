import dataclasses as dc
import functools
import json
import logging
import random
import signal
import sys
import time

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from retrying import retry

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s')


@dc.dataclass(unsafe_hash=True)
class Result:
    value: dict
    key: object = None


class KafkaApp:

    def __init__(self, name, consumer_config, producer_config, consumer_batch_size=1, consumer_timeout=60):
        self.name = name
        self.consumer_config = consumer_config
        self.producer_config = producer_config
        self.processors = []
        self.subs = {}
        self.logger = logging.getLogger(name)
        self.consumer_batch_size = consumer_batch_size
        self.consumer_timeout = consumer_timeout
        self.on_processed_callbacks = []
        # self.on_failed_callbacks = []
        # self.consumer = Consumer(consumer_config)
        # self.producer = Producer(producer_config)
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.producer.flush()
        sys.exit()

    def _initialise_clients(self):
        self.logger.debug('Initialising clients')
        self.consumer = Consumer(self.consumer_config)
        self.producer = Producer(self.producer_config)
        topics = list(self.subs.keys())
        self.logger.debug(f'Subscribing to topics: {topics}')
        self.consumer.subscribe(topics)

    def run(self):
        """
        Main loop. Should never exit.
        """
        self.logger.debug('Run loop started')
        self._initialise_clients()
        while True:
            self.logger.debug('Iteration started')
            try:
                msgs = self._consume()
                for msg in msgs:
                    t0 = time.perf_counter()
                    self.logger.debug('Processing message')
                    if msg.error() is not None:
                        self.logger.debug(f'Message has an error: {msg.error()}')
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            self.logger.info(
                                f' {msg.topic()}[{msg.partition()}] reached end \
                                of offset {msg.offset()}'
                            )
                            continue
                        else:
                            self.consumer.commit(msg)
                            raise KafkaException(msg.error())
                    else:
                        self.logger.debug('Message has no error')
                        # User code
                        process_output = self._process_message(msg)
                        t1 = time.perf_counter()
                        to_publish = [
                            (result, publish_to) for
                            result, publish_to in process_output
                            if publish_to is not None
                        ]
                        # TODO: make callbacks suppress exceptions
                        for callback in self.on_processed_callbacks:
                            callback(msg, t1 - t0)

                        # Publish results
                        for result, publish_to in to_publish:
                            self._produce(result, publish_to=publish_to)

                        self.logger.info(f'Commiting message: {msg.value()}')
                        self.consumer.commit(msg)
            except BufferError as error:
                self.error(error)
                self.logger.info('Sleeping for 10 seconds.')
                time.sleep(10)
            except Exception as error:
                # TODO: make callbacks suppress exceptions
                # for callback in self.on_failed_callbacks:
                #     callback(msg, error)
                self.logger.error(error)
                self.logger.info('Re-initialising clients')
                self._initialise_clients()
                self.logger.info('Sleeping for 3 seconds.')
                time.sleep(3)
            self.logger.debug('Iteration ended')


    def _process_message(self, msg):
        # Process single message
        topic = msg.topic()
        subs = self._get_subs(topic)
        self.logger.debug(f'Found {len(subs)} function(s) subscribed to topic "{topic}"')
        for func, publish_to in subs:
            for result in func(msg):
                self.logger.debug(f'User function "{func.__name__}" produced a result')
                yield result, publish_to

    def _get_subs(self, topic):
        return self.subs.get(topic) or []


    @retry(retry_on_exception=(KafkaException, BufferError), wait_fixed=5000)
    def _consume(self):
        self.logger.debug(f'Consuming messages...')
        msgs = self.consumer.consume(
            num_messages=self.consumer_batch_size,
            timeout=self.consumer_timeout
        )
        if len(msgs) == 0:
            minutes = self.consumer_timeout/60
            self.logger.info(f'Nothing received for {minutes} minutes.')
        else:
            self.logger.info(f'Consumed {len(msgs)} new messages from Kafka')
            for msg in msgs:
                self.logger.info(f'Message consumed: {msg.value()}')
        return msgs


    @retry(retry_on_exception=(KafkaException, BufferError), wait_fixed=10000)
    def _produce(self, result, publish_to):
        key = str(result.key or random.random()).encode('utf-8')
        self.producer.produce(
            topic=publish_to,
            key=key,
            value=result.value
        )
        self.logger.info(f'Message produced: {result.value}')
        self.producer.poll(0)


    def process(self, topic, publish_to=None):
        """
        Use to decorate functions that process single events
        """
        def process_decorator(func):
            sub = (func, publish_to)
            self.subs.setdefault(topic, []).append(sub)
            return func
        return process_decorator


    def on_processed(self, func):
        self.on_processed_callbacks.append(func)
        return func


    # def on_failed(self, func):
    #     self.on_failed_callbacks.append(func)
    #     return func
