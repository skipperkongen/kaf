import dataclasses as dc
import functools
import hashlib
import json
import logging
import random
import signal
import sys
import time

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from retrying import retry

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s')

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
        self.running = False

    def exit_gracefully(self, signum, frame):
        self.producer.flush()
        self.running = False
        self.logger.info('Exiting gracefully')
        sys.exit()

    @retry(wait_fixed=5000)
    def _initialise_clients(self):
        """
        Try to initialise until successful
        """
        try:
            self.logger.info('Initialising clients')
            self.consumer = Consumer(self.consumer_config)
            self.producer = Producer(self.producer_config)
            topics = list(self.subs.keys())
            self.logger.debug(f'Subscribing to topics: {topics}')
            self.consumer.subscribe(topics)
        except Exception as e:
            self.logger.error(e)
            raise e


    @retry(wait_fixed=5000)
    def _consume_messages(self):
        """
        Try to consume until successful
        """
        try:
            self.logger.info('Consuming message')
            msgs = self.consumer.consume(
                num_messages=self.consumer_batch_size,
                timeout=self.consumer_timeout
            )
            return msgs
        except KafkaException as e:
            kafka_error = e.args[0]
            self.logger.error(kafka_error)
            if kafka_error.retriable(): raise e


    @retry(wait_fixed=5000)
    def _produce_message(self, key, value, publish_to):
        """
        Try to produce until successful
        """
        try:
            self.logger.debug(f'Producing message: KEY={key}, VALUE={value}')
            self.logger.info(f'Producing message')
            self.producer.produce(
                key=key,
                value=value,
                topic=publish_to
            )
            self.producer.poll(0)
        except KafkaException as e:
            kafka_error = e.args[0]
            self.logger.error(kafka_error)
            if kafka_error.retriable(): raise e


    @retry(wait_fixed=5000)
    def _commit_message(self, msg):
        """
        Try to commit until successful
        """
        try:
            self.consumer.commit(msg)
        except KafkaException as e:
            kafka_error = e.args[0]
            self.logger.error(kafka_error)
            if kafka_error.retriable(): raise e

    def run(self):
        """
        Main loop. Should never exit.

        Pseudo-code:

            inputs = consume()
            for input in inputs:
                outputs = process(input)
                for output in outputs:
                    produce(output)
                commit(input)

        """
        self.logger.debug('Run loop started')
        # Try to initialise clients until successful
        self._initialise_clients()

        # Loop forever
        self.running = True
        while self.running:
            iter_t0 = time.perf_counter()
            self.logger.debug('Iteration started')

            # Try to consume messages until successful
            msgs = self._consume_messages()
            if len(msgs) == 0:
                self.logger.info(f'No messages consumed for {self.consumer_timeout} seconds')
            else:
                self.logger.info(f'Consumed {len(msgs)} message(s)')
            for msg in msgs:
                # Case 1a: msg has retriable error => don't commit
                # Case 1b: msg has fatal error => commit
                # Case 2: msg was processed successfully => commit
                # Case 3: msg processing failed => don't commit

                    # Completely process each message before continuing to next
                    try:
                        t0 = time.perf_counter()
                        commit = False
                        sha256_value = self._get_sha256_hash(msg.value())
                        self.logger.info(f'Processing message; SHA256_VALUE={sha256_value}')
                        self.logger.debug(f'Processing message; VALUE={msg.value()}')

                        if msg.error() is not None:
                            # Case 1a / 1b
                            commit = not error.retriable()
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                self.logger.info(
                                    f' {msg.topic()}[{msg.partition()}] reached end \
                                    of offset {msg.offset()}'
                                )
                            else:
                                self.logger.error(msg.error())

                        else:
                            #
                            process_output = self._process_message(msg)
                            # Publish results
                            for i, (key, value, publish_to) in enumerate(process_output):
                                sha256_prod = self._get_sha256_hash(value)
                                self._produce_message(
                                    key=key,
                                    value=value,
                                    publish_to=publish_to
                                )
                                self.logger.info(f'Produced message[{i+1}]; SHA256_VALUE={sha256_prod}')
                            self.logger.info(f'Processed message; SHA256_VALUE={sha256_value}')
                            # We don't care if callback raises an Exception
                            t1 = time.perf_counter()
                            commit = True
                            for callback in self.on_processed_callbacks:
                                callback(msg, t1 - t0)
                    except KafkaException as e:
                        kafka_error = e.args[0]
                        commit = not kafka_error.retriable()
                        self.logger.error(e)
                    except Exception as e:
                        self.logger.error(e)
                    finally:
                        if commit:
                            self._commit_message(msg)
                            self.logger.info(f'Committed message; SHA256_VALUE={sha256_value}')
                        else:
                            self.logger.info(f'Did not commit message; SHA256_VALUE={sha256_value}')

            iter_t1 = time.perf_counter()
            self.logger.debug(f'Iteration completed in {iter_t1 - iter_t0} seconds')

    def _get_sha256_hash(self, value):
        if value is not None:
            return hashlib.sha256(value).hexdigest()
        else:
            return '-'

    def _process_message(self, msg):
        # Process single message
        topic = msg.topic()
        subs = self._get_subs(topic)
        self.logger.debug(f'Found {len(subs)} function(s) subscribed to topic "{topic}"')
        for func, publish_to in subs:
            self.logger.info(f'Calling user function "{func.__name__}"')
            for key, value in func(msg):
                if publish_to is None: continue
                key = self._bytify(key or random.random())
                value = self._bytify(value)
                yield key, value, publish_to

    def _bytify(self, x):
        if type(x) == dict:
            return json.dumps(x).encode('utf-8')
        try:
            return bytes(x)
        except:
            return str(x).encode('utf-8')

    def _get_subs(self, topic):
        return self.subs.get(topic) or []


    def process(self, topic, publish_to=None):
        """
        Decorator for user functions that process single events
        """
        def process_decorator(func):
            sub = (func, publish_to)
            self.subs.setdefault(topic, []).append(sub)
            return func
        return process_decorator


    def on_processed(self, func):
        """
        Decorator for user callbacks
        """
        self.on_processed_callbacks.append(func)
        return func


    # def on_failed(self, func):
    #     self.on_failed_callbacks.append(func)
    #     return func
