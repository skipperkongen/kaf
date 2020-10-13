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


def retry_if_buffer_error_or_retriable(exception):
    """Return True if BufferError or retriable KafkaError, False otherwise"""
    return (
        isinstance(exception, KafkaException) and exception.args[0].retriable()
        or
        isinstance(exception, BufferError)
    )


class KafkaApp:

    def __init__(self, name, consumer_config, producer_config, consumer_batch_size=1, consumer_timeout=60):
        self.name = name
        self.consumer_config = consumer_config
        self.producer_config = producer_config
        self.processors = []
        self.subs = {}
        self.logger = logging.getLogger(f'KafkaApp[{name}]')
        self.consumer_batch_size = consumer_batch_size
        self.consumer_timeout = consumer_timeout
        self.on_processed_callbacks = []
        self.consumer = Consumer(consumer_config)
        self.producer = Producer(producer_config)
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.running = False

    def exit_gracefully(self, signum, frame):
        self.producer.flush()
        self.running = False
        self.logger.info('Exiting gracefully')
        sys.exit()

    @retry(wait_fixed=5000, retry_on_exception=retry_if_buffer_error_or_retriable)
    def _initialise_clients(self):
        """
        Try to initialise until successful
        """
        self.logger.info('Trying to initialise clients...')
        #self.consumer = Consumer(self.consumer_config)
        #self.producer = Producer(self.producer_config)
        topics = list(self.subs.keys())
        self.logger.debug(f'Subscribing to topics: {topics}')
        self.consumer.subscribe(topics)
        self.logger.info('Clients initialised')

    @retry(wait_fixed=5000, retry_on_exception=retry_if_buffer_error_or_retriable)
    def _consume_messages(self):
        """
        Try to consume until successful (unless error is fatal)
        """
        return self.consumer.consume(
            num_messages=self.consumer_batch_size,
            timeout=self.consumer_timeout
        )

    @retry(wait_fixed=5000, retry_on_exception=retry_if_buffer_error_or_retriable)
    def _produce_message(self, key, value, publish_to):
        """
        Try to produce until successful (unless error is fatal)
        """
        self.producer.produce(
            key=key,
            value=value,
            topic=publish_to
        )
        self.producer.poll(0)

    @retry(wait_fixed=5000, retry_on_exception=retry_if_buffer_error_or_retriable)
    def _commit_message(self, msg):
        """
        Try to commit until successful (unless error is fatal)
        """
        self.consumer.commit(msg)

    def run(self):
        """
        Main loop of kaf. Should never exit.

        Pseudo-code:

            inputs = consume()
            for input in inputs:
                outputs = process(input)
                for output in outputs:
                    produce(output)
                commit(input)

        """
        self.logger.debug('Run loop started')
        self._initialise_clients()

        # Loop forever
        self.running = True
        while self.running:
            iter_t0 = time.perf_counter()
            self.logger.debug('Iteration started')

            # Try to consume messages until successful
            self.logger.info('Consuming messages')
            msgs = self._consume_messages()
            if len(msgs) == 0:
                self.logger.info(f'No messages consumed for {self.consumer_timeout} seconds')
            else:
                self.logger.info(f'Consumed {len(msgs)} message(s)')
            for i, msg in enumerate(msgs):
                # Case 1a: msg has retriable error => don't commit
                # Case 1b: msg has fatal error => commit
                # Case 2: msg was processed successfully => commit
                # Case 3: msg processing failed => don't commit

                    # Completely process each message before continuing to next
                    try:
                        i += 1
                        t0 = time.perf_counter()
                        self.logger.info(f'Input message[{i}] processing started')

                        error = msg.error()
                        if error is not None:
                            # Case 1a / 1b
                            if error.code() == KafkaError._PARTITION_EOF:
                                self.logger.info(
                                    f' {msg.topic()}[{msg.partition()}] reached end \
                                    of offset {msg.offset()}'
                                )
                            else:
                                self.logger.error(error)
                        else:
                            # Call user functions
                            process_output = self._process_message(msg)
                            # Publish results
                            for j, (value, key, publish_to) in enumerate(process_output):
                                j += 1
                                self._produce_message(
                                    key=key,
                                    value=value,
                                    publish_to=publish_to
                                )
                                self.logger.info(f'Output message[{j}] produced')
                            # We don't care if callback raises an Exception
                            t1 = time.perf_counter()
                            for callback in self.on_processed_callbacks:
                                try:
                                    callback(msg, t1 - t0)
                                except Exception as e_inner:
                                    self.logger.exception(e)
                    except Exception as e:
                        self.logger.error(f'An error occured in run loop: {e}')
                        self.logger.exception(e)
                    finally:
                        try:
                            self._commit_message(msg)
                            self.logger.info(f'Input message[{i}] committed')
                        except Exception as e:
                            self.logger.error(f'Input message[{i}] not committed')
                            self.logger.exception(e)

            iter_t1 = time.perf_counter()
            self.logger.debug(f'Iteration completed in {iter_t1 - iter_t0} seconds')

    def _process_message(self, msg):
        """
        Process a single message by calling all subscribed user functions
        """
        input_bytes = msg.value()
        topic = msg.topic()

        subs = self._get_subs(topic)
        self.logger.debug(f'Found {len(subs)} function(s) subscribed to topic "{topic}"')
        for func, publish_to, accepts, returns in subs:
            try:
                input_obj = self._parse(input_bytes, accepts)
                outputs = func(input_obj)
                self.logger.info(f'User function "{func.__name__}" completed successfully')
                for output_obj, key in outputs:
                    if publish_to is None: continue
                    key = self._keyify(key)
                    output_bytes = self._serialize(output_obj, returns)
                    yield output_bytes, key, publish_to
            except Exception as e:
                self.logger.error(f'User function "{func.__name__}" raised an exception: {e}')
                self.logger.exception(e)


    def _parse(self, input_bytes, accepts):
        if accepts == 'bytes':
            return input_bytes
        elif accepts == 'json':
            return json.loads(input_bytes)
        else:
            raise TypeError(f'Unsupported value for accepts parameter: {accepts}')

    def _keyify(self, key):
        if key is None:
            return key
        else:
            return bytes(key)

    def _serialize(self, output_obj, returns):
        """
        Serialize an output from a user function, i.e. turn it into bytes.
        """
        if returns == 'bytes':
            # Assert that already serialized
            if type(output_obj) != bytes:
                raise TypeError(f'User function should return bytes, but returned {type(output_obj)}')
            return output_obj
        elif returns == 'json':
            try:
                return json.dumps(output_obj).encode('utf-8')
            except:
                raise TypeError(f'User function returned value that can not be serialized to JSON: {output_obj}')
        else:
            raise TypeError(f'User function returned unsupported type: {type(output_obj)}')

    def _get_subs(self, topic):
        """
        Returns a list of user functions subscriptions on a a topic.
        """
        return self.subs.get(topic) or []


    def process(self, topic, publish_to=None, accepts='bytes', returns='bytes'):
        """
        Decorator for user functions that processes a single event. The value
        of the event is passed to the user function.
        - The accepts parameter can be set to 'bytes' or 'json'
        - The returns parameter can be set to 'bytes' or 'json'
        The user function should return results as `yield value, key`, where
        the type of value depends on the returns parameter (either raw bytes or something that can
        be passed to json.dumps). The key should be either None or bytes.
        """
        assert(accepts in ['bytes', 'json'])
        assert(returns in ['bytes', 'json'])
        def process_decorator(func):
            sub = (func, publish_to, accepts, returns)
            self.subs.setdefault(topic, []).append(sub)
            return func
        return process_decorator


    def on_processed(self, func):
        """
        Decorator for user callbacks
        """
        self.on_processed_callbacks.append(func)
        return func
