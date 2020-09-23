import logging
import functools
import time
import json

from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s')

class KafkaApp:

    def __init__(self, name, consumer_conf, producer_conf, consumer_batch_size=1, consumer_timeout=1800):
        self.name = name
        self.consumer_conf = consumer_conf
        self.producer_conf = producer_conf
        self.processors = []
        self.subs = {}
        self.logger = logging.getLogger(name)
        self.consumer_batch_size = consumer_batch_size
        self.consumer_timeout = consumer_timeout
        self.on_processed_callbacks = []
        self.on_failed_callbacks = []


    def _initialise_clients(self):
        self.consumer = Consumer(self.consumer_conf)
        self.producer = Producer(self.producer_conf)
        self.consumer.subscribe(self.subs.keys())


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
                    t0 = time.perf_counter()
                    # process a single message
                    try:
                        outcomes = self._process_message(msg)
                        for result, publish_to in outcomes:
                            if publish_to is not None:
                                self._produce(result, publish_to=publish_to)
                            else:
                                logger.info(f'No producer set')
                        t1 = time.perf_counter()
                        for callback in self.on_processed_callbacks:
                            callback(msg, t1 - t0)
                    except Exception as inner_error:
                        # An unhandled exception occured while handling message
                        self.logger.error(inner_error)
                        self._heal()
                        for callback in self.on_failed_callbacks:
                            callback(msg, inner_error)
                    finally:
                        # completely done with message, commit it
                        msg.commit()
            except Exception as error:
                # An unhandled exception occured in the pipeline
                self.logger.error(error)  # BOMB, if fails app crashes
                self._heal()  # BOMB, if fails app crashes


    def _process_message(self, msg):
        msg_error = msg.error()
        if msg_error is None:
            # Process single message
            subs = self._get_subs(msg.topic)
            for func, publish_to in subs:
                value_dict = json.loads(msg.value())
                result = func(value_dict)
                yield result, publish_to
        elif msg_error == KafkaError._PARTITION_EOF:
            # Log the EOF, but don't process the message further
            self.logger.info(f'{msg.topic()}[{msg.partition()}] reached end of offset {msg.offset()}')
        else:
            raise Exception(msg_error)


    def _heal(self, seconds=3):
        self.logger.info(f"Recreating clients and sleeping for 3 seconds.")
        self._initialise_clients()
        time.sleep(seconds)


    def _get_subs(self, topic):
        return self.subs.get(topic) or []


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


    def _produce(self, message, publish_to):
        self.producer.produce(
            topic=publish_to,
            value=json.dumps(message).encode('utf-8')
        )
        logger.info(f'Message produced: {json.dumps(message.as_dict())}')
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


    def on_failed(self, func):
        self.on_failed_callbacks.append(func)
        return func
