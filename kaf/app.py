import dataclasses as dc
import functools
import json
import logging
import time

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s')


@dc.dataclass(unsafe_hash=True)
class Result:
    value: dict
    key: object = None


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
                    if msg.error() is not None:
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.info(
                                f' {msg.topic()}[{msg.partition()}] reached end \
                                of offset {msg.offset()}')
                        else:
                            logger.error(f' Fatal error: {msg.error()}')
                            self.consumer.commit(msg)
                            raise KafkaException(msg.error())
                    else:
                        try:
                            for result, publish_to in self._process_message(msg):
                                if publish_to is not None:
                                    self._produce(result, publish_to=publish_to)
                                else:
                                    logger.info(f'No producer set')
                            t1 = time.perf_counter()
                            for callback in self.on_processed_callbacks:
                                callback(msg, t1 - t0)
                        except JSONDecodeError as error:
                            # Non-retriable error
                            logger.error(error)
                            for callback in self.on_failed_callbacks:
                                callback(msg, inner_error)
                        finally:
                            self.consumer.commit(msg)
            except(BufferError):
                self.logger.info('Sleeping for 10 seconds.')
                time.sleep(10)
            except Exception as error:
                self.logger.error(error)
                self.logger.info('Re-initialising clients')
                self._initialise_clients()
                self.logger.info('Sleeping for 5 seconds.')
                time.sleep(10)

    def _coalesce_result(self, result):
        if result is None:
            return result
        if isinstance(result, Result):
            return result
        return Result(value=result)

    def _process_message(self, msg):
        msg_error = msg.error()
        if msg_error is None:
            # Process single message
            subs = self._get_subs(msg.topic)
            for func, publish_to in subs:
                value_dict = json.loads(msg.value())
                for result in func(value_dict):
                    result = self._coalesce_result(result)
                    yield result, publish_to
        elif msg_error == KafkaError._PARTITION_EOF:
            # Log the EOF, but don't process the message further
            self.logger.info(f'{msg.topic()}[{msg.partition()}] reached end of offset {msg.offset()}')
        else:
            raise Exception(msg_error)

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
            key=message.key,
            value=json.dumps(message.value).encode('utf-8')
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
