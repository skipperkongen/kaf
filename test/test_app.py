import json
import unittest

from kaf import KafkaApp


class Message:
    def __init__(self, topic, value, error, partition, offset):
        self.topic = topic
        self._value = value
        self._error = error
        self.partition = partition
        self.offset = offset

    def value(self):
        return self._value

    def error(self):
        return self._error


class TestKafkaApp(unittest.TestCase):

    def setUp(self):

        self.app = KafkaApp(
            'testapp',
            consumer_config={'bootstrap.servers': 'kafka:9092', 'group.id': '0'},
            producer_config={'bootstrap.servers': 'kafka:9092'})

        @self.app.process(topic='foo', publish_to='bar', accepts='json', returns='json')
        def process(input_value):
            yield {'x': 42}, bytes(1)


        @self.app.on_processed
        def inc_ok(msg, seconds_elapsed):
            print('OK')


    def test_1(self):
        subs = self.app._get_subs('foo')
        self.assertTrue(len(subs) == 1)

    def test_2(self):
        subs = self.app._get_subs('foo')
        self.assertTrue(subs[0][1] == 'bar')

    def test_3(self):
        subs = self.app._get_subs('foo')
        for func, publish_to, accepts, returns in subs:
            input = {'message': 'foo'}
            self.assertTrue(callable(func))
            for value, key in func(input):
                self.assertTrue(type(key) == bytes)
                self.assertTrue(json.dumps(value))

    def test_4(self):
        def fff():
            @self.app.process()
            def process2(_):
                return {}
        self.assertRaises(Exception, fff)

    def test_5(self):
        @self.app.process(topic='foo')
        def process2(_):
            return {}
        subs = self.app._get_subs('foo')
        self.assertTrue(len(subs) == 2)


    def test_6(self):
        msg = Message(topic=lambda: 'foo2', value=lambda:'{"foo": "bar"}', error=lambda:None, partition=lambda:0, offset=lambda:0)
        for result, publish_to in self.app._process_message(msg):
            self.assertTrue(type(result) == dict)

    def test_7(self):
        msg = Message(topic='foo2', value=None, error='BADNESS', partition=0, offset=0)
        self.assertRaises(Exception, self.app._process_message(msg))


if __name__ == '__main__':
    unittest.main()
