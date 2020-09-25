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
        self.counter_ok = 0
        self.counter_failed = 0

        @self.app.process(topic='foo', publish_to='bar')
        def process(msg):
            message = msg.get('message') or '-'
            return {'x': 42, 'y': message.upper()}

        @self.app.process(topic='foo2')
        def process(msg):
            return {}

        @self.app.on_processed
        def inc_ok(msg, seconds_elapsed):
            self.counter_ok += 1

        @self.app.on_failed
        def inc_failed(msg, error):
            self.counter_failed += 1


    def test_1(self):
        subs = self.app._get_subs('foo')
        self.assertTrue(len(subs) == 1)

    def test_2(self):
        subs = self.app._get_subs('foo')
        self.assertTrue(subs[0][1] == 'bar')

    def test_3(self):
        subs = self.app._get_subs('foo')
        for func, publish_to in subs:
            input = {'message': 'foo'}
            output = func(input)
            self.assertTrue(callable(func))
            self.assertTrue(type(output) == dict)
            self.assertTrue(output['y'] == input['message'].upper())

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
