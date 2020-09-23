import unittest
from kaf import KafkaApp

class TestKafkaApp(unittest.TestCase):

    def setUp(self):

        self.app = KafkaApp('', {}, {})

        @self.app.process(topic='foo', publish_to='bar')
        def process(msg):
            message = msg.get('message') or '-'
            return {'x': 42, 'y': message.upper()}

    def test_1(self):
        subs = self.app._get_subs('foo')
        self.assertTrue(len(subs) == 1)

    def test_2(self):
        subs = self.app._get_subs('foo')
        self.assertTrue(subs[0][1] == 'bar')

    def test_3(self):
        subs = self.app._get_subs('foo')
        for func,publish_to in subs:
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


if __name__ == '__main__':
    unittest.main()
