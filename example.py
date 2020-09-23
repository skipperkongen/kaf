import logging
import random

from kaf import KafkaApp

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'myapp'
}
producer_conf = {
    'bootstrap.servers': 'kafka:9092'
}

app = KafkaApp('myapp', consumer_conf, producer_conf)

app.logger.setLevel(logging.INFO)

@app.process(topic='foo', publish_to='bar')
def process(msg):
    if random.random() < 0.1:
        raise Exception('Something went wrong')
    else:
        return {'foo': 'bars'}

if __name__ == '__main__':
    app.run2()
