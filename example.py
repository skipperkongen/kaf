import logging
import random

from kaf import KafkaApp, Consumer, Producer


consumer = Consumer()
producer = Producer()
app = KafkaApp('myapp', consumer, producer, batch_size=3)

app.logger.setLevel(logging.INFO)

@app.process
def process(msg):
    if random.random() < 0.1:
        raise Exception('Something went wrong')
    else:
        return {'foo': 'bars'}

if __name__ == '__main__':
    app.run()
