import datetime
import json
import logging
import random
import time
import uuid
import yaml

from kaf import KafkaApp, Result

with open('config.yaml.local', "r") as stream:
    config = yaml.safe_load(stream)


app = KafkaApp(
    'myapp',
    consumer_config=config['KafkaConsumer'],
    producer_config=config['KafkaProducer'],
    consumer_timeout=5
)

app.logger.setLevel(logging.INFO)

def make_value(old_counter=None):
    value = {
        'id': str(uuid.uuid4()),
        'created_at': datetime.datetime.utcnow().isoformat()
    }
    if old_counter is None:
        value['counter'] = 0
    else:
        value['counter'] = old_counter + 1
    return value

@app.process(topic='test', publish_to='test')
def process(msg):
    incoming_value = json.loads(msg.value())
    outgoing_value = make_value(incoming_value['counter'])
    time.sleep(1)
    yield Result(json.dumps(outgoing_value).encode('utf-8'))

@app.on_processed
def on_processed(msg, seconds_elapsed):
    pass

if __name__ == '__main__':
    # Start the merry-go-round
    app._initialise_clients()
    first_result = Result(value=json.dumps(make_value()).encode('utf-8'))
    app._produce(result=first_result, publish_to='test')
    app.producer.flush()
    app.run()
