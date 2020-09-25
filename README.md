# Kaf

> Så er det tid til en kop kaf'

Kaf is a small Python framework for creating Kafka apps. It is inspired by Faust, but differs in the following ways:

- Kaf is synchronous (async is future work)
- Kaf is compatible with Azure Eventhubs (over Kafka interface)
- Kaf is designed to work with different brokers for the consumer and producer
- Kaf expects JSON events

The framework depends on Confluent Kafka.

## How to use

Minimal example:

```python
import logging

from kaf import KafkaApp, Result

consumer_config = {'bootstrap.servers': 'kafka:9092', 'group.id': 'myapp'}
producer_config = {'bootstrap.servers': 'kafka:9092'}

app = KafkaApp(
    'myapp',
    consumer_config=consumer_config,
    producer_config=producer_config
)
app.logger.setLevel(logging.INFO)

@app.process(topic='foo', publish_to='bar')
def hello(msg):
    yield Result('hello'.encode('utf-8'), key="world")

@app.on_processed
def done(msg, seconds_elapsed):
    app.logger.info('Processed one message')

if __name__ == '__main__':
  app.run()
```

The merry-go-round:

```python
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
    if random.random() < 0.25:
        raise Exception('Random error occured while processing [USER CODE]')
    incoming_value = json.loads(msg.value())
    outgoing_value = make_value(incoming_value['counter'])
    app.logger.info(f'Processing message [USER CODE] ')
    time.sleep(1)
    yield Result(json.dumps(outgoing_value).encode('utf-8'))

@app.on_processed
def on_processed(msg, seconds_elapsed):
    app.logger.info(f'Processing completed in {seconds_elapsed} seconds [USER CODE]')

if __name__ == '__main__':
    # Start the merry-go-round
    app._initialise_clients()
    first_result = Result(value=json.dumps(make_value()).encode('utf-8'))
    app._produce(result=first_result, publish_to='test')
    app.producer.flush()
    app.run()
```

## How errors are handled

In case of an unhandled error, the framework will reinitialise the consumer and producer
and wait for 3 seconds.

## Future work:

Features to be added:

- Add decorators for app events `on_consume`, `on_processed` and `on_publised`. This will allow to hook up e.g. Datadog metrics to these events.

## How to deploy a new version

Steps (can maybe be improved):

1. change version in setup.py
1. git add + commit + push
1. create new release in GitHub (copy link)
1. update download_url in setup.py
1. git add + commit + push (again)
1. Run `python setup.py sdist`
1. Run `twine upload dist/* --verbose` (if not installed, `pip install twine` first)



Useful links used:

- https://realpython.com/primer-on-python-decorators/#registering-plugins
- https://medium.com/@joel.barmettler/how-to-upload-your-python-package-to-pypi-65edc5fe9c56
