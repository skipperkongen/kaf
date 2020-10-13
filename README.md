# Kaf

> Så er det tid til en kop kaf'

Kaf is a small Python framework for creating Kafka apps. It is inspired by Faust, but differs in the following ways:

- Kaf is synchronous (async is future work)
- Kaf is compatible with Azure Eventhubs (over Kafka interface)
- Kaf is designed to work with different brokers for the consumer and producer

The framework depends on Confluent Kafka.

## How to use

Minimal example:

```python
import logging

from kaf import KafkaApp

consumer_config = {'bootstrap.servers': 'kafka:9092', 'group.id': 'myapp'}
producer_config = {'bootstrap.servers': 'kafka:9092'}

app = KafkaApp(
    'myapp',
    consumer_config=consumer_config,
    producer_config=producer_config,
    consumer_timeout=5,
    consumer_batch_size=1    
)
app.logger.setLevel(logging.INFO)

@app.process(topic='foo', publish_to='bar', accepts='json', returns='json')
def add_one(input):
    number = input['number']
    yield {'result':  number+1}, bytes(number)

@app.on_processed
def done(msg, seconds_elapsed):
    app.logger.info(f'Processed message in {seconds_elapsed} seconds')


if __name__ == '__main__':
  app.run()
```

## How errors are handled

Kafka functions keep trying until they succeed.
Each user function will get one chance to process each incoming message. Any exception raised by a user functions will only be logged, but otherwise ignored.
If a user wants to implement retrying they can do that, but it will stall the pipeline until the function returns.

## Future work:

Features to be added:

- Add decorators for app events `on_consume`, `on_processed` and `on_publised`. This will allow to hook up e.g. Datadog metrics to these events.

## How to deploy a new version

Steps (can maybe be improved):

1. change version and download_url in setup.py
1. git add + commit + push
1. create new release in GitHub (check source-code link, should match download_url)
1. Run `python setup.py sdist`
1. Run `twine upload dist/* --verbose` (if not installed, `pip install twine` first)



Useful links used:

- https://realpython.com/primer-on-python-decorators/#registering-plugins
- https://medium.com/@joel.barmettler/how-to-upload-your-python-package-to-pypi-65edc5fe9c56
