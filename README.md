# Kaf

> Så er det tid til en kop kaf'

Kaf is a small Python framework for creating Kafka apps. It is inspired by Faust, but differs in the following ways:

- Kaf is synchronous (async is future work)
- Kaf is compatible with Azure Eventhubs (over Kafka interface)
- Kaf is designed to work with different brokers for the consumer and producer
- Kaf expects JSON events

The framework depends on Confluent Kafka.

## How to use

Hello world:

```python
import logging

from kaf import KafkaApp

consumer_conf = {'bootstrap.servers': 'kafka:9092', 'group.id': 'myapp'}
producer_conf = {'bootstrap.servers': 'kafka:9092'}

app = KafkaApp('myapp', consumer_conf, producer_conf)
counter_ok = 0
counter_failed = 0

app.logger.setLevel(logging.INFO)

@app.process(topic='foo', publish_to='bar')
def uppercase_everything(msg):
  return {key:str(val).upper() for key, val in msg.items()}

@app.on_processed
def inc_ok(stats):
  counter_ok += 1

@app.on_failed
def inc_failed(stats):
  counter_failed += 1


if __name__ == '__main__':
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
