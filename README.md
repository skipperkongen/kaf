# Kaf

> Så er det tid til en kop kaf'

Kaf is a small Python framework for creating Kafka apps. It is inspired by Faust, but differs in the following ways:

- Kaf is synchronous (async is future work)
- Kaf is compatible with Azure Eventhubs (over Kafka interface)

The framework depends on Confluent Kafka.

## How to use

Hello world:

```python
import logging

from kaf import KafkaApp

consumer_conf = {}
producer_conf = {}
app = KafkaApp('myapp', consumer_conf, producer_conf)

app.logger.setLevel(logging.INFO)

@app.process
def uppercase_messages(msg):
  return {'message_upper': msg['message'].upper()}

if __name__ == '__main__':
  app.run()
```

## How errors are handled

In case of an unhandled error, the framework will reinitialise the consumer and producer
and wait for 3 seconds.

## Future work:

Features to be added:

- Add decorators for app events `on_consume`, `on_processed` and `on_publised`. This will allow to hook up e.g. Datadog metrics to these events.
