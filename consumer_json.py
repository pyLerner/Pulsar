import pulsar
from pulsar.schema import JsonSchema
from producer_json import Meta

# host = 'nii.global:6650'
host = 'nii.local:6650'
# host = '188.134.85.21:6650'

topic = 'meta_json'

client = pulsar.Client(f'pulsar://{host}')

consumer = client.subscribe(
    topic,
    'my-subscription',
    schema=JsonSchema(Meta)
)

while True:
    msg = consumer.receive()
    try:
        data = msg.value()

        print(data)

        consumer.acknowledge(msg)

    except Exception:
        consumer.negative_acknowledge(msg)

client.close()
