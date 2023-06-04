import pulsar
from pulsar.schema import JsonSchema, Record, Integer, String

# host = 'nii.global:6650'
host = 'nii.local:6650'
# host = '188.134.85.21:6650'
topic = 'meta_json'

class Meta(Record):
    name = String()
    size = Integer()
    address = String()


client = pulsar.Client(f'pulsar://{host}')

producer = client.create_producer(
    topic,
    schema=JsonSchema(Meta),
)

data = Meta(
    name='My File',
    size=1234,
    address='192.168.99.99'
)

producer.send(data)

client.close()
