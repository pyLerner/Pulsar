import pulsar
from pulsar.schema import BytesSchema

# host = 'nii.global:6650'
host = 'nii.local:6650'
# host = '188.134.85.21:6650'

topic = 'flac'

client = pulsar.Client(f'pulsar://{host}')

consumer = client.subscribe(
    topic,
    'my-subscription',
    schema=BytesSchema(),
    auto_ack_oldest_chunked_message_on_queue_full=True,
    max_pending_chunked_message=10
)

while True:
    msg = consumer.receive()
    try:
        data = msg.value()

        # print(data)
        # print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))

        print(data)
        # print(msg.message_id())
        # print(msg.properties())

        # name += '.flac'

        with open('out/test.flac', 'wb') as file:
            file.write(data)

        consumer.acknowledge(msg)

    except Exception:
        consumer.negative_acknowledge(msg)

client.close()
