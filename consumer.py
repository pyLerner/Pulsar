import pulsar

host = 'nii.global:6650'
# host = 'nii.local:6650'
# host = '188.134.85.21:6650'

topic = 'flac'

client = pulsar.Client(f'pulsar://{host}')
consumer = client.subscribe(topic, subscription_name='my-sub',
                            auto_ack_oldest_chunked_message_on_queue_full=True,
                            max_pending_chunked_message=10
                            )

while True:
    msg = consumer.receive()
    print("Received message: '%s'" % msg.data(), '\n\n')

    data = msg.value()
    with open('out/test.flac', 'wb') as file:
        file.write(data)

    consumer.acknowledge(msg)

client.close()
