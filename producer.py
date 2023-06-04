import pulsar
import time

host = 'nii.global:6650'
# host = 'nii.local:6650'
# host = '188.134.85.21:6650'

topic = 'flac'
client = pulsar.Client('pulsar://nii.global:6650')
producer = client.create_producer(topic)

for i in range(100):
    producer.send(('hello-pulsar-%d' % i).encode('utf-8'))
    time.sleep(0.5)

client.close()