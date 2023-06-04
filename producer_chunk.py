import pulsar
from pulsar.schema import BytesSchema
import os

# host = 'nii.global:6650'
host = 'nii.local:6650'
# host = '188.134.85.21:6650'

topic = 'flac'
in_dir = 'in.big'

client = pulsar.Client(f'pulsar://{host}')

producer = client.create_producer(
    topic,
    # block_if_queue_full=True,
    # batching_enabled=True,
    # batching_max_publish_delay_ms=10,
    schema=BytesSchema(),
    chunking_enabled=True,
    # compression_type='zlib'
)

# for file in os.listdir(in_dir):
#     name = file.split('.')[0]
#     file = os.path.join(dir, file)
#     print(file)
#     size = os.stat(file).st_size
#
#     with open(file, 'rb') as f:
#         byte = f.read()
#
#     # producer.send_async(byte, callback)
#     producer.send(byte)

def callback(res, msg_id):
    print('Message published res=%s', res)

file = os.path.join(in_dir, '10 - Singularity.flac')

with open(file, 'rb') as f:
    data = f.read()

producer.send_async(data, callback=callback)

client.close()
