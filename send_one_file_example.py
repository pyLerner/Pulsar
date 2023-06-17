import asyncio
from pulsar_file_transfer import transfer_one_file
import os
import config


host = config.HOST
topic_file = config.TOPIC_FILE

device = 'DEVICE-XXX'
serial = '1234'
file = 'in.middle/anydesk_6.2.1-1_amd64.deb'

size = str(os.stat(file).st_size)
filename = file.split('/')[-1]


async def main():

    with open(file, 'rb') as f:
        data = f.read()

    await transfer_one_file(
        pulsar_host=host,
        device=device,
        serial=serial,
        filename=filename,
        data=data,
        size=size,
        topic=topic_file
    )

asyncio.run(main())
