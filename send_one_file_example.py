import asyncio
from pulsar_file_transfer import transfer_one_file
import os
from dotenv import load_dotenv
import config

load_dotenv()

host = os.getenv('HOST')
topic = config.TOPIC_FILE
device = 'DEVICE-XXX'
serial = '1234'
file = 'in.middle/unetbootin-mac-702.dmg'
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
        topic=topic
    )

asyncio.run(main())
