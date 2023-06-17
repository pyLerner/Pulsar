# device - каталог с файлами (имитация устройства с известным серийным номером)

from pulsar_file_transfer import transfer_files
import asyncio
import config


host = config.HOST
topic = config.TOPIC_FILE


async def main():

    device = asyncio.create_task(
        transfer_files(
            pulsar_host=host,
            device=config.DEVICE_2,
            serial=config.SERIAL_2,
            topic=topic
        )
    )

    # await zero_device
    # await first_device
    await device


if __name__ == '__main__':

    asyncio.run(main())
