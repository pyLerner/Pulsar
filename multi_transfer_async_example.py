# device - каталог с файлами (имитация устройства с известным серийным номером)

from pulsar.schema import BytesSchema
import aiopulsar

# Для асинхронного чтения/записи в файл
import aiofiles
import asyncio
from typing import List
import os
import config

host = config.HOST
topic = config.TOPIC_FILE


async def transfer_files(
        device: str,
        serial: str
) -> List:

    async with aiopulsar.connect(f'pulsar://{host}') as client:
        async with client.create_producer(
            topic,
            schema=BytesSchema(),
            chunking_enabled=True,
            send_timeout_millis=200_000,     # увеличивает допустимый таймаут от брокера
        ) as producer_file:

            await producer_file.flush()

            tasks = []
            for name in os.listdir(device):

                file = os.path.join(device, name)
                size = os.stat(file).st_size

                print(file)

                async with aiofiles.open(file, 'rb') as f:
                    data = await f.read()

                # print(data)

                properties = {
                    'device': device,
                    'serialNumber': serial,
                    'filename': name, 
                    'size': str(size)
                }

                print(properties)

                task = await producer_file.send(
                            data,
                            properties,     # Передача имени файла и размера вместе с байтами
                            # deliver_at=10,
                            # deliver_after=timedelta(seconds=100)
                        )

                print('?')
                tasks.append(task)
                print(f'{file} was appended in task')

            print(f'Файлы с устройства {device} переданы')

            return tasks


async def main():
    # zero_device = asyncio.create_task(
    #     transfer_files(
    #         device=os.getenv('DEVICE_0'),
    #         serial=os.getenv('SERIAL_0')
    #     )
    # )
    # first_device = asyncio.create_task(
    #     transfer_files(
    #         device=os.getenv('DEVICE_1'),
    #         serial=os.getenv('SERIAL_1')
    #     )
    # )
    second_device = asyncio.create_task(
        transfer_files(
            device=os.getenv('DEVICE_2'),
            serial=os.getenv('SERIAL_2')
        )
    )

    # await zero_device
    # await first_device
    await second_device


if __name__ == '__main__':

    asyncio.run(main())
