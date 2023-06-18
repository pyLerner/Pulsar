import asyncio

import pulsar
from pulsar.schema import Record, BytesSchema, JsonSchema
from pulsar.schema import String, Boolean
from typing import List
import aiofiles
import aiopulsar
from aiofiles import os
from datetime import datetime


class FileMetadata(Record):
    """
    Формат словаря для отправки метаданных с подтверждением получения файлов
    """
    deviceName = String()
    deviceSerialNumber = String()
    fileName = String()
    isExist = Boolean()


def log_print(string: str) -> None:
    """
    Печать в выводом времемени в начале строки и ключевого слова MAIN
    :param string: строка для печати
    :return: None
    """
    print(
        f'{datetime.now().isoformat(timespec="milliseconds").replace("T", " ")}'
        f' MAIN'
        f'  {string}'
    )


async def send_metadata(
        pulsar_host: str,
        metadata: FileMetadata,
        topic: str = 'confirmation'
) -> None:
    """
    Функция отправки метаданных о получении файлов очередью pulsar
    :param pulsar_host: сервер очереди pulsar ip:port
    :param metadata: объект класса FileMetadata
    :param topic: топик очереди (отличный от топика для отправки файлов)
    :return: None
    """

    async with aiopulsar.connect(f'pulsar://{pulsar_host}') as client:
        async with client.create_producer(
                topic,
                schema=JsonSchema(FileMetadata),
        ) as producer_meta:

            await producer_meta.send(
                metadata
            )


async def get_json_meta(
        pulsar_host: str,
        topic: str = 'confirmation'
) -> None:
    """
    Функция получения JSON метаданных о доставке файла через очередь pulsar
    :param pulsar_host: сервер очереди pulsar ip:port
    :param topic: топик очереди
    :return: None
    """

    async with aiopulsar.connect(f'pulsar://{pulsar_host}') as client:
        async with client.subscribe(
                topic,
                'my-subscription',
                schema=JsonSchema(FileMetadata),
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=5000
        ) as consumer_JSON:

            while True:
                msg = await consumer_JSON.receive()
                try:
                    data = msg.value()
                    log_print(f'Отправка метаданных о файле {data}')

                    if data.isExist:
                        # Здесь может быть добавлен вызов функции удаления
                        # успешно переданного файла

                        log_print(f'Файл '
                              f'{data.deviceName}/'
                              f'{data.deviceSerialNumber}/'
                              f'{data.fileName} '
                              f'МОЖНО УДАЛЯТЬ')

                    await consumer_JSON.acknowledge(msg)

                except Exception:
                    await consumer_JSON.negative_acknowledge(msg)


#:TODO properties сделать функцией
async def transfer_one_file(
        pulsar_host: str,
        device: str,
        serial: str,
        filename: str,
        data: bytes,
        size: str,
        topic: str,
) -> None:

    """
    Функция отправляет в очередь pulsar один файл

    :param pulsar_host: сервер очереди pulsar ip:port
    :param device: имя устройства (если удобно, то модель)
    :param serial: серийный номер устройства
    :param filename: имя передаваемого файла
    :param data: байт-содержимое файла
    :param size: размер в байтах передаваемого файла
    :param topic: топик очереди
    :return: None
    """

    async with aiopulsar.connect(f'pulsar://{pulsar_host}') as client:
        async with client.create_producer(
            topic,
            schema=BytesSchema(),
            chunking_enabled=True,
            send_timeout_millis=200_000,     # увеличивает допустимый таймаут от брокера
        ) as producer_file:

            await producer_file.flush()

            properties = {
                'deviceModel': device,
                'serialNumber': serial,
                'fileName': filename,
                'fileSize': size
            }

            await producer_file.send(
                data,
                properties,     # Передача имени файла и размера вместе с байтами
            )

            log_print(f'Файл {filename} с устройства {device} передан')


#TODO: сделать properties функцией
async def transfer_files(
        pulsar_host: str,
        device: str,
        serial: str,
        topic: str
) -> List:

    async with aiopulsar.connect(f'pulsar://{pulsar_host}') as client:
        async with client.create_producer(
            topic,
            schema=BytesSchema(),
            chunking_enabled=True,
            send_timeout_millis=200_000,     # увеличивает допустимый таймаут от брокера
        ) as producer_file:

            await producer_file.flush()

            tasks = []
            for name in await aiofiles.os.listdir(device):

                file = device + "/" + name
                log_print(f'Передача файла {file}')
                size = await aiofiles.os.stat(file)
                size = size.st_size

                async with aiofiles.open(file, 'rb') as f:
                    data = await f.read()

                properties = {
                    'deviceModel': device,
                    'serialNumber': serial,
                    'fileName': name,
                    'fileSize': str(size)
                }

                log_print(f'Передача свойств файла {str(properties)}')

                task = await producer_file.send(
                            data,
                            properties,     # Передача имени файла и размера вместе с байтами
                            # deliver_at=10,
                            # deliver_after=timedelta(seconds=100)
                        )

                tasks.append(task)
                log_print(f'{file} поставлен в очередь задач')

            log_print(f'Файлы с устройства {device} переданы')

            return tasks


async def get_files(
        pulsar_host: str,
        out_dir: str,
        topic: str
) -> None:

    """
    Функция получения бинарных сообщений в режиме chunking
    :param pulsar_host: сервер очереди pulsar  ip:port
    :param out_dir: каталог для сохранения файлов
    :param topic: топик очереди
    :return: None
    """
    async with aiopulsar.connect(f'pulsar://{pulsar_host}') as client:

        async with client.subscribe(
                topic,
                'my-subscription',
                schema=BytesSchema(),
                auto_ack_oldest_chunked_message_on_queue_full=True,
                max_pending_chunked_message=10,
                initial_position=pulsar.InitialPosition.Latest,
                receiver_queue_size=5000
        ) as consumer_file:

            while True:
                msg = await consumer_file.receive()
                try:
                    data = msg.value()

                    log_print(f'Получено сообщение о файле {msg.properties()}')

                    ############ QUEUEE CLEANING ################
                    if 'fileName' not in msg.properties().keys():
                        log_print(f'Свойства сообщания не соответсвуют формату. Отбрасывается.')
                        await consumer_file.acknowledge(msg)
                    #############################################

                    initial_name = msg.properties()['fileName']
                    device = msg.properties()['deviceModel']
                    serial = msg.properties()['serialNumber']
                    size = msg.properties()['fileSize']

                    # Сюда можно добавить создание подкаталогов для каждого устройства
                    # ...

                    name = out_dir + "/" + initial_name
                    # name = os.path.join(out_dir, name)

                    async with aiofiles.open(name, 'wb') as file:
                        await file.write(data)
                        await file.flush()

                        # Проверка размера файла
                        n_size = await aiofiles.os.stat(name)
                        n_size = n_size.st_size

                        if n_size == int(size):
                            is_exist = True
                        else:
                            is_exist = False

                        log_print(f'файл {name} доставлен '
                              f'и может быть удален с устройства '
                              f'{device} / {serial}')

                    metadata = FileMetadata(
                        deviceName=device,
                        deviceSerialNumber=serial,
                        fileName=initial_name,
                        isExist=is_exist
                    )

                    await send_metadata(
                        pulsar_host=pulsar_host,
                        metadata=metadata
                    )

                    log_print(f'Отправка метаданных о получении файла {metadata}')

                    await consumer_file.acknowledge(msg)

                except Exception:
                    await consumer_file.negative_acknowledge(msg)


if __name__ == "__main__":
    pass
