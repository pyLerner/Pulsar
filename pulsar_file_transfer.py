import pulsar
from pulsar.schema import Record, BytesSchema, JsonSchema
from pulsar.schema import String, Boolean
# import asyncio
import aiofiles
import aiopulsar
import os


class FileMetadata(Record):
    """
    Формат словаря для отправки метаданных с подтверждением получения файлов
    """
    deviceName = String()
    deviceSerialNumber = String()
    fileName = String()
    isExist = Boolean()


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
                    print(data)

                    if data.isExist:
                        # Здесь может быть добавлен вызов функции удаления
                        # успешно переданного файла

                        print(f'Файл '
                              f'{data.deviceName}/'
                              f'{data.deviceSerialNumber}/'
                              f'{data.fileName} '
                              f'МОЖНО УДАЛЯТЬ')

                    await consumer_JSON.acknowledge(msg)

                except Exception:
                    await consumer_JSON.negative_acknowledge(msg)


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

            print(f'Файл {filename} с устройства {device} передан')


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
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=5000
        ) as consumer_file:

            while True:
                msg = await consumer_file.receive()
                try:
                    data = msg.value()

                    print(msg.properties())

                    name = msg.properties()['fileName']

                    device = msg.properties()['deviceModel']
                    serial = msg.properties()['serialNumber']
                    size = msg.properties()['fileSize']

                    # Сюда можно добавить создание подкаталогов для каждого устройства
                    # ...

                    name = os.path.join(out_dir, name)

                    async with aiofiles.open(name, 'wb') as file:
                        await file.write(data)
                        await file.flush()

                        # Проверка размера файла
                        if os.stat(name).st_size == int(size):
                            is_exist = True
                        else:
                            is_exist = False

                    print(f'файл {name} доставлен '
                          f'и может быть удален с устройства '
                          f'{device} / {serial}')

                    metadata = FileMetadata(
                        deviceName=device,
                        deviceSerialNumber=serial,
                        fileName=name,
                        isExist=is_exist
                    )

                    await send_metadata(
                        pulsar_host=pulsar_host,
                        metadata=metadata
                    )

                    print('metadata was sent')

                    await consumer_file.acknowledge(msg)

                except Exception:
                    await consumer_file.negative_acknowledge(msg)


if __name__ == "__main__":
    pass

# Пример использования:

    # host = 'nii.global:6650'
    # device = 'DEVICE-XXX'
    # serial = '1234'
    # filename = 'in/501.png'
    # size = os.stat(filename).st_size
    #
    # async def main():
    #
    #     await get_files(
    #         pulsar_host=host,
    #         out_dir='out'
    #     )
    #
    #     with open(filename, 'rb') as f:
    #         data = f.read()
    #
    #         await transfer_one_file(
    #             pulsar_host=host,
    #             device=device,
    #             serial=serial,
    #             filename=filename,
    #             data=data,
    #             size=str(size)
    #         )
    #
    # asyncio.run(main())
