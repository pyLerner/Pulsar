import asyncio
# import os

import aiohttp
import aiofiles
from aiofiles import os

import re
from typing import List, Tuple

# Пример http каталога
url_example = 'https://mirror.yandex.ru/gentoo-distfiles/distfiles/00/'


async def parsing(url: str) -> Tuple[List[str], List[str]]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print(resp.status)
            text = await resp.text()

            # Парсинг ссылок на файлы
            text = re.findall(r'\".+\"', text)
            files = list(map(lambda x: x[1:-1], text[1:]))
            urls = list(map(lambda x: url + x, files))

            return files, urls


async def download_file(
        session: aiohttp.ClientSession,
        url: str,
        filename: str
) -> Tuple[str, bytes]:

    async with session.get(url) as response:
        assert response.status == 200
        data = await response.read()

        async with aiofiles.open('out/'+filename, 'wb') as f:

            await f.write(data)

        # For large files use response.content.read(chunk_size) instead.
        return filename, await response.read()

# async def save_file(data: bytes) -> None:


async def download_multiple(
        url: str,
        session: aiohttp.ClientSession
) -> List:

    files, urls = await parsing(url)

    # Ограничение количества файлов: n (для демонстрации)
    n = 10

    download_futures = [
        download_file(
            session,
            ur,
            file
        ) for file, ur in zip(
            files,
            urls[:n]
        )
    ]
    print('Results')
    for download_future in asyncio.as_completed(download_futures):
        result = await download_future
        # print(url)
        # print('finished:', result)
    return urls


async def main():
    async with aiohttp.ClientSession() as session:
        result = await download_multiple(url_example, session)
        print('finished:', await os.listdir('out'))


if __name__ == "__main__":

    asyncio.run(main())
