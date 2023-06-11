import asyncio
import aiohttp
import re
from typing import List, Tuple

# Пример http каталога
url = 'https://mirror.yandex.ru/gentoo-distfiles/distfiles/00/'


async def parsing(url: str) -> List:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print(resp.status)
            text = await resp.text()

            # Парсинг ссылок на файлы
            text = re.findall(r'\".+\"', text)
            files = list(map(lambda x: x[1:-1], text[1:]))
            urls = list(map(lambda x: url + x, files))

            return urls


async def download_file(
        session: aiohttp.ClientSession,
        url: str
) -> Tuple:

    async with session.get(url) as response:
        assert response.status == 200

        # For large files use response.content.read(chunk_size) instead.
        return url, await response.read()


async def download_multiple(url: str, session: aiohttp.ClientSession):
    urls = await parsing(url)

    # Ограничение количества файлов: N (для демонстрации)
    N = 3

    download_futures = [download_file(session, ur) for ur in urls[:N]]
    print('Results')
    for download_future in asyncio.as_completed(download_futures):
        result = await download_future
        print('finished:', result)
    return urls


async def main():
    async with aiohttp.ClientSession() as session:
        result = await download_multiple(url, session)
        print('finished:', result)

asyncio.run(main())
