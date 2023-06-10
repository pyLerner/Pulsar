import config
from pulsar_file_transfer import get_json_meta
import asyncio

host = config.HOST


async def main():
    await get_json_meta(pulsar_host=host)

asyncio.run(main())
