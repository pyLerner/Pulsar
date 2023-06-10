from pulsar_file_transfer import get_json_meta
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST')

async def main():
    await get_json_meta(pulsar_host=host)

asyncio.run(main())
