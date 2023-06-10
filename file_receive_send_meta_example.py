from pulsar_file_transfer import get_files
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST')
out_dir = 'out'

async def main():
    await get_files(
        pulsar_host=host,
        out_dir='out'
    )

asyncio.run(main())
