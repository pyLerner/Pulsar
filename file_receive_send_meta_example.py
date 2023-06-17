import config
from pulsar_file_transfer import get_files
import asyncio


host = config.HOST
out_dir = config.OUT_DIR
topic_file = config.TOPIC_FILE


async def main():
    await get_files(
        pulsar_host=host,
        out_dir=out_dir,
        topic=topic_file
    )

asyncio.run(main())
