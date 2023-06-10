import os

from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv('HOST')
TOPIC_FILE = 'transfer_file'
TOPIC_META = 'confirmation'

OUT_DIR = 'out'
if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)

# Секция для тестов c multitransfer
DEVICE_0 = 'in.big'
DEVICE_1 = 'in'
DEVICE_2 = 'in.middle'

SERIAL_0 = 'BIG43256'
SERIAL_1 = 'SMALL314'
SERIAL_2 = 'MIDDLE65'