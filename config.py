
import json

LOGGER_HOST = None
LOGGER_PORT = None
TOKEN = None
POLL_INTERVAL = None
NUM_CONSUMERS = None
FILES = None

def assign_globals(fpath):
    data = {}
    with open(fpath) as fh:
        data = json.load(fh)
    for k, v in data.items():
        globals()[k.upper()] = v

import threading
from Queue import Queue

STOP_EVENT = threading.Event()
LOG_QUEUE = Queue()
