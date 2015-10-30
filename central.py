
import logging
import threading
from Queue import Queue

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                   )

MAX_BUFFER_SIZE = 10 # this should come from config
STOP_EVENT = threading.Event()
LOG_QUEUE = Queue()
