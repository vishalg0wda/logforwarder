
import logging
import threading
from Queue import Queue

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                   )
STOP_EVENT = threading.Event()
LOG_QUEUE = Queue()

# all these should come from config
MAX_BUFFER_SIZE = 100
FILES = {'monty': '/var/log/apache2/access_log',
         'cheese': '/var/log/apache2/error_log'}
POLL_INTERVAL = 2 # seconds
LOGGER_HOST = 'localhost'
LOGGER_PORT = 9898
TOKEN = 'entergeneratedtokenhere'
