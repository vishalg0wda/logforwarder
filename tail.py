
import os
import time
from Queue import Queue
import threading
import logging
from os import path

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                   )
MAX_BUFFER_SIZE = 10
LOG_QUEUE = Queue()

def touch(fpath):
    open(fpath, 'a').close()

class Tail(threading.Thread):
    """This class tails a file continuously and pushes lines into
       a global queue.
    """
    def __init__(self, filepath, q, stop_event, interval=1, out_file=None):
        threading.Thread.__init__(self)
        self.filepath = filepath
        self.q = q
        self.interval = interval
        self.out_file = out_file or '%s.offset' % path.basename(filepath)
        touch(self.out_file)
        self.offset = 0
        try:
            self.offset = int(open(self.out_file).read().strip())
        except ValueError:
            open(self.out_file, 'w').write('0')
        self.fh = open(self.filepath, 'r')
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            self.fh.seek(self.offset)
            while self.q.qsize() < MAX_BUFFER_SIZE:
                line = self.fh.readline()
                if not line:
                    break
                self.q.put(line)
                self.offset = self.fh.tell()
            time.sleep(self.interval)
        else:
            with open(self.out_file, 'w') as out_fh:
                out_fh.write(str(self.offset))
            self.fh.close()

class Consumer(threading.Thread):
    def __init__(self, q, stop_event):
        threading.Thread.__init__(self)
        self.q = q
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            while not self.q.empty():
                logging.debug(self.q.get().rstrip('\n'))
                self.q.task_done()
            time.sleep(2)

STOP_EVENT = threading.Event()

tailer1 = Tail('/var/log/apache2/access_log', LOG_QUEUE, STOP_EVENT)
tailer2 = Tail('/var/log/apache2/error_log', LOG_QUEUE, STOP_EVENT)

tailer1.start()
tailer2.start()

consumer = Consumer(LOG_QUEUE, STOP_EVENT)
consumer.start()

timer = threading.Timer(100, STOP_EVENT.set)
timer.start()
