
import time
import logging
import threading
from central import POLL_INTERVAL
from stream import SocketStreamer

class Consumer(threading.Thread):
    def __init__(self, q, stop_event, name=None, poll_interval=POLL_INTERVAL):
        threading.Thread.__init__(self)
        self.q = q
        self.stop_event = stop_event
        self.sent_records = 0
        if name is not None:
            self.name = name
        self.poll_interval = poll_interval

    def run(self):
        sock = SocketStreamer()
        while sock is None:
            sock = SocketStreamer()
            time.sleep(self.poll_interval)
        while not self.stop_event.is_set():
            while not self.q.empty():
                app, log = self.q.get()
                logging.debug(
                    'consumed from queue, size(%d)'%self.q.qsize())
                while True:
                    try:
                        sent = sock.send(log, app=app)
                    except AttributeError:
                        time.sleep(self.poll_interval)
                        sock = SocketStreamer()
                    else:
                        if sent > 0:
                            break
                        else:
                            time.sleep(self.poll_interval)
                    sock = SocketStreamer()
                self.sent_records += 1
                self.q.task_done()
            time.sleep(self.poll_interval)
