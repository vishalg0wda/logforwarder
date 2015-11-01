
import time
import config
import logging
import threading
from stream import SocketStreamer

class Consumer(threading.Thread):
    def __init__(self, q, stop_event,
                 poll_interval, name=None):
        threading.Thread.__init__(self)
        self.q = q
        self.stop_event = stop_event
        self.sent_records = 0
        if name is not None:
            self.name = name
        self.poll_interval = poll_interval

    def run(self):
        sock = SocketStreamer(host=config.LOGGER_HOST,
                              port=config.LOGGER_PORT,
                              token=config.TOKEN)
        while sock is None:
            sock = SocketStreamer(host=config.LOGGER_HOST,
                                  port=config.LOGGER_PORT,
                                  token=config.TOKEN)
            time.sleep(self.poll_interval)
        while not self.stop_event.is_set():
            while not self.q.empty():
                entry = self.q.get()
                logging.debug(
                    'consumed from queue, size(%d)'%self.q.qsize())
                while True:
                    try:
                        sent = sock.send(entry)
                    except AttributeError:
                        time.sleep(self.poll_interval)
                        sock = SocketStreamer(host=config.LOGGER_HOST,
                                              port=config.LOGGER_PORT,
                                              token=config.TOKEN)
                    else:
                        if sent > 0:
                            break
                        else:
                            time.sleep(self.poll_interval)
                    sock = SocketStreamer(host=config.LOGGER_HOST,
                                          port=config.LOGGER_PORT,
                                          token=config.TOKEN)
                self.sent_records += 1
                self.q.task_done()
            time.sleep(self.poll_interval)
