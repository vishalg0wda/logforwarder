
import sys
import time
import atexit
import logging
import threading
from tail import Tail
from stream import SocketStreamer
from central import LOG_QUEUE, STOP_EVENT

class Consumer(threading.Thread):
    def __init__(self, q, stop_event, name=None):
        threading.Thread.__init__(self)
        self.q = q
        self.stop_event = stop_event
        self.sent_records = 0
        if name is not None:
            self.name = name

    def run(self):
        while not self.stop_event.is_set():
            with SocketStreamer('localhost', 9898) as sock:
                while not self.q.empty():
                    log = self.q.get()
                    logging.debug(
                        'consumed from queue, size(%d)'%self.q.qsize())
                    while True:
                        sent = sock.send(log)
                        if sent > 0:
                            break
                    self.sent_records += 1
                    self.q.task_done()
            time.sleep(1)


# this isn't being called during sys.exit :/
atexit.register(STOP_EVENT.set)

def event_loop():
    tailer1 = Tail('/var/log/apache2/access_log', LOG_QUEUE, STOP_EVENT, name='access_log')
    tailer2 = Tail('/var/log/apache2/error_log', LOG_QUEUE, STOP_EVENT, name='error_log')
    tailer1.start()
    tailer2.start()
    consumer1 = Consumer(LOG_QUEUE, STOP_EVENT, name='optimus')
    consumer2 = Consumer(LOG_QUEUE, STOP_EVENT, name='megatron')
    consumer1.start()
    consumer2.start()
    # this part continues to block even though all
    # queue items were processed :/
    # LOG_QUEUE.join() # Commenting for now...
    # logging.debug('finished processing queue')
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            STOP_EVENT.set()
            print
            logging.info(
                '{0.name} sent {0.sent_records} records!'.format(consumer1))
            logging.info(
                '{0.name} sent {0.sent_records} records!'.format(consumer2))
            sys.exit('shutting down streamer...')

if __name__ == '__main__':
    event_loop()
