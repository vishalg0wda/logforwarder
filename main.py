
import sys
import time
import atexit
import logging
from tail import Tail
from consumer import Consumer
from central import LOG_QUEUE, STOP_EVENT, FILES

# this isn't being called during sys.exit :/
atexit.register(STOP_EVENT.set)

def event_loop():
    tailer_threads = []
    for name, path in FILES.items():
        tailer = Tail(path, LOG_QUEUE, STOP_EVENT, name=name)
        tailer.start()
        tailer_threads.append(tailer)
    lumberjack = Consumer(LOG_QUEUE, STOP_EVENT, name='jack')
    lumberjack.start()
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
                '{0.name} sent {0.sent_records} records!'.format(lumberjack))
            sys.exit('shutting down streamer...')

if __name__ == '__main__':
    event_loop()
