
import sys
import time
import atexit
import logging
from tail import Tail
from consumer import Consumer
from central import LOG_QUEUE, STOP_EVENT, FILES, NUM_CONSUMERS

# this isn't being called during sys.exit :/
atexit.register(STOP_EVENT.set)

def event_loop():
    tailer_threads = []
    for name, path in FILES.items():
        tailer = Tail(path, LOG_QUEUE, STOP_EVENT, name=name)
        tailer.start()
        tailer_threads.append(tailer)
    consumer_map = {0: 'yoda', 1: 'obiwan',
                    2: 'vader', 3: 'chewbacca'}
    consumer_threads = []
    for i in range(NUM_CONSUMERS):
        consumer = Consumer(LOG_QUEUE, STOP_EVENT,
                            name=consumer_map.get(i % 4))
        consumer.start()
        consumer_threads.append(consumer)
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
            for consumer in consumer_threads:
                logging.info(
                    '{0.name} sent {0.sent_records} records!'.format(consumer))
            sys.exit('shutting down streamer...')

if __name__ == '__main__':
    event_loop()
