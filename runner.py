
import sys
import time
import atexit
import config
import logging
from os import path
from tail import Tail
from consumer import Consumer

CONSUMER_MAP = {0: 'yoda', 1: 'obiwan',
                2: 'vader', 3: 'chewbacca'}

def path_tailers(fdict):
    "generate tailer threads with all the initialization done."
    fields = fdict.get('fields', {"type": "default"})
    if "type" not in fields:
        fields["type"] = "default"
    for _path in fdict.get('paths', []):
        annotation = {'name': path.basename(_path)}
        annotation.update(fields)
        tailer = Tail(_path, q=config.LOG_QUEUE,
                      stop_event=config.STOP_EVENT,
                      fields=annotation,
                      interval=config.POLL_INTERVAL)
        yield tailer

def event_loop():
    "this is the main event loop where everything happens"
    # this isn't being called during sys.exit :/
    atexit.register(config.STOP_EVENT.set)
    tailer_threads = []
    # initiate threads to tail from files
    for fdict in config.FILES:
        for tailer in path_tailers(fdict):
            tailer.start()
            tailer_threads.append(tailer)
    # initiate threads to consume logs pushed into queue
    consumer_threads = []
    for i in range(config.NUM_CONSUMERS):
        consumer = Consumer(config.LOG_QUEUE, config.STOP_EVENT,
                            poll_interval=config.POLL_INTERVAL,
                            name=CONSUMER_MAP.get(i % 4))
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
            config.STOP_EVENT.set()
            print
            for consumer in consumer_threads:
                logging.info(
                    '{0.name} sent {0.sent_records} records!'.format(consumer))
            sys.exit('shutting down streamer...')

if __name__ == '__main__':
    event_loop()
