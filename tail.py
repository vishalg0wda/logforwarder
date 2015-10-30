
import os
import time
import logging
import threading
from os import path
from central import MAX_BUFFER_SIZE

def touch(fpath):
    open(fpath, 'a').close()

class Tail(threading.Thread):
    """This class tails a file continuously and pushes lines into
       a global queue.
    """
    def __init__(self, filepath, q, stop_event, interval=1, out_file=None, name=None):
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
        if name is not None:
            self.name = name

    def run(self):
        while not self.stop_event.is_set():
            self.fh.seek(self.offset)
            while self.q.qsize() < MAX_BUFFER_SIZE:
                line = self.fh.readline()
                if not line:
                    break
                self.q.put(line)
                logging.debug('added to queue, size(%d)'%self.q.qsize())
                self.offset = self.fh.tell()
                self.flush_offset()
            else:
                logging.warn('max buffer reached!')
            time.sleep(self.interval)
        else:
            self.flush_offset(fsync=True)
            self.fh.close()

    def flush_offset(self, fsync=False):
        with open(self.out_file, 'w') as out_fh:
            out_fh.write(str(self.offset))
            if fsync:
                out_fh.flush()
                os.fsync(out_fh.fileno())
