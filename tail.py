
import os
import time
import logging
import threading
from os import path

OFFSETS_DIR = path.abspath('offsets')
if not path.exists(OFFSETS_DIR):
    os.makedirs(OFFSETS_DIR)

def get_offset_file(fpath):
    "provide path to offset file"
    name = path.basename(fpath)
    fpath = path.join(OFFSETS_DIR, '%s.offset'%name)
    # touch the file
    open(fpath, 'a').close()
    return fpath

class Tail(threading.Thread):
    """This class tails a file continuously and pushes lines into
       a global queue.
    """
    def __init__(self, filepath, q, stop_event, fields, interval):
        threading.Thread.__init__(self)
        self.filepath = filepath
        self.q = q
        self.interval = interval
        self.offset_file = get_offset_file(filepath)
        self.offset = 0
        try:
            self.offset = int(open(self.offset_file).read().strip())
        except ValueError:
            open(self.offset_file, 'w').write('0')
        self.fh = open(self.filepath, 'r')
        self.stop_event = stop_event
        self.fields = fields

    def run(self):
        while not self.stop_event.is_set():
            self.fh.seek(self.offset)
            while True:
                line = self.fh.readline()
                if not line:
                    break
                entry = self.fields.copy()
                entry['data'] = line
                self.q.put(entry)
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
        with open(self.offset_file, 'w') as off_fh:
            off_fh.write(str(self.offset))
            if fsync:
                off_fh.flush()
                os.fsync(off_fh.fileno())
