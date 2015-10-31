
import sys
import errno
import struct
import socket
import logging

class SocketStreamer(object):
    """This class is responsible for connecting, framing and
       streaming of messages to a remote socket.
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = None

    def __enter__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._sock.connect((self.host, self.port))
        except socket.error, e:
            # do some more handling
            return -1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # TODO: Handle exceptions when streaming within context 
        self._sock.close()
        self._sock = None

    def frame_message(self, msg):
        "return a 2-tuple: (orig message length, framed message)"
        msg_len = len(msg)
        bin_msg_len = struct.pack('>L', msg_len)
        payload = '{len}{msg}'.format(len=bin_msg_len, msg=msg)
        return (msg_len, payload)

    def send(self, msg):
        "block until entire message is sent. return sent bytes."
        _, payload = self.frame_message(msg)
        sent_bytes = 0
        # return self._sock.send(msg)
        while sent_bytes < len(payload):
            try:
                sent_bytes += self._sock.send(payload[sent_bytes:])
            except socket.error, e:
                if isinstance(e.args, tuple):
                    logging.error("errno is %d" % e[0])
                    if e[0] == errno.EPIPE:
                        # remote peer disconnected
                        logging.error("Detected remote disconnect")
                    else:
                        # determine and handle different error
                        logging.error("unpredicted behavior")
                else:
                    logging.error("socket error %s" % e)
                self._sock.close()
                return -1
        return sent_bytes

if __name__ == '__main__':
    with SocketStreamer('localhost', 9898) as sock:
        # with open('tail.py') as fh:
        # sock.send('hahaha')
        for line in sys.stdin:
            print sock.send(line)
        # sock.send("today was a good day! indeed it was!")
        # sock.send("goodbye world...")
