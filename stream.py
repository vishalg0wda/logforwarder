
import sys
import json
import errno
import struct
import socket
import logging
from central import LOGGER_HOST, LOGGER_PORT, TOKEN

class SocketStreamer(object):
    """This class is responsible for connecting, framing and
       streaming of messages to a remote socket.
    """
    def __new__(cls, host=LOGGER_HOST, port=LOGGER_PORT):
        obj = super(SocketStreamer, cls).__new__(cls)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except socket.error, e:
            # do some more handling
            logging.error(e)
            return None
        else:
            obj._sock = sock
            return obj

    def __init__(self, host=LOGGER_HOST, port=LOGGER_PORT, token=TOKEN):
        self.host = host
        self.port = port
        self.token = token

    def __del__(self):
        logging.debug('socket is being destroyed')
        # TODO: Handle exceptions
        try:
            self._sock.close()
        except:
            pass

    def frame_message(self, msg, **kwargs):
        """return a 2-tuple: (orig message length, framed message)
        """
        payload = kwargs.copy()
        payload['data'] = msg
        payload['token'] = self.token
        payload = json.dumps(payload)
        msg_len = len(payload)
        bin_msg_len = struct.pack('>L', msg_len)
        payload = '{len}{msg}'.format(len=bin_msg_len, msg=payload)
        return (msg_len, payload)

    def send(self, msg, **kwargs):
        """block until entire message is sent. return sent bytes.
           return -1 if error.
        """
        _, payload = self.frame_message(msg, **kwargs)
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
                        logging.error("detected remote disconnect")
                    else:
                        # determine and handle different error
                        logging.error("unpredicted behavior")
                else:
                    logging.error("socket error %s" % e)
                self._sock.close()
                self._sock = None
                return -1
        return sent_bytes

if __name__ == '__main__':
    with SocketStreamer('localhost', 9898) as sock:
        for line in sys.stdin:
            print sock.send(line)
        # sock.send("today was a good day! indeed it was!")
        # sock.send("goodbye world...")
