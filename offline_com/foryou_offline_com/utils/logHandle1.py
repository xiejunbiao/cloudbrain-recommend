import os
import logging
import logging.handlers
import traceback
import _pickle as cPickle
import struct
import socketserver as SocketServer
from multiprocessing import Process

class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
    def handle(self):
         while True:
             try:
                 chunk = self.connection.recv(4)
                 if len(chunk) < 4:
                     break
                 slen = struct.unpack(">L", chunk)[0]
                 chunk = self.connection.recv(slen)
                 while len(chunk) < slen:
                     chunk = chunk + self.connection.recv(slen - len(chunk))
                 obj = self.unpickle(chunk)
                 record = logging.makeLogRecord(obj)
                 self.handle_log_record(record)

             except:
                 break

    @classmethod
    def unpickle(cls, data):
         return cPickle.loads(data)
    def handle_log_record(self, record):
         if self.server.logname is not None:
             name = self.server.logname
         else:
             name = record.name
         logger = logging.getLogger(name)
         logger.handle(record)


class LogRecordSocketReceiver(SocketServer.ThreadingTCPServer):
     allow_reuse_address = 1

     def __init__(self, host='localhost', port=logging.handlers.DEFAULT_TCP_LOGGING_PORT, handler=LogRecordStreamHandler):
         SocketServer.ThreadingTCPServer.__init__(self, (host, port), handler)
         self.abort = 0
         self.timeout = 1
         self.logname = None

     def serve_until_stopped(self):
         import select
         abort = 0
         while not abort:
             rd, wr, ex = select.select([self.socket.fileno()], [], [], self.timeout)
             if rd:
                 self.handle_request()
             abort = self.abort


def _log_listener_process(log_format, log_time_format, log_file):
     log_file = os.path.realpath(log_file)
     logging.basicConfig(level=logging.DEBUG, format=log_format, datefmt=log_time_format, filename=log_file, filemode='a+')

     # Console log
     console = logging.StreamHandler()
     console.setLevel(logging.INFO)
     console.setFormatter(logging.Formatter(fmt=log_format, datefmt=log_time_format))
     logging.getLogger().addHandler(console)

     tcp_server = LogRecordSocketReceiver()

     logging.debug('Log listener process started ...')
     tcp_server.serve_until_stopped()