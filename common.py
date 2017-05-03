# -*- coding:utf-8 -*-
import threading
import traceback
import socket

import select
import string
import random
import time
import os
import logging
import logging.config

import cust_logging

BUFFER_SIZE = 9216
LOGGER = 'logger'

def connect_to(addr, port):
    target = (addr, int(port))
    try:
        # 建立socket对象
        sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 建立连接
        sck.settimeout(10)
        sck.connect(target)
        return sck
    except Exception, e:
        logger = logging.getLogger("logger")
        logger.error(''.join(('connected to ', str(target), "failed\r\n",traceback.format_exc())) )
        pass
    return None


def listen_on(port, ip='0.0.0.0'):
    try:
        tcpsock = socket.socket()
        tcpsock.bind((ip, int(port)))
        tcpsock.listen(500)
        set_sock_buff_size(tcpsock)
        return tcpsock
    except Exception, e:
        logger = logging.getLogger("logger")
        logger.error("listen on port %s fail!"%port)
        logger.error(traceback.format_exc())
        pass
    return None

def recv( sock, size=BUFFER_SIZE):
    sock.setblocking(0)
    data = sock.recv(size)
    sock.setblocking(1)
    return data


def safe_close_socket(sock):
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except socket.error,e:
        pass
    except Exception, e:
        logger = logging.getLogger("logger")
        logger.error(traceback.format_exc())
    try:
        sock.close()
    except:
        logger = logging.getLogger("logger")
        logger.error(traceback.format_exc())


def set_sock_buff_size(sock):
    try:
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
    except Exception, e:
        logger = logging.getLogger("logger")
        logger.error(traceback.format_exc())

def parse_line(line):
    line = line.strip() + " "
    all = line.split(' ')
    return all[0], all[1:]


def key_gen():
    KEY_LEN = 20
    keylist = [random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') for i in range(KEY_LEN)]
    return "".join(keylist)

def build_filename(name):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), name)

def init_logger(name):
    log_file = build_filename("logs/%s.log"%name)

    conf = {'version': 1,
            'disable_existing_loggers': True,
            'incremental': False,
            'formatters': {'myformat1': {'class': 'logging.Formatter',
                                         'format': '|%(asctime)s|%(filename)s|%(lineno)d|%(levelname)s|%(message)s',
                                         'datefmt': '%Y-%m-%d %H:%M:%S'}
                           },
            'handlers': {
                # 'console': {
                #     'class': 'logging.StreamHandler',
                #     'level': 'INFO',
                #     'formatter': 'myformat1',
                # },
                'fileHandler': {
                    'class': 'cust_logging.CustTimedRotatingFileHandler',
                    'level': logging.INFO,
                    'formatter': 'myformat1',
                    'filename': log_file,
                    'when': 'h',
                    'interval': 8,
                    'backupCount': 12
                }
            },
            'loggers': {
                'logger': {'handlers': ['fileHandler'],
                           'level': 'INFO'}
            }
        }

    logging.config.dictConfig(conf)
    return logging.getLogger(LOGGER)


class TcpForwarding(threading.Thread):
    def __init__(self, name):
        super(TcpForwarding, self).__init__(name="TcpForwardingThread %s"%name)
        self.sck_side_A = None
        self.sck_side_B = None
        self.forwarding = True

    def send_all(self, sock, data):
        to_send = len(data)
        sent = 0
        while sent < to_send:
            sent += sock.send(data[sent:])

    def run(self):
        if self.sck_side_A is not None and self.sck_side_B is not None:
            logger = logging.getLogger("logger")
            logger.info("%s start"%self.getName())
            set_sock_buff_size(self.sck_side_A)
            set_sock_buff_size(self.sck_side_B)
            scks = (self.sck_side_A, self.sck_side_B)
            while self.forwarding:
                try:
                    readable,writeable,exceptional = select.select(scks,[],scks, 1)
                    for s in readable:
                        data = recv(s, BUFFER_SIZE)
                        if len(data):
                            if s == self.sck_side_A:
                                #self.sck_side_B.sendall(data)
                                self.send_all(self.sck_side_B, data)
                            else:
                                #self.sck_side_A.sendall(data)
                                self.send_all(self.sck_side_A, data)
                        else:
                            # remote closed
                            self.forwarding = False
                            break
                    for s in exceptional:
                        socket.getpeername()
                        peer_addr, peer_port = s.getpeername()
                        local_addr, local_port = s.getsockname(self)
                        logger.error("socket in exceptional %s:%d->%s:%d"%(local_addr, local_port, peer_addr, peer_port))
                        self.forwarding = False
                        break
                except socket.error,e:
                    logger.error(traceback.format_exc())
                    self.forwarding = False
                except Exception, e:
                    logger.error(traceback.format_exc())
                    self.forwarding = False
        if self.sck_side_A is not None:
            safe_close_socket(self.sck_side_A)
        if self.sck_side_B is not None:
            safe_close_socket(self.sck_side_B)
        logger.info("%s end"%self.getName())