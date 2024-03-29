import multiprocessing
import struct
import traceback
import collections
import time
import select
import socket
import errno

import msgpack

import atomic_t


data = {}
WORKERS = 8
PROXIES = 8

BASE_BACKEND = 22222


class Connection(object):
    def __init__(self, sock):
        self.sock = sock
        self.outbuf = ''
        self.inbuf = ''


class StreamServer(object):
    def __init__(self, sock, connection_class):
        self.sock = sock
        self.connection_class = connection_class

    def __call__(self):
        epoll = select.epoll()
        epoll.register(self.sock.fileno(), select.EPOLLIN)
        conns = {}
        while True:
            for (fd, event) in epoll.poll():
                try:
                    conn = conns.get(fd, None)
                    if fd == self.sock.fileno():
                        sock, address = self.sock.accept()
                        sock.setsockopt(socket.IPPROTO_TCP,
                                        socket.TCP_NODELAY, 1)
                        epoll.register(sock.fileno(), select.EPOLLIN)
                        conns[sock.fileno()] = self.connection_class(sock)
                    elif event & select.EPOLLERR:
                        epoll.unregister(fd)
                        del conns[fd]
                        continue
                    elif event & select.EPOLLIN:
                        chunk = conn.sock.recv(65536)
                        if not chunk:
                            epoll.unregister(fd)
                            del conns[fd]
                            continue
                        conns[fd].feed(chunk)
                        if conns[fd].outbuf:
                            epoll.modify(fd, select.EPOLLOUT)
                    elif event & select.EPOLLOUT:
                        send_len = conn.sock.send(conn.outbuf)
                        conn.outbuf = conn.outbuf[send_len:]
                        if not conn.outbuf:
                            epoll.modify(fd, select.EPOLLIN)
                    else:
                        print fd, event
                except socket.error as err:
                    if err.args[0] in (errno.EAGAIN, errno.EWOULDBLOCK):
                        continue
                    if fd in conns:
                        epoll.unregister(fd)
                        del conns[fd]
                    raise


backend_connection_pool = collections.defaultdict(lambda: [])


class ProxyConnection(Connection):
    def __init__(self, *args, **kwargs):
        super(ProxyConnection, self).__init__(*args, **kwargs)
        self.response = msgpack.packb(True)
        self.unpacker = msgpack.Unpacker()

    def feed(self, chunk):
        self.unpacker.feed(chunk)
        for obj in self.unpacker:
            self.outbuf += self.response
            progress_counter.inc()


def progress_report():
    start = time.time()
    last = 0
    while True:
        current = progress_counter.value()
        print (current - last), "per second"
        last = current
        time.sleep(1)


def listen_socket(port):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.bind(('', 12345))
    sock.setblocking(0)
    sock.listen(socket.SOMAXCONN)
    return sock


if __name__ == '__main__':
    sock = listen_socket(12345)
    progress_counter = atomic_t.AtomicT()
    proxies = [multiprocessing.Process(target=StreamServer(sock, ProxyConnection))
               for x in xrange(PROXIES)]
    progress = multiprocessing.Process(target=progress_report)
    progress.start()
    for proxy in proxies:
        proxy.start()
    for proxy in proxies:
        proxy.join()
