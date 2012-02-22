from gevent.server import StreamServer
from gevent import socket
import gevent
import msgpack
import multiprocessing
import struct


data = {}
WORKERS = 8
PROXIES = 8

BASE_BACKEND = 22222

class StreamUnpacker(msgpack.Unpacker):
    def readnext(self, sock):
        while True:
            self.feed(sock.recv(65536))
            for obj in self:
                return obj


def serve_proxy(sock, address):
    buf = ''
    buf2 = ''
    while True:
        while len(buf) < 8:
            buf += sock.recv(65536)
        length, hash_ = struct.unpack('II', buf[:8])
        buf = buf[8:]
        while len(buf) < length:
            buf += sock.recv(65536)
        port = int(BASE_BACKEND + (hash_ % WORKERS))
        backend = socket.create_connection(('127.0.0.1', port))
        try:
            backend.sendall(buf[:length])

            while len(buf2) < 4:
                buf2 += sock.recv(65536)
            length, = struct.unpack('I', buf2[:4])
            while len(buf2) < length + 4:
                buf2 += sock.recv(65536)
            sock.sendall(buf2)
        finally:
            backend.close()


def serve_worker(sock, address):
    packer = msgpack.Packer()
    unpacker = StreamUnpacker()
    while True:
        obj = unpacker.readnext(sock)
        op, lookup, func, args, kwargs = obj
        value = data
        for name in lookup:
            value = value[name]
        resp = getattr(value, func)(*args, **kwargs)
        resp_packed = packer.pack(resp)
        resp_enveloped = struct.pack('I', len(resp_packed)) + resp_packed
        sock.sendall(resp_enveloped)


def serve(sock, target):
    gevent.reinit()
    StreamServer(sock, target).serve_forever()


if __name__ == '__main__':
    workers = [multiprocessing.Process(target=serve, args=(
        socket.tcp_listener(('127.0.0.1', BASE_BACKEND + x), backlog=50, reuse_addr=True), serve_worker)
        ) for x in xrange(WORKERS)]
    front_sock = socket.tcp_listener(('', 12345), backlog=50, reuse_addr=True)
    proxies = [multiprocessing.Process(target=serve, args=(front_sock, serve_worker)) for x in xrange(PROXIES)]
    for worker in workers:
        worker.start()
    for proxy in proxies:
        proxy.start()
    for worker in workers:
        worker.join()
    for proxy in proxies:
        proxy.join()
