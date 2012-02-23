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
            chunk = sock.recv(65536)
            if not chunk:
                raise Exception()
            self.feed(chunk)
            for obj in self:
                return obj


def recv_to(sock, current, size):
    while len(current) < size:
        chunk = sock.recv(65536)
        if not chunk:
            raise Exception()
        current += chunk
    return current[:size], current[size:]


def serve_proxy(sock, address):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        buf = ''
        buf2 = ''
        while True:
            packed_data, buf = recv_to(sock, buf, 8)
            length, hash_ = struct.unpack('II', packed_data)
            msg, buf = recv_to(sock, buf, length)

            port = int(BASE_BACKEND + (hash_ % WORKERS))
            backend = socket.create_connection(('127.0.0.1', port))
            try:
                backend.sendall(msg)
                packed_length, buf2 = recv_to(backend, buf2, 4)
                length, = struct.unpack('I', packed_length)
                response, buf2 = recv_to(backend, buf2, length)
                sock.sendall(response)
            finally:
                backend.close()
    except Exception:
        sock.close()


def serve_worker(sock, address):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
    except Exception:
        sock.close()


def serve(sock, target):
    gevent.reinit()
    StreamServer(sock, target).serve_forever()


if __name__ == '__main__':
    workers = [multiprocessing.Process(target=serve, args=(
        socket.tcp_listener(('127.0.0.1', BASE_BACKEND + x), backlog=50,
        reuse_addr=True), serve_worker)) for x in xrange(WORKERS)]
    front_sock = socket.tcp_listener(('', 12345), backlog=50, reuse_addr=True)
    proxies = [multiprocessing.Process(target=serve,
               args=(front_sock, serve_proxy)) for x in xrange(PROXIES)]
    for worker in workers:
        worker.start()
    for proxy in proxies:
        proxy.start()
    for worker in workers:
        worker.join()
    for proxy in proxies:
        proxy.join()
