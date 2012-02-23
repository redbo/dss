import socket
import struct
import multiprocessing
import random
import time

import msgpack

WORKERS = 16


class ProxyObject(object):
    def __init__(self, client, lookup_list):
        self.client = client
        self.lookup_list = lookup_list

    def __getitem__(self, key):
        return ProxyObject(self.client, self.lookup_list + [key])

    def __contains__(self, key):
        return self.client.call(self.lookup_list, '__contains__', (key,), {})

    def __setitem__(self, key, value):
        return self.client.call(self.lookup_list, '__setitem__',
                    (key, value), {})


class Client(object):
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.packer = msgpack.Packer()
        self.unpacker = msgpack.Unpacker()

    def call(self, lookup_list, func, args, kwargs):
        if lookup_list:
            hash_ = abs(hash(lookup_list[0])) % 2**32
        else:
            hash_ = abs(hash(args[0])) % 2**32
        request = ('C', lookup_list, func, args, kwargs)
        packed = self.packer.pack(request)
        enveloped = struct.pack('II', len(packed), hash_) + packed
        self.sock.sendall(enveloped)
        while True:
            self.unpacker.feed(self.sock.recv(65536))
            for response in self.unpacker:
                return response

    def __getitem__(self, key):
        return ProxyObject(self, [key])

    def __setitem__(self, key, value):
        return self.call([], '__setitem__', (key, value), {})


def work_client():
    start = time.time()
    x = Client('localhost', 12345)
    while time.time() - start < 60:
        key = 'something%d' % random.randint(0, 5000)
        x[key]['blah'] = [1, 2, 3]
        assert 2 in x[key]['blah']


if __name__ == '__main__':
    cli = Client('localhost', 12345)
    for x in xrange(5001):
        cli['something%d' % x] = {}
    workers = [multiprocessing.Process(target=work_client, args=()) for x in xrange(WORKERS)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
