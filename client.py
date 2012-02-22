import msgpack
import socket
import struct


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
        print "Sending request", len(packed), hash_, request
        enveloped = struct.pack('II', len(packed), hash_) + packed
        self.sock.sendall(enveloped)
        while True:
            self.unpacker.feed(self.sock.recv(65536))
            for response in self.unpacker:
                print "Returning", response
                return response

    def __getitem__(self, key):
        return ProxyObject(self, [key])

    def __setitem__(self, key, value):
        return self.call([], '__setitem__', (key, value), {})

x = Client('localhost', 12345)
x['something'] = {}
x['something']['blah'] = [1, 2, 3]
print 2 in x['something']['blah']

