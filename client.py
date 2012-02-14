import msgpack
import socket


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
        self.packer = msgpack.Packer()
        self.unpacker = msgpack.Unpacker()

    def call(self, lookup_list, func, args, kwargs):
        request = ('C', lookup_list, func, args, kwargs)
        self.sock.sendall(self.packer.pack(request))
        while True:
            self.unpacker.feed(self.sock.recv(65536))
            for response in self.unpacker:
                return response

    def proxy_object(self, key):
        return ProxyObject(self, [key])

    def create_object(self, key, value):
        return self.call([], '__setitem__', (key, value), {})

x = Client('localhost', 12345)
x.create_object('something', {})
y = x.proxy_object('something')
y['blah'] = [1, 2, 3]
print 2 in y['blah']

