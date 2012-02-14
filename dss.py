import msgpack
import socket
import errno
import select


data = {}


def serve(server_sock):
    epoll = select.epoll()
    epoll.register(server_sock.fileno(), select.EPOLLIN)
    conns = {}
    packer = msgpack.Packer()
    while True:
        for (fd, event) in epoll.poll():
            try:
                if fd == server_sock.fileno():
                    sock, address = server_sock.accept()
                    epoll.register(sock.fileno(), select.EPOLLIN)
                    conns[sock.fileno()] = [sock, msgpack.Unpacker(), '']
                elif event == select.EPOLLIN:
                    conns[fd][1].feed(conns[fd][0].recv(65536))
                    for obj in conns[fd][1]:
                        print obj
                        op, lookup, func, args, kwargs = obj
                        value = data
                        for name in lookup:
                            value = value[name]
                        resp = getattr(value, func)(*args, **kwargs)
                        conns[fd][2] += packer.pack(resp)
                        epoll.modify(fd, select.EPOLLIN | select.EPOLLOUT)
                elif event == select.EPOLLOUT:
                    send_len = conns[fd][0].send(conns[fd][2])
                    conns[fd][2] = conns[fd][2][send_len:]
                    if not conns[fd][2]:
                        epoll.modify(fd, select.EPOLLIN)
            except socket.error as err:
                if err.args[0] in (errno.EAGAIN, errno.EWOULDBLOCK):
                    continue
                if fd in conns:
                    epoll.unregister(fd)
                    del conns[fd]
                raise


def listen_socket(port):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', 12345))
    sock.setblocking(0)
    sock.listen(socket.SOMAXCONN)
    return sock


if __name__ == '__main__':
    serve(listen_socket(12345))
