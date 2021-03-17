from proxy import Proxy
import socket

if __name__ == '__main__':
    s = socket.socket()
    host = socket.gethostname()
    port = 23456
    s.bind((host, port))
    s.listen(5)

    while True:
        sock, addr = s.accept()
        print(f"node {addr} connected")
        proxy = Proxy(socket=sock)
