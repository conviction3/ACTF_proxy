from threading import Thread
from package import receive_package
from socket import socket as Socket


class Proxy:
    def __init__(self, socket: Socket):
        self.socket: Socket = socket
        # while True:
        #     # establish connect to the client
        #     sock, addr = s.accept()
        #     print(f"node {addr} connected")
        self.start_receive_thread()

    def start_receive_thread(self):
        def temp():
            while True:
                header = receive_package(self.socket)
                print(header.get_message(parse=True))
        t = Thread(target=temp)
        t.start()
        t.join()
