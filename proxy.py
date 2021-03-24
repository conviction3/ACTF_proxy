from threading import Thread
from package import receive_package, Package, Header
from socket import socket as Socket
from app.utils import Logger
log=Logger()


class Proxy:
    def __init__(self, socket: Socket):
        self.socket: Socket = socket
        # while True:
        #     # establish connect to the client
        #     sock, addr = s.accept()
        #     print(f"node {addr} connected")
        # self.start_receive_thread()

    def start_receive_thread(self):
        def temp():
            while True:
                result = receive_package(self.socket)
                if isinstance(result, Header):
                    header = result
                # elif isinstance(result, Package):
                else:
                    header = result.get_header()
                log.debug(f"hash: {header.get_package_hashcode()}\t message:{header.get_message(parse=True)}")

                print(header.get_message(parse=True))

        t = Thread(target=temp)
        t.start()
        t.join()
