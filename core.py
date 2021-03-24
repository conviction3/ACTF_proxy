import socket
from app.utils import *
from threading import Thread, Lock, Timer
from flask import Flask
from web.apis import Api
from flask_cors import CORS

# class Buffer:
#     def __init__(self, buffer_size=1024):
#         # size of byte
#         self.buffer_size = buffer_size
#         self.buffer_area = []

bytes_count = 0
lock = Lock()


class Node:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.thread = None

    def run(self):
        def receive_data():
            global bytes_count
            while True:
                data = self.sock.recv(1024)
                print(f"getting data from {self.addr}, the data is {data}")
                lock.acquire()
                bytes_count += len(data)
                lock.release()

        self.thread = Thread(target=receive_data)
        self.thread.start()

    def stop_threading(self):
        # todo: stop
        pass


client_num = 2


def get_bytes_count() -> int:
    # lock.acquire()
    # temp = bytes_count
    # lock.release()
    # return temp
    return bytes_count


def temp():
    global bytes_count
    print(f"speed: {bytes_count} B/s")
    bytes_count = 0

    Timer(0.6, temp).start()


def temp2():
    def target():
        # 启动flask
        app = Flask(__name__)
        CORS(app, supports_credentials=True)
        api = Api(app)
        app.run()

    Thread(target=target).start()


def main():
    # create socket instance
    s = socket.socket()
    # local host name
    host = socket.gethostname()
    # set local port
    port = 23456
    # bind local port to local host
    s.bind((host, port))

    s.listen(5)
    print("waiting for client")

    client_count = 0
    data_list = []

    # 启动定时器统计流量
    # temp()

    # 启动flask
    # temp2()

    while True:
        # establish connect to the client
        sock, addr = s.accept()
        print(f"node {addr} connected")

        # starting a new thread to handle the node's data
        node = Node(sock, addr)
        node.run()
