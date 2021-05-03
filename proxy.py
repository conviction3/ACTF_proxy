from threading import Thread, Lock
import socket
from package import receive_package, Package, Header, PackageDataType, send_package, send_message
from socket import socket as Socket
from app.utils import Logger, generate_client_uuid, int_list_to_bytes
from typing import List, Tuple
import queue
import sqlite3

log = Logger()


class Client:
    def __init__(self, uuid: str, socket: Socket):
        self.uuid = uuid
        self.socket = socket


class SeqData:
    def __init__(self, seq: int, data):
        self.seq = seq
        self.data = data

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"({self.seq},{self.data})"


class Proxy:
    """
        Suppose there're would be 8 ordered packages, namely when proxy had received 8 packages which
    seq from 0 to 8, then the job is done, the ordered packages will be combined into a one single huger
    package then send to the sever.
    """

    # TARGET_SEQ_DATA_NUM = 10
    # MAX_BUFFER = 10

    def __init__(self, socket: Socket, target_seq_data_num: int, max_buffer: int = 10):
        self.socket: Socket = socket
        self.client_list: List[Client] = []

        self.consuming_count: int = 0

        self.job_finished_flag = False
        self.job_finished_flag_lock = Lock()

        self.target_seq_data_num: int = target_seq_data_num

        """
            The package received from clients will be placed into this buffer immediately, then 
        there will be another thread to handle the buffer to reorder the package by package seq.
        If the this buffer is full, then should limit the sending speed of clients.
            Actually, the received_buffer and the ordered_packages is a producer-consumer model.
        The received buffer is a shared resources, the thread which receives packages from clients
        is producers, and the thread which retrieves data from the buffer is a consumer.
            The item in queue is class SeqData
        """
        self.max_buffer = max_buffer
        self.received_buffer: queue.Queue = queue.Queue(maxsize=self.max_buffer)
        """
            List of ordered packages, the size of this list is grater than received_buffer a lot.
        This list could be stored in files or database orderly, then combine to one package or do some
        calculation before sending to server.
            But for now, it's just stored in memory.
        """
        self.ordered_seq_data: List[SeqData] = []
        self.__init_ordered_packages()

        self.start_consume()

        while True:
            # establish connect to the client
            sock, addr = socket.accept()
            client = Client(uuid=generate_client_uuid(), socket=sock)
            self.client_list.append(client)
            log.info(f"Node {addr} connected, uuid: {client.uuid}")
            log.debug(f"Total {len(self.client_list)} node(s)")

            self.start_receive_thread(client)

    def __init_ordered_packages(self):
        self.ordered_seq_data = [None for i in range(self.target_seq_data_num)]

    def start_consume(self):
        """
        :return:
        """

        def temp():
            conn = sqlite3.connect('./data/result_data.db')
            c = conn.cursor()
            c.execute("""
                DROP TABLE seq_data
            """)
            c.execute(
                '''CREATE TABLE seq_data
                    (   seq     INTEGER PRIMARY KEY NOT NULL,
                        number  INT             NOT NULL
                    );
                '''
            )
            conn.commit()

            while True:
                if self.job_finished_flag:
                    self.finish_job()
                    break
                buffer_length = self.received_buffer.qsize()
                if not self.received_buffer.empty():
                    for i in range(buffer_length):
                        seq_data: SeqData = self.received_buffer.get()
                        log.debug(f"consume seq data {seq_data}")
                        self.ordered_seq_data[seq_data.seq] = seq_data
                        self.consuming_count += 1
                        # time.sleep(1)
                        c.execute(f'''
                            INSERT INTO seq_data(seq,number)
                            VALUES
                            (
                                {seq_data.seq},{seq_data.data}
                            )
                        ''')
                        conn.commit()
                        self.print_buffer()

                        if self.consuming_count == self.target_seq_data_num:
                            self.job_finished_flag_lock.acquire()
                            self.job_finished_flag = True
                            self.job_finished_flag_lock.release()
                            break

        t = Thread(target=temp)
        t.start()

    def start_receive_thread(self, client: Client):

        def temp():
            while True:
                if self.job_finished_flag:
                    break
                try:
                    result = receive_package(client.socket)
                    if isinstance(result, Header):
                        header = result
                        log.debug(f"<- message: \"{header.get_message()}\" "
                                  f"| hash: {header.get_package_hashcode()}")
                    else:
                        package = result
                        header = result.get_header()
                        log.debug(f"[{client.uuid}] -> " + package.get_desc())
                        # place the package into ordered list by the package seq
                        if not header.has_package_seq():
                            continue
                        seq = header.get_package_seq(parse=True)
                        # suppose the payload is list of integer, ordered
                        payload: List[int] = package.get_payload(parse=True)
                        payload_length = len(payload)

                        buffer_length = self.received_buffer.qsize()
                        # ---> discard the package
                        if payload_length + buffer_length > self.max_buffer:
                            log.warning(f"The buffer size is {buffer_length} of {self.max_buffer} now, "
                                        f"but received payload size is {payload_length}, "
                                        f"the package will be discarded!")
                            send_message(message=Header.MSG_PACKAGE_DISCARD, ack=header.get_package_hashcode(),
                                         sock=client.socket)
                            continue
                        # <--- discard the package
                        # ---> parse and handle the package
                        for i in range(len(payload)):
                            self.received_buffer.put(SeqData(seq=seq + i, data=payload[i]))
                        send_message(message=Header.MSG_ACKNOWLEDGED, ack=header.get_package_hashcode(),
                                     sock=client.socket)
                        self.print_buffer()
                    # <--- parse and handle the package
                except (ConnectionAbortedError, ConnectionResetError):
                    break

        t = Thread(target=temp)
        client.thread = t
        t.start()

    def print_buffer(self):
        log.debug(f"the buffer data is:{list(self.received_buffer.queue)}")

    def finish_job(self):
        """
            Close socket, delete client item.
        :return:
        """
        self.job_finished_flag_lock.acquire()
        log.info("The ordered packages has been full-filled, job is done.")
        for client in self.client_list:
            client.socket.close()
            log.info(f"Close connection of {client.uuid}")
        self.client_list.clear()
        self.job_finished_flag_lock.release()

        s = socket.socket()
        server_host = socket.gethostname()
        server_port = 23457
        s.connect((server_host, server_port))

        """
            Add all seq data, combine to one package, send to server
        """
        temp_list = []
        for seq_data in self.ordered_seq_data:
            temp_list.append(seq_data.data)
            # result += seq_data.data
        # temp_list = [result]
        package = Package(payload=int_list_to_bytes(temp_list), data_type=PackageDataType.INT)
        package.generate_default_header()
        package.get_header().set_message("Ordered min value group")
        send_package(package, s)
