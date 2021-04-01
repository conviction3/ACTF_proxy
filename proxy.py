from threading import Thread
from package import receive_package, Package, Header, PackageDataType, send_package, send_message
from socket import socket as Socket
from app.utils import Logger, generate_client_uuid, read_csv_int, get_obj_hash, int_list_to_bytes
from typing import List, Tuple
import queue
import time

log = Logger()


class Client:
    def __init__(self, uuid: str, socket: Socket):
        self.uuid = uuid
        self.socket = socket
        self.thread = None
        self.stop = False


class RowDataDesc:
    def __init__(self, _slice: Tuple[int, int], _hash: str, file_name: str = None):
        self.slice = _slice
        self.hash = _hash
        self.filename = file_name
        self.start_time = time.time()
        self.end_time = None
        self.finished = False


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
    ORDERED_PACKAGE_NUM = 8
    MAX_BUFFER = 10

    def __init__(self, socket: Socket):
        self.socket: Socket = socket
        self.client_list: List[Client] = []
        self.assigned_data: List[RowDataDesc] = []
        """
            The package received from clients will be placed into this buffer immediately, then 
        there will be another thread to handle the buffer to reorder the package by package seq.
        If the this buffer is full, then should limit the sending speed of clients.
            Actually, the received_buffer and the ordered_packages is a producer-consumer model.
        The received buffer is a critical resources, the thread which receives packages from 
        clients is producers, and the thread which retrieves data from the buffer is a consumer.
            The item in queue is class SeqData
        """
        self.received_buffer: queue.Queue = queue.Queue(maxsize=Proxy.MAX_BUFFER)
        """
        #     List of packages. When the proxy had received packages from client, it will be placed the
        # package into the specific position of the list by the package seq.
            List of ordered packages, the size of this list is grater than received_buffer a lot.
        This list could be stored in files or database orderly, then combine to one package or do some
        calculation before sending to server.
            But for now, it's just stored in memory.
        """
        self.ordered_packages: List[Package] = []
        self.__init_ordered_packages()

        self.start_consume()

        while True:
            # establish connect to the client
            sock, addr = socket.accept()
            client = Client(uuid=generate_client_uuid(), socket=sock)
            self.client_list.append(client)
            log.info(f"Node {addr} connected, uuid: {client.uuid}")
            log.debug(f"Total {len(self.client_list)} node(s)")

            # self.send_raw_data(client)
            self.start_receive_thread(client)

    def __init_ordered_packages(self):
        self.ordered_packages = [None for i in range(Proxy.ORDERED_PACKAGE_NUM)]

    def __get_data_slice(self) -> Tuple[int, int]:
        """
            Supposed the csv file should be split into different data to send to
        different clients. There must be a method to deal with the slice of the
        data.
            The slice should be determined by the number of clients、the data has
        been assigned and has been finished by client, and the data that has been
        assigned but not been finished ( client offline or timeout ).
        :return:
        """
        if len(self.assigned_data) == 0:
            return 0, 10

        _max_slice_left = 0
        for row_data in self.assigned_data:
            # todo: reassigned the failed data
            # find the max slice
            _max_slice_left = max(row_data.slice[0], _max_slice_left)
        return _max_slice_left, _max_slice_left + 10

    def send_raw_data(self, client: Client):
        file_name = "./data/distribution_add_data_1.csv"
        # load data from file
        _slice = self.__get_data_slice()
        slice_data = read_csv_int(file_name, _slice[0], _slice[1])
        row_data_desc = RowDataDesc(_slice=_slice, _hash=get_obj_hash(slice_data), file_name=file_name)

        package = Package(payload=int_list_to_bytes(slice_data), data_type=PackageDataType.INT)
        msg = "transmit raw data"
        package.generate_default_header(msg)
        send_package(package, client.socket)

        log.debug(
            f"-> Sending raw data[{_slice[0]}:{_slice[1]}] to client \"{client.uuid}\" | "
            + package.get_desc())

    def start_consume(self):
        def temp():
            while True:
                buffer_length = self.received_buffer.qsize()
                if buffer_length > 0:
                    for i in range(buffer_length):
                        seq_data: SeqData = self.received_buffer.get()
                        log.debug(f"consume data {seq_data.seq}")
                        self.print_buffer()
                        time.sleep(1)

        t = Thread(target=temp)
        t.start()

    def start_receive_thread(self, client: Client):

        def temp():
            while True:
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
                    # todo: may there should be a list length in header
                    payload: List[int] = package.get_payload(parse=True)
                    payload_length = len(payload)

                    buffer_length = self.received_buffer.qsize()
                    # ---> discard the package
                    if payload_length + buffer_length > Proxy.MAX_BUFFER:
                        log.warning(f"The buffer size is {buffer_length} of {Proxy.MAX_BUFFER} now, "
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

                    # unpack the package, then add it to queue

                    # if self.check_ordered_packages(_log=False):
                    #     break
                    # self.ordered_packages[seq] = package
                    # # double check
                    # if self.check_ordered_packages(_log=True):
                    #     break
            """
                If program running to this, meaning the ordered_packages has been full-filled.
            """
            log.info("The ordered packages has been full-filled, job is done.")
            self.finish_job()

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
        for client in self.client_list:
            client.stop = True
            client.socket.close()
            log.info(f"Close connection of {client.uuid}")
        self.client_list.clear()
        # todo: send the combined packages
        self.__init_ordered_packages()

    def check_ordered_packages(self, _log: False) -> bool:
        """
            Check the status of ordered package list
        :param _log: print the status or not
        :return: False: if the list is not full
        """
        none_count = 0
        for item in self.ordered_packages:
            if item is None and not _log:
                return False
            if item is None and _log:
                none_count += 1
        if _log:
            log.debug(f"{len(self.ordered_packages) - none_count} of {len(self.ordered_packages)}"
                      f" ordered packages is filled")
        if none_count == 0:
            return True
        else:
            return False

    # def start_receive_thread(self, client: Client):
    #     def temp():
    #         while True:
    #             result = receive_package(client.socket)
    #             if isinstance(result, Header):
    #                 header = result
    #                 log.debug(f"<- message: \"{header.get_message()}\" "
    #                           f"| hash: {header.get_package_hashcode()}")
    #             else:
    #                 package = result
    #                 header = result.get_header()
    #                 log.debug("<- " + package.get_desc())
    #                 if header.has_ack():
    #                     log.info(
    #                         f"Slice {header.get_ack(parse=True)} done by client {client.uuid}, "
    #                         f"result = {package.get_payload(parse=True)}")
    #
    #     t = Thread(target=temp)
    #     t.start()
