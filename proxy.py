from threading import Thread
from package import receive_package, Package, Header, PackageDataType, send_package
from socket import socket as Socket
from app.utils import Logger, generate_client_uuid, read_csv_int, get_obj_hash, int_list_to_bytes
from typing import List, Tuple
import time

log = Logger()


class Client:
    def __init__(self, uuid: str, socket: Socket):
        self.uuid = uuid
        self.socket = socket


class RowDataDesc:
    def __init__(self, _slice: Tuple[int, int], _hash: str, file_name: str = None):
        self.slice = _slice
        self.hash = _hash
        self.filename = file_name
        self.start_time = time.time()
        self.end_time = None
        self.finished = False


class Proxy:
    def __init__(self, socket: Socket):
        self.socket: Socket = socket
        self.client_list: List[Client] = []
        self.assigned_data: List[RowDataDesc] = []

        while True:
            # establish connect to the client
            sock, addr = socket.accept()
            client = Client(uuid=generate_client_uuid(), socket=sock)
            self.client_list.append(client)
            log.info(f"Node {addr} connected, uuid: {client.uuid}")
            log.debug(f"Total {len(self.client_list)} node(s)")

            self.send_raw_data(client)

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
        msg = "transmit row data"
        package.generate_default_header(msg)
        send_package(package, client.socket)

        log.debug(
            f"-> Sending row data[{_slice[0]}:{_slice[1]}] to client \"{client.uuid}\" "
            f"| message: \"{msg}\" "
            f"| hash: {package.get_header().get_package_hashcode().hex()}")

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
