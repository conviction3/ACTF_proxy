import logging.handlers
import uuid
import csv
from typing import List
import hashlib
from functools import reduce


class Logger(logging.Logger):
    def __init__(self, name: str = None, filename=None):
        super().__init__(name)
        if filename is None:
            filename = './logs/proxy.log'
        self.filename = filename

        """
            File log handler, for writing log to file. One log file per day,
        only keep 30 day's logs.
        """
        fh = logging.handlers.TimedRotatingFileHandler(self.filename, 'D', 1, 30)
        fh.suffix = "%Y%m%d-%H%M.log"
        fh.setLevel(logging.DEBUG)
        """
            Console log handler.
        """
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            '[%(asctime)s] - %(filename)s [Line:%(lineno)d] - [%(levelname)5s]-[thread:%(thread)s]-[process:%(process)s] : %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.addHandler(fh)
        self.addHandler(ch)


def bytes2int(b: bytes) -> int:
    return int(b)


def string2bytes(string: str) -> bytes:
    return bytes(string, encoding="utf-8")


def read_csv_int(file_name: str, from_index: int, to_index: int) -> List[int]:
    """
    read data from csv file
    :param file_name:   file name will be read
    :param from_index: start index, start from 0, include from_index
    :param to_index: end index,start from 0, exclude to_index
    :return: list of int
    """
    result_list = []
    with open(file_name, 'r') as f:
        reader = csv.reader(f)
        next(f)
        for item in reader:
            result_list.append(item[0])
    return [int(t) for t in result_list[from_index:to_index]]


def generate_client_uuid() -> str:
    return str(uuid.uuid4())[:8]


def get_obj_hash(obj) -> str:
    return hashlib.md5(str(obj).encode()).hexdigest()


def int2bytes(val: int) -> bytes:
    """
        Used to transmit integer type in package payload,
    the size of int must be 8 bytes, signed.
    """
    return val.to_bytes(length=8, byteorder='big', signed=True)


def int_list_to_bytes(int_list: List[int]) -> bytes:
    bytes_list = [int2bytes(i) for i in int_list]
    return reduce(lambda x, y: x + y, bytes_list)
