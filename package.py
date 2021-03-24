from socket import socket as Socket
from enum import Enum
import hashlib


class PackageDataType(Enum):
    BINARY = bytes(0x00)
    # size of int must be 4 bytes, signed
    INT = bytes(0x01)
    UTF8_STR = bytes(0x02)

    @staticmethod
    def get_type_by_value(value) -> Enum:
        for name, member in PackageDataType.__members__.items():
            if member.value == value:
                return member
        raise ValueError()


class Header:
    """
    ------------------------------------------------------------------------------------------
    | 0                4B                  20B                21B         37B          1024B |
    |         4B        |        16B       |        1B         |    16B    |       987B      |
    | length of package | package hashcode | package data type |    ACK    | message string  |
    ------------------------------------------------------------------------------------------

        The header has 1024 bytes data.
    length of package   :       Could be zero if there's no package, will be useful for message transfer only.
                            There's 4B for it, but it could not be greater than 1048576 (b'\x00\x10\x00\x00'),
                            for 1024*1024=1048576, namely the length of package could not be greater than 1MB.
    package hashcode    :       Should be a unique identifier, commonly MD5.
    ACK                 :       Acknowledge character, the value is one of the identifier of the packages. Zero
                            for none.
    package data type   :       See the supported types in PackageDataType below.
    message string      :       Must encode with utf-8, could store 987 pure ASCII characters or 329 pure Chinese
                            characters.
    """
    HEADER_LEN = 1024
    HEADER_PACKAGE_LEN_OFFSET, HEADER_PACKAGE_LEN_LEN = 0, 4
    HEADER_PACKAGE_HASHCODE_OFFSET, HEADER_PACKAGE_HASHCODE_LEN = 4, 16
    HEADER_PACKAGE_DATATYPE_OFFSET, HEADER_PACKAGE_DATATYPE_LEN = 20, 1
    HEADER_ACK_OFFSET, HEADER_ACK_LEN = 21, 16
    HEADER_MESSAGE_OFFSET, HEADER_MESSAGE_LEN = 37, 987

    def __init__(self,
                 package_len: bytes = b'\x00' * HEADER_PACKAGE_LEN_LEN,
                 package_hashcode: bytes = b'\x00' * HEADER_PACKAGE_HASHCODE_LEN,
                 package_data_type: bytes = b'\x00' * HEADER_PACKAGE_DATATYPE_LEN,
                 ack: bytes = b'\x00' * HEADER_ACK_LEN,
                 message: bytes = b'\x00' * HEADER_MESSAGE_LEN):
        self.__package_len: bytes = package_len
        self.__package_hashcode: bytes = package_hashcode
        self.__package_data_type: bytes = package_data_type
        self.__ack: bytes = ack
        self.__message: bytes = message

    def load_from_header_data(self, header_data: bytes):
        if len(header_data) != self.HEADER_LEN:
            raise HeaderParseError(f"Header size is {len(header_data)} rather than {self.HEADER_LEN}")

        self.set_package_len(header_data[self.HEADER_PACKAGE_LEN_OFFSET
                                         :self.HEADER_PACKAGE_LEN_OFFSET + self.HEADER_PACKAGE_LEN_LEN])
        self.set_package_hashcode(header_data[self.HEADER_PACKAGE_HASHCODE_OFFSET
                                              :self.HEADER_PACKAGE_HASHCODE_OFFSET + self.HEADER_PACKAGE_HASHCODE_LEN])
        self.set_package_data_type(header_data[self.HEADER_PACKAGE_DATATYPE_OFFSET
                                               :self.HEADER_PACKAGE_DATATYPE_OFFSET + self.HEADER_PACKAGE_DATATYPE_LEN])
        self.set_ack(header_data[self.HEADER_ACK_OFFSET:self.HEADER_ACK_OFFSET + self.HEADER_ACK_LEN])
        self.set_message(header_data[self.HEADER_MESSAGE_OFFSET:self.HEADER_MESSAGE_OFFSET + self.HEADER_MESSAGE_LEN])

    def get_header_data(self) -> bytes:
        header_data: bytes = self.__package_len + self.__package_hashcode + self.__ack + self.__package_data_type + self.__message
        if len(header_data) != self.HEADER_LEN:
            raise BytesLengthError()
        return header_data

    def set_package_len(self, package_len):
        if package_len is None:
            self.__package_len = b'\x00' * self.HEADER_PACKAGE_LEN_LEN
            return
        if isinstance(package_len, bytes):
            self.__package_len = package_len
        elif isinstance(package_len, int):
            self.__package_len = package_len.to_bytes(length=self.HEADER_PACKAGE_LEN_LEN, byteorder='big', signed=False)
        else:
            raise TypeError("Unsupported package len type!")

    def get_package_len(self, parse=False):
        if not parse:
            return self.__package_len
        return int.from_bytes(self.__package_len, byteorder='big', signed=False)

    def has_package(self):
        """
            Check if there's a package after the header.
        :return:    True  ->   has package
                    False ->   has no package
        """
        return self.get_package_len(parse=True) != 0

    def set_package_hashcode(self, hashcode):
        if hashcode is None:
            self.__package_hashcode = b'\x00' * self.HEADER_PACKAGE_HASHCODE_LEN
            return
        if isinstance(hashcode, bytes):
            self.__package_hashcode = hashcode
        else:
            raise TypeError("Unsupported package hashcode type!")

    def get_package_hashcode(self):
        return self.__package_hashcode

    def set_package_data_type(self, data_type):
        if data_type is None:
            self.__package_data_type = b'\x00' * self.HEADER_PACKAGE_DATATYPE_LEN
            return
        if isinstance(data_type, bytes):
            self.__package_data_type = data_type
        elif isinstance(data_type, PackageDataType):
            self.__package_data_type = data_type.value
        else:
            raise TypeError("Unsupported package data type!")

    def get_package_data_type(self, parse=False):
        if not parse:
            return self.__package_data_type
        return PackageDataType.get_type_by_value(self.__package_data_type)

    def set_ack(self, ack):
        if ack is None:
            self.__ack = b'\x00' * self.HEADER_ACK_LEN
            return
        if isinstance(ack, bytes):
            self.__ack = ack
        else:
            raise TypeError("Unsupported ACK type!")

    def get_ack(self):
        return self.__ack

    def set_message(self, message):
        if message is None:
            self.__message = b'\x00' * self.HEADER_MESSAGE_LEN
            return
        if isinstance(message, bytes):
            self.__message = message
        elif isinstance(message, str):
            encode_bytes: bytes = message.encode()
            if len(encode_bytes) > self.HEADER_MESSAGE_LEN:
                raise MessageOutOfSizeException()
            if len(encode_bytes) < self.HEADER_MESSAGE_LEN:
                encode_bytes += b'\x00' * (self.HEADER_MESSAGE_LEN - len(encode_bytes))
            self.__message = encode_bytes
        else:
            raise TypeError("Unsupported message type!")

    def get_message(self, parse=False):
        if not parse:
            return self.__message
        if self.__message == b'\x00' * self.HEADER_MESSAGE_LEN:
            return None
        return self.__message.decode()

    # def get_package_len(self,):

    # def __init__(self, package_len: int = 0, package_hashcode: bytes = None, ack: bytes = b'\x00' * 4,
    #              package_data_type: PackageDataType = PackageDataType.BINARY, message: str = None):
    #     """
    #     :param message:         If message is not none, then the value about package could be none.
    #     :param package_len:     1MBytes=1048576Bytes most
    #                             The max integer value of 4B is 4294967295, which
    #                         is far enough for the bytes'number of 1MB.
    #     """
    #     if (package_hashcode is not None and len(package_hashcode) != self.HEADER_PACKAGE_HASHCODE_LEN) \
    #             or len(ack) != self.HEADER_ACK_LEN:
    #         raise BytesLengthError()
    #     if package_len != 0:
    #         if package_hashcode is None:
    #             raise LackOfPackageHashcodeException()
    #     if message is None and package_len == 0:
    #         raise LackOfMessageException()
    #     if message is not None and len(message.encode()) > self.HEADER_MESSAGE_LEN:
    #         raise MessageOutOfSizeException()
    #
    #     self.package_len: bytes = package_len.to_bytes(length=4, byteorder='big', signed=False)
    #     self.package_hashcode: bytes = b'\x00' * self.HEADER_PACKAGE_HASHCODE_LEN if package_hashcode is None \
    #         else package_hashcode
    #     self.ack: bytes = ack
    #     self.package_data_type: bytes = package_data_type.value
    #     if message is None:
    #         self.message: bytes = b'\x00' * self.HEADER_MESSAGE_LEN
    #     else:
    #         temp: bytes = message.encode()
    #         if len(temp) < self.HEADER_MESSAGE_LEN:
    #             temp += b'\x00' * (self.HEADER_MESSAGE_LEN - len(temp))
    #         self.message: bytes = temp
    #
    #     self.data: bytes = self.package_len + self.package_hashcode + self.ack + self.package_data_type + self.message
    #     assert len(self.data) == self.HEADER_LEN


class Package:
    def __init__(self, payload: bytes, data_type: PackageDataType, header: Header = None):
        self.__payload = payload
        self.__data_type = data_type
        self.__header = header

    def get_header(self):
        return self.__header

    def get_payload(self):
        return self.__payload

    def generate_default_header(self, msg: str = None):
        header = Header()
        header.set_message(msg)
        header.set_package_len(len(self.__payload))

        hash_value: bytes = hashlib.md5(self.__payload).digest()
        header.set_package_hashcode(hash_value)

        self.__header = header

    # def __init__(self, payload: bytes, package_hashcode: bytes, message: str = None,
    #              datatype: PackageDataType = PackageDataType.BINARY, header=None):
    #     """
    #         Every package has a header, the header contains the package's length,
    #     before sending the package, must send header first, otherwise the receiver
    #     has no idea to the package's size.
    #     """
    #     self.payload = payload
    #     if header is not None:
    #         self.header = header
    #     else:
    #         header = Header()
    #         header.set_package_len(package_len=len(payload))
    #         header.set_package_hashcode(hashcode=package_hashcode)
    #         header.set_package_data_type(data_type=datatype)
    #         header.set_message(message=message)
    #         self.header = header


class PackageWithTimer:
    def __init__(self, package):
        self.package = package
        self.timer = None
        """
            True means ack has been received on time.
            False means ack has not been received on time. 
        """
        self.ack_flag = False


def send_package(package: Package, sock: Socket):
    header = package.get_header()
    payload = package.get_payload()

    sock.send(header.get_header_data())
    sock.send(payload)


def receive_package(sock: Socket):
    """
        Receive the header first, then receive the package if has.
    :param sock:
    :return:    Header object if the size of package is zero.
                Package object if the size of package is not zero,
            but there's still a header object in the package object.
    """
    header_data = sock.recv(Header.HEADER_LEN)

    header = Header()
    header.load_from_header_data(header_data)

    if not header.has_package():
        return header
    payload = sock.recv(header.get_package_len(parse=True))
    data_type = header.get_package_data_type(parse=True)

    package = Package(payload=payload, data_type=data_type, header=header)
    return package


# package_len = int.from_bytes(header_data[:Header.HEADER_PACKAGE_LEN_LEN], byteorder='big', signed=False)
#
# if package_len == 0:
#
# b_package_hashcode = header_data[Header.HEADER_PACKAGE_HASHCODE_OFFSET:
#                                  Header.HEADER_PACKAGE_HASHCODE_OFFSET + Header.HEADER_PACKAGE_HASHCODE_LEN]

class LackOfMessageException(Exception):
    pass


class LackOfPackageHashcodeException(Exception):
    pass


class SendPackageException(Exception):
    pass


class BytesLengthError(ValueError):
    pass


class HeaderParseError(ValueError):
    pass


class PackageOutOfSizeException(Exception):
    pass


class MessageOutOfSizeException(Exception):
    pass


class ReceivePackageException(Exception):
    pass
