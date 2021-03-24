import logging.handlers


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
            '[%(asctime)s] - %(filename)s [Line:%(lineno)d] - [%(levelname)s]-[thread:%(thread)s]-[process:%(process)s] - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.addHandler(fh)
        self.addHandler(ch)


def bytes2int(b: bytes) -> int:
    return int(b)


def string2bytes(string: str) -> bytes:
    return bytes(string, encoding="utf-8")
