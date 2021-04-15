from proxy import Proxy
import socket
from app.utils import Logger

log = Logger()

if __name__ == '__main__':
    s = socket.socket()
    host = socket.gethostname()
    port = 23456
    s.bind((host, port))
    s.listen(5)
    log.info("""
           ___    ______ ______ ______   ____   ____   ____  _  ____  __
          /   |  / ____//_  __// ____/  / __ \ / __ \ / __ \| |/ /\ \/ /
         / /| | / /      / /  / /_     / /_/ // /_/ // / / /|   /  \  / 
        / ___ |/ /___   / /  / __/    / ____// _, _// /_/ //   |   / /  
       /_/  |_|\____/  /_/  /_/      /_/    /_/ |_| \____//_/|_|  /_/   
    """)
    proxy = Proxy(socket=s, target_seq_data_num=30, max_buffer=5)
