import socket
import struct
from time import sleep

HOST="127.0.0.1"
PORT=8888

MSG_REGIS = 0
MSG_CHAT = 3
MSG_NOTICE = 4
MSG_RECOGISE = 5

def main():
    sock=socket.socket()
    try:
        sock.connect((HOST,PORT))
        sleep(1)
        name="Beth"

        body=name.encode("utf-8")
        header=struct.pack("!I",len(body))
        Type=struct.pack("!B",MSG_REGIS)
        sock.sendall(header+Type+body)

        sleep(1)

        msg="Hi"
        body=msg.encode("utf-8")
        header=struct.pack("!I",len(body))
        Type=struct.pack("!B",MSG_CHAT)
        sock.sendall(header+Type+body)
        sleep(1)


    finally:
        sock.close()

if __name__=="__main__":
    main()