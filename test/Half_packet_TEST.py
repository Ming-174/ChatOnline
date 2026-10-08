#半包测试：
#先发送协议头，间隔5秒后在发送消息体
import socket
import struct
from time import sleep

HOST="127.0.0.1"
PORT=8888

def send_pack(sock,msg):
    body=msg.encode("utf-8")
    header=struct.pack("!I",len(body))
    sock.sendall(header)
    sleep(5)
    sock.sendall(body)

def main():
    sock=socket.socket()
    try:
        sock.connect((HOST,PORT))
        msg="Hi Beth"
        send_pack(sock,msg)
    finally:
        sock.close()

if __name__=="__main__":
    main()