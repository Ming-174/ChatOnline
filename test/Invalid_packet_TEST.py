#违法数据包测试：
#违规数据包为 消息头+消息体 长度超过1024字节
#先发送极限数据包[4字节消息头]+[1020字节消息体]
#再发送违法数据包[4字节消息头]+[1021字节消息体]
import socket
import struct
from time import sleep

HOST="127.0.0.1"
PORT=8888
def send_pack(sock,msg):
    body=msg.encode("utf-8")
    header=struct.pack("!I",len(body))
    sock.sendall(header+body)
def main():
    sock=socket.socket()
    try:
        sock.connect((HOST,PORT))
        msg="😈"*255
        send_pack(sock,msg)
        sleep(1)
        msg+="A"
        send_pack(sock,msg)
    finally:
        sock.close()
if __name__=="__main__":
    main()