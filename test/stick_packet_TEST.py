#粘包测试
#将3个完整数据包一次性写入 TCP 字节流
import socket
import struct
HOST="127.0.0.1"
PORT=8888
def send_pack(sock,msg1,msg2,msg3):
    body1 = msg1.encode("utf-8")
    body2 = msg2.encode("utf-8")
    body3 = msg3.encode("utf-8")

    header1 = struct.pack("!I", len(body1))
    header2 = struct.pack("!I", len(body2))
    header3 = struct.pack("!I", len(body3))

    sock.sendall(header1+body1+header2+body2+header3+body3)
def main():
    sock=socket.socket()
    try:
        sock.connect((HOST,PORT))
        msg1="Beth"
        msg2="Harmon"
        msg3="Love You"
        send_pack(sock,msg1,msg2,msg3)
    finally:
        sock.close()
if __name__=="__main__":
    main()