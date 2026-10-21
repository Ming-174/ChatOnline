import socket
import struct
from time import sleep

HOST="127.0.0.1"
PORT=8888

MSG_REGIS = 0
MSG_CHAT = 3
MSG_NOTICE = 4
MSG_RECOGISE = 5

#自动接收，转码，拆包
def recv_packet(f):
    header = f.read(4)                    # makefile 保证读满 4 字节
    if not header:
        raise ConnectionError("server closed")
    (length,) = struct.unpack("!I", header)
    t = f.read(1)[0]
    body = f.read(length)                 # 保证读满 length 字节
    return t, body

#传入一个buf，自动转码，组装，发包
def send_packet(sock,Type,buf1,buf2,buf3):
    body1=buf1.encode("utf-8")
    header1=struct.pack("!I",len(body1))
    Type=struct.pack("!B",Type)

    body2=buf2.encode("utf-8")
    header2=struct.pack("!I",len(body2))

    body3=buf3.encode("utf-8")
    header3=struct.pack("!I",len(body3))

    sock.sendall(header1+Type+body1+header2+Type)
    sock.sendall(body2+header3+Type+body3)

def main():
    sock=socket.socket()
    f = sock.makefile("rb") 
    try:
        sock.connect((HOST,PORT))

        t, body = recv_packet(f)
        assert t == MSG_REGIS, f"expected REGIS, got {t}"
        print(f"{body!r}")

        name="Beth"
        empty=""

        send_packet(sock,MSG_REGIS,name,empty,empty)

        t, body = recv_packet(f)
        assert t == MSG_RECOGISE, f"expected RECOGISE, got {t}"
        print(f"{body!r}")

        msg1="Hi"
        msg2="I'm Beth"
        msg3="I love you"

        send_packet(sock,MSG_CHAT,msg1,msg2,msg3)


    finally:
        sock.close()

if __name__=="__main__":
    main()