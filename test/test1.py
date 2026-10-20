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
def send_packet(sock,Type,buf):
    body=buf.encode("utf-8")
    header=struct.pack("!I",len(body))
    Type=struct.pack("!B",Type)
    sock.sendall(header+Type+body)

def main():
    sock=socket.socket()
    f = sock.makefile("rb") 
    try:
        sock.connect((HOST,PORT))

        t, body = recv_packet(f)
        assert t == MSG_REGIS, f"expected REGIS, got {t}"
        print(f"[OK] server sent REGIS: {body!r}")

        name="Beth"

        send_packet(sock,MSG_REGIS,name)

        t, body = recv_packet(f)
        assert t == MSG_RECOGISE, f"expected RECOGISE, got {t}"
        assert b"Beth" in body, f"RECOGISE missing name: {body!r}"
        print(f"[OK] server sent RECOGISE: {body!r}")

        msg="Hi"

        send_packet(sock,MSG_CHAT,msg)

        


    finally:
        sock.close()

if __name__=="__main__":
    main()