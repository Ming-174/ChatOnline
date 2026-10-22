#半包断连测试
#在发送完header后直接断开连接，重连，循环执行50次
#在50次之后，发送一次正常消息，看看这个消息正不正常
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
    sock.sendall(header)
    sock.sendall(Type)

    #sock.sendall(body[0:1])
    #sock.sendall(body[1:])

def send_packet2(sock,Type,buf):
    body=buf.encode("utf-8")
    header=struct.pack("!I",len(body))
    Type=struct.pack("!B",Type)
    sock.sendall(header)
    sock.sendall(Type)

    sock.sendall(body[0:1])
    sock.sendall(body[1:])

def main():
    for i in range(51):
        sock=socket.socket()
        f = sock.makefile("rb") 
        try:
            sock.connect((HOST,PORT))

            t, body = recv_packet(f)
            assert t == MSG_REGIS, f"expected REGIS, got {t}"
            print(f"{body!r}")

            name="Beth"

            send_packet2(sock,MSG_REGIS,name)

            t, body = recv_packet(f)
            assert t == MSG_RECOGISE, f"expected RECOGISE, got {t}"
            print(f"{body!r}")
            

            msg="Hi"

            if i == 50:
                send_packet2(sock,MSG_CHAT,msg)
            else:
                send_packet(sock,MSG_CHAT,msg)



        finally:
            f.close()
            sock.close()
            sleep(0.5)

if __name__=="__main__":
    main()