#权限越界测试
#在未完成注册时，服务器不接收chat消息，只接收regis消息
#在完成注册之后，服务器不接收regis消息，只接收chat消息
#一、在注册前发送chat，服务器日志打印越界消息与丢包操作
#二，在注册之后发regis，服务器日志打印越界消息与丢包操作

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

        #接收登录消息
        t, body = recv_packet(f)
        assert t == MSG_REGIS, f"expected REGIS, got {t}"
        print(f"{body!r}")

        name="Beth"
        #故意不注册，先发送chat消息
        send_packet(sock,MSG_CHAT,name)
        #没有提示，服务器直接把包丢了就不管你了
        #t, body = recv_packet(f)
        #print(f"{body!r}")

        #正常登录
        msg="Beth"
        send_packet(sock,MSG_REGIS,msg)

        #收到认可消息,
        t,body=recv_packet(f)
        print(f"{body!r}")

        msg="Harmon"
        #故意发送regis消息
        send_packet(sock,MSG_REGIS,msg)
        #还是没有提示，服务器依旧不管
        #t,body=recv_packet(f)
        #print(f"{body!r}")

        msg="Hi"
        send_packet(sock,MSG_CHAT,msg)

    finally:
        sock.close()

if __name__=="__main__":
    main()