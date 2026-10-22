#慢客户端测试搭档1
#满客户端在sleep期间需要有其他客户端向服务器发送chat消息，服务器广播给慢客户端，才能检测服务器应对慢客户端的功能
#当前为不丢包测试，发送多个数据包，总计4100字节
#未开启边界测试预期：慢客户端在sleep结束后正常收到消息，服务器运行正常，日志正常播报
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
        print(f"{body!r}")

        name="Beth_partner"

        send_packet(sock,MSG_REGIS,name)

        t, body = recv_packet(f)
        assert t == MSG_RECOGISE, f"expected RECOGISE, got {t}"
        print(f"{body!r}")

        #名字长度为12，那能发送的最大消息为1024-4-1-4-12=1003
        #每个消息的上文长度为21
        #而我们需要发送4100字节的数据包

        msg="Hi"*489

        #发送999*4=3996字节
        send_packet(sock,MSG_CHAT,msg)
        send_packet(sock,MSG_CHAT,msg)
        send_packet(sock,MSG_CHAT,msg)
        send_packet(sock,MSG_CHAT,msg)

        #还需要发104字节
        msg="a"*83
        send_packet(sock,MSG_CHAT,msg)

    finally:
        sock.close()

if __name__=="__main__":
    main()