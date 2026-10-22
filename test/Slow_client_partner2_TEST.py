#慢客户端测试搭档2,极限测试
#慢客户端在sleep期间需要有其他客户端向服务器发送chat消息，服务器广播给慢客户端，才能检测服务器应对慢客户端的功能
#在慢客户端sleep期间，partner客户端使用循环发送数据包，检查服务器在双方缓冲区全满情况下的处理行为是否正常
#预期：服务器在极短时间内执行丢包，打印日志drop packet，慢客户端只能收到前面部分的数据包
#可以使用while true; do ss -tn dport = :8888; echo "---"; sleep 1; done在bash查看内核缓冲区情况
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

        msg="Hi"*489

        while 1:
            send_packet(sock,MSG_CHAT,msg)

    finally:
        sock.close()

if __name__=="__main__":
    main()