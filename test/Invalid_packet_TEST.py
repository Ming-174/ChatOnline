#不合规数据包测试
#不合规数据包有以下几种：注册名超限32个字节，数据包过大，数据包大小违规，数据类型不明
#数据包过大为其发送的数据包大小本身是合规的，但是由于其太大了，在服务器组装后拼接比如添加name_len和name后数据包大小违规，服务器会拒绝业务并提示
#数据包大小违规为其发送的数据包本身大小就超过了最大数据包限制
#一、发送33字节名字，预期收到notice提示后再收到regis重新注册要求
#二、发送32字节合法名字，收到recogise认证消息
#三、发送刚好过大消息，预期收到notice提醒，且消息未被转发
#四、发送刚好不过大消息，预期收不到来信，消息正常被转发
#五、发送不明类型的消息，预期可能收到notice并被直接挂断
#五、发送刚好违规大小，预期可能收到notice并被直接挂断
#注释：五只能选一个做，手动修改执行路径来切换
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

        #初次连接，等待收到regis消息
        t, body = recv_packet(f)
        assert t == MSG_REGIS, f"expected REGIS, got {t}"
        print(f"{body!r}")

        #一、超限名字
        name="BethBethBethBethBethBethBethBeth1"#33个字，刚好超限名字
        send_packet(sock,MSG_REGIS,name)

        #预期收到notice提示和regis重新要求注册
        t, body = recv_packet(f)
        assert t == MSG_NOTICE, f"expected MSG_NOTICE, got {t}"
        print(f"{body!r}")
        t, body = recv_packet(f)
        assert t == MSG_REGIS, f"expected MSG_REGIS, got {t}"
        print(f"{body!r}")

        #二、刚好名字不过大
        name="BethBethBethBethBethBethBethBeth"#刚好32个字，最长长度名子
        send_packet(sock,MSG_REGIS,name)
        #预期收到recogise
        t, body = recv_packet(f)
        assert t == MSG_RECOGISE, f"expected RECOGISE, got {t}"
        print(f"{body!r}")
        

        #三、发送过大消息
        #能发送的最大长度为1024-4-1-4-name_len(32)=983
        msg="a"*984
        send_packet(sock,MSG_CHAT,msg)

        #预期收到提示
        t, body = recv_packet(f)
        assert t == MSG_NOTICE, f"expected MSG_NOTICE, got {t}"
        print(f"{body!r}")

        #四、刚好不过大
        msg="b"*983#刚好合法
        send_packet(sock,MSG_CHAT,msg)

        sock.settimeout(0.5)
        #预期收不到提示
        t, body = recv_packet(f)
        assert t == MSG_NOTICE, f"expected MSG_NOTICE, got {t}"
        print(f"{body!r}")

        sleep(2)

        #五、违规大小数据包  1024-4-1=1020
        msg="c"*1020
        send_packet(sock,MSG_CHAT,msg)
        t, body = recv_packet(f)
        assert t == MSG_NOTICE, f"expected MSG_NOTICE, got {t}"
        print(f"{body!r}")

        #五、不明类型数据包
        #msg="You shouldn't see me"
        #send_packet(sock,9,msg)
        ##收到提醒，或收不到提醒就被挂断
        #t, body = recv_packet(f)
        #assert t == MSG_NOTICE, f"expected MSG_NOTICE, got {t}"
        #print(f"{body!r}")



    finally:
        sock.close()

if __name__=="__main__":
    main()
    