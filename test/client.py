import socket
import struct
import threading

HOST = "127.0.0.1"
PORT = 8888

MSG_REGIS = 0
MSG_CHAT = 3
MSG_NOTICE = 4
MSG_RECOGISE = 5

STA_UNREADY 0
STA_READY 1
STA_CLOSE 2

def recv_exact(sock,size):
    #创建bytes变量
    data=b""

    while len(data)<size:
        #clip接收数据
        clip=sock.recv(size-len(data))
        #检查接收到的数据是否为空
        if not clip:
            raise ConnectionError("Server disconnected")
        #装进data
        data+=clip

    return data

#固定读取4字节头后，读取1字节type，再按照4字节头给的长度读取payload，最后返回type和未解析的payload
def recv_pack(sock):
    #接收消息头
    header=recv_exact(sock,4)
    #消息头从网络字节序转为主机字节序
    length=struct.unpack("!I",header)[0]
    #接收类型Type
    Type=recv_exact(sock,1)
    Type=struct.unpack("!B",Type)[0]
    #接收消息体
    data=recv_exact(sock,length)
    
    return Type,data

def send_package(sock,msg):
    #python文本转utf-8
    data = msg.encode("utf-8")
    #消息头长度转网络序
    header=struct.pack("!I",len(data))
    #python的牛逼发送
    sock.sendall(header+data)

def work(sock):
    try:
        while True:
            Type,data=recv_pack(sock)
            if Type==MSG_REGIS:
                data.decode('utf-8')
                print(data)
                

    except(ConnectionError,OSError):
        print("Thread close")

def main():
    #创建连接
    sock = socket.socket()

    #创建与执行线程
    thread=threading.Thread(
        target=work,
        args=(sock,)
    )


    #try建立判断区
    try:

        sock.connect((HOST,PORT))

        thread.start()

        while 1:

            msg=input(":")

            if msg == "quit":
                break

            send_package(sock,msg)
    #终结程序
    finally:

        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        sock.close()

        print("Disconnect")
if __name__=="__main__":
    main()