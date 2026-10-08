import socket
import struct


HOST = "127.0.0.1"
PORT = 8888


def send_packet(sock, message):
    """按照服务器协议发送一个数据包。"""
    data = message.encode("utf-8")

    header = struct.pack("!I", len(data))

    sock.sendall(header + data)


def recv_exact(sock, size):
    """接收指定数量的字节。"""
    data = b""

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise ConnectionError("服务器断开连接")

        data += chunk

    return data


def recv_packet(sock):
    """按照服务器协议接收一个数据包。"""
    header = recv_exact(sock, 4)

    length = struct.unpack("!I", header)[0]

    data = recv_exact(sock, length)

    return data.decode("utf-8")


def main():
    sock = socket.socket()

    try:
        # 连接服务器
        sock.connect((HOST, PORT))

        print("已连接服务器")
        print("输入消息并发送，输入 quit 退出")

        while True:
            message = input("> ")

            if message == "quit":
                break

            send_packet(sock, message)

            print("消息已发送")

    finally:
        sock.close()
        print("连接已关闭")


if __name__ == "__main__":
    main()