#include<stdio.h>
#include<unistd.h>
#include<fcntl.h>
#include<pthread.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<netinet/in.h>

typedef struct pack {
	int fd;
	char rcbuf[1024];
	int rclen;
}Pack;

//接收的时候是整个完整数据包带协议头一起接收
int read_exact(int fd, char* buf, ssize_t size) {
	int ret = 0;
	while (ret < size) {
		int n = recv(fd, buf + ret, size - ret, 0);
		if (n == 0)return 0;
		else if (n < 0)return -1;
		else if (n > 0) {
			ret += n;
		}
	}
	return ret;
}
//清空接收的内核缓冲区
int recv_package(Pack*pack) {

}

//数据包解析
void* handler(void* argc) {
	Pack pack;
	pack.fd = (int)(intptr)argc;
	pack.rclen = 0;


}

int write_exact(int fd, char* buf, ssize_t size) {
	int sent = 0;
	while (sent < size) {
		int n = send(fd, buf + sent, size - sent, 0);
		if (n < 0) {
			return -1;
		}
		else if (n == 0) {
			return 0;
		}
		else if (n > 0) {
			sent += n;
		}
	}
	return sent;
}
//发送的时候是分段发送的
void send_package(int fd, char* buf) {
	uint32_t len = htonl(strlen(buf));
	int n = send_package(fd, (char*)&len, sizeof(len));
	if (n == 0) {
		printf("Server disconnect\n");
		return;
	}
	else if (n == -1) {
		perror("send");
		return;
	}
	n = send_package(fd, buf, strlen(buf), 0);
	if (n == 0) {
		printf("Server disconnect\n");
		return;
	}
	else if (n == -1) {
		perror("send");
		return;
	}
}

int main() {
	int client_fd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(8888);
	inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
	if ((connect(client_fd, (struct sockaddr*)&addr), sizeof(addr)) < 0) {
		perror("connect");
		return 1;
	}
	pthread_t tid;
	pthread_create(tid);
	while (1) {
		char buf[1023];
		scanf("%.1023s", buf);
		send_package(buf);
	}
}