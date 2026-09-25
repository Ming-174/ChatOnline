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

//清空接收的内核缓冲区
int recv_package(Pack*pack) {
	int got = 0;
	while (1) {
		int n = recv(pack->fd, pack->rcbuf + pack->rclen, 1024 - pack->rclen, 0);
		if (n == 0) {
			return 0;
		}
		if (n < 0) {
			if (errno == EAGAIN) {
				return got;
			}
			else return -1;
		}
		else {
			got += n;
			pack->rclen += n;
		}
	}
}

//数据包解析
void* handler(void* argc) {
	Pack*pack = (Pack*)argc;
	while(1){
		int n = recv_package(pack);
		if (n == 0) {
			printf("Server disconnect\n");
			break;
		}
		else if (n < 0) {
			perror("recv");
			break;
		}
		if (pack->rclen > 0) {
			if (pack->rclen < 4)continue;
			else if (pack->rclen >= 4) {
				uint32_t len;
				memcpy(&len, pack->rcbuf, 4);
				len = ntohl(len);
				if (len + 4 <= pack->rclen) {
					char buf[1020];
					memcpy(buf, pack->rcbuf+4, len);
					buf[len] = '\0';
					printf("%s\n", buf);
					memmove(pack->rcbuf, pack->rcbuf + len + 4, pack->rclen - len - 4);
					pack->rclen -= len + 4;
				}
				else {
					continue;
				}
			}
		}
	}
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
	int flag = fcntl(client_fd, F_GETFL, 0);
	fcntl(client_fd, F_SETFL, flga | O_NOBLOCK);
	Pack pack;
	pack.fd = client_fd;
	pack.rclen = 0;

	pthread_t tid;
	pthread_create(tid);
	while (1) {
		char buf[1019];
		scanf("%.1019s", buf);
		send_package(buf);
	}
}