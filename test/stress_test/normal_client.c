#include<stdio.h>
#include<arpa/inet.h>
#include<sys/socket.h>
#include<sys/epoll.h>
#include<netinet/in.h>
#include<unistd.h>
#include<fcntl.h>
#include<errno.h>
#include<string.h>
#include<stdint.h>
#include<stdlib.h>
#include<time.h>
#include<endian.h>

#define MAX_PACKET 1024

int epfd;
int client_fd;

#define STA_CLOSE -1
#define STA_CONNECT 0
#define STA_UNCONNECT 1
#define STA_UNREADY 2
#define STA_READY 3

#define MAX_EVENTS 128	//最大同时处理响应链接数
#define MAX_CLIENTS 1024	//最大客户端链接数
#define MAX_PACKET 1024		//单个数据包最大大小
#define RCBUF_SIZE (2 * MAX_PACKET + 4 )	//接收缓冲区长度
#define SDBUF_SIZE (4 * MAX_PACKET + 4 )	//发送缓冲区长度
#define MAX_NAME 32		//最长名字

#define MSG_REGIS 0
#define MSG_LOGIN 1
#define MSG_LOGOUT 2
#define MSG_CHAT 3
#define MSG_NOTICE 4
#define MSG_RECOGISE 5

/*
sdbuf用来存放要发给服务器的字节
rcbuf用来存放服务器发来的字节
键盘发来的字节用keyborad_buf存放，对于键盘，只有EPOLLIN输入事件
*/
//服务器缓冲区控制
char sdbuf[SDBUF_SIZE];
int sdlen = 0;
char rcbuf[RCBUF_SIZE];
int rclen = 0;

//键盘输入专用
char keyborad_buf[RCBUF_SIZE];
int keyborad_len = 0;

int status = STA_UNCONNECT;

uint64_t current_us() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}

int recv_clear() {
	ssize_t got = 0;
	while (rclen+got < RCBUF_SIZE) {//用户态缓冲区写满了的出口,此时内核缓冲区可能还有数据，但是暂时不读了
		ssize_t n = recv(client_fd, rcbuf + rclen + got, RCBUF_SIZE - rclen - got, 0);
		if (n == 0) {//服务器关闭
			return -1;
		}
		else if (n < 0) {
			if (errno == EINTR)continue;
			else if (errno == EAGAIN || errno == EWOULDBLOCK) {
				rclen += got;
				return got;
			}
			else {
				return -2;
			}
		}
		got += n;
	}
	rclen += got;
	return got;//got为实际读取的字节数
}


//使用send_clear尝试发送清空用户态缓冲区，不管成不成功都返回具体发送的字节数，是否清空由sdlen判断，返回-1代表服务器关闭，原因不明
int send_clear() {
	size_t sent = 0;
	while (sent < sdlen) {//将数据全部放入内核缓冲区时的出口，此时内核缓冲区可能满了,可能没满
		ssize_t n = send(client_fd, sdbuf + sent, sdlen - sent, 0);
		if (n <= 0) {
			if (errno == EINTR)continue;
			//内核缓冲区满时出口
			else if (errno == EAGAIN || errno == EWOULDBLOCK) {
				sdlen -= sent;
				memmove(sdbuf, sdbuf + sent, sdlen);
				return sent;
			}
			else {
				return -1;
			}
		}
		else if (n > 0) {
			sent += n;
		}
	}
	sdlen -= sent;
	sdlen = 0;
	return sent;
}

//返回-1代表服务器关闭，1代表正常发送，不保证发送完成，若没完成，将在下一轮的EPOLL事件触发
int flush() {
	int x = send_clear();
	if (x < 0) {
		return -1;
	}
	struct epoll_event ev;
	ev.data.fd = client_fd;
	//如果没发送完，就注册EPOLLOUT
	if (sdlen > 0) {
		ev.events = EPOLLIN | EPOLLOUT;
		epoll_ctl(epfd, EPOLL_CTL_MOD, client_fd, &ev);
	}
	else {
		ev.events = EPOLLIN;
		epoll_ctl(epfd, EPOLL_CTL_MOD, client_fd, &ev);
	}
	return 1;
}

int main(int argc,char*argv[]) {

	char* dir = "stress_test_result";
	char path[300];
	snprintf(path, sizeof(path), "./%s/%s", dir, argv[1]);

	int result_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

	client_fd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(8888);
	inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

	if (connect(client_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		perror("connect");
		return 1;
	}

	status = STA_CONNECT;

	int flag = fcntl(client_fd, F_GETFL, 0);
	fcntl(client_fd, F_SETFL, flag | O_NONBLOCK);

	epfd = epoll_create(1);


	struct epoll_event  client_ev;

	client_ev.data.fd = client_fd;
	client_ev.events = EPOLLIN;

	epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);

	struct epoll_event events[2];

	int turn = 1;

	while (1) {
		int n = epoll_wait(epfd, events, 2, -1);
		if (n < 0) {
			if (errno == EINTR)continue;
			perror("epoll_wait");
			break;
		}
		for (int i = 0; i < n; i++) {
			int fd = events[i].data.fd;
			if (fd == client_fd) {
				if (events[i].events & EPOLLIN) {
					int x = recv_clear();
					if (x == -1) {
						printf("Server disconnect\n");
						return 0;
					}
					else if (x == -2) {
						perror("recv");
						return 1;
					}

					//是否能读取出一个头
					while (rclen >= 5) {

						//获得数据头
						uint32_t len;
						memcpy(&len, rcbuf, 4);
						len = ntohl(len);

						//是否有一个完整的数据包
						if (rclen - 5 < len)break;

						//消费数据头
						rclen -= 4;
						memmove(rcbuf, rcbuf + 4, rclen);

						//获取类型
						uint8_t type;
						memcpy(&type, rcbuf, 1);
						//消费类型
						rclen -= 1;
						memmove(rcbuf, rcbuf + 1, rclen);
						if (type == MSG_REGIS) {
							status = STA_UNREADY;
							printf("%.*s\n", len, rcbuf);
						}
						else if (type == MSG_RECOGISE) {
							status = STA_READY;
							printf("%.*s\n", len, rcbuf);
						}
						else if (type == MSG_NOTICE) {
							printf("%.*s\n", len, rcbuf);
						}
						else if (type == MSG_CHAT) {
							uint32_t name_len;
							memcpy(&name_len, rcbuf, 4);
							name_len = ntohl(name_len);

							char result[900];

							//printf("%.*s:", name_len, rcbuf + 4);
							//snprintf(result, sizeof(result),"%.*s:", name_len, rcbuf + 4);

							//printf("%.*s\n", len - 4 - name_len - 8, rcbuf + 4 + name_len);
							//snprintf(result + name_len + 1, sizeof(result), "%.*s", len - 4 - name_len - 8, rcbuf + 4 + name_len);

							uint64_t time;
							memcpy(&time, rcbuf + len - 8, 8);
							time = be64toh(time);
							uint64_t curr_time = current_us();
							uint64_t distant_time = curr_time - time;
							//printf("%d\n", (int)distant_time);
							//snprintf(result + name_len + 1 + len - 4 - name_len - 8, sizeof(result), "%d\n", (int)distant_time);
							int len = snprintf(result, sizeof(result), "%.*s:%d\n", name_len, rcbuf + 4, (int)distant_time);

							write(result_fd, result, len);
						}
						rclen -= len;
						memmove(rcbuf, rcbuf + len, rclen);
					}
				}
				if (events[i].events & EPOLLOUT) {
					int x = flush();
					if (x < 0) {
						printf("Server disconnect\n");
						return 0;
					}
				}
			}
		}
		if (status == STA_UNREADY && turn == 1) {
			uint32_t len = htonl(strlen(argv[1]));
			memcpy(sdbuf + sdlen, &len, sizeof(len));
			sdlen += 4;

			uint8_t type = MSG_REGIS;
			memcpy(sdbuf + sdlen, &type, 1);
			sdlen += 1;

			memcpy(sdbuf + sdlen, argv[1], strlen(argv[1]));
			sdlen += strlen(argv[1]);

			int x = flush();
			if (x < 0) {
				printf("Server disconnect\n");
				return 0;
			}
			turn = 2;
		}
		else if (status == STA_READY&&turn == 2) {
			sleep(20);
			char buf[] = "Hi!I'm Beth,Beth love you";
			if (SDBUF_SIZE - sdlen < strlen(buf) + 5) {
				printf("Space no enough");
				continue;
			}
			uint32_t len = htonl(strlen(buf)+8);
			memcpy(sdbuf + sdlen, &len, sizeof(len));
			sdlen += 4;

			uint8_t type = MSG_CHAT;
			memcpy(sdbuf + sdlen, &type, 1);
			sdlen += 1;

			memcpy(sdbuf + sdlen, buf, strlen(buf));
			sdlen += strlen(buf);

			uint64_t time = current_us();
			time = htobe64(time);
			memcpy(sdbuf + sdlen, &time, sizeof(time));
			sdlen += 8;

			int x = flush();
			if (x < 0) {
				printf("Server disconnect\n");
				return 0;
			}
			turn = 0;
		}
	}
}