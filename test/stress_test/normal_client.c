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

int connect_number = 0;

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

typedef struct Conn {
	int fd;
	//接收缓冲区
	char rcbuf[RCBUF_SIZE];
	//接收缓冲区的数据字节长度
	int rclen;
	//发送缓冲区
	char sdbuf[SDBUF_SIZE];
	//发送缓冲区的长度
	int sdlen;

	//连接状态
	int status;		//-1故障  0未启用  1已连接	2已可完成登录，可启用
}Conn;

Conn conns[MAX_CLIENTS];

//先在整个conns名单遍历一遍，找到空位后将这个连接初始化，再放入，这个放入的位置idx几乎是随机，只是按顺序找空位，他所处的位置不再代表任何含义
Conn* conn_insert(int fd) {
	for (int i = 0; i < MAX_CLIENTS; i++) {
		if (conns[i].fd == -1) {
			conns[i].fd = fd;
			conns[i].rclen = 0;
			conns[i].sdlen = 0;
			conns[i].status = STA_CONNECT;
			return &conns[i];
		}
	}
	return NULL;
}


uint64_t current_us() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}


//返回值为-1的情况代表错误和客户端关闭，这两种都需要进行处理，所以被归为一类
//正常情况返回值为got
//recv_clear只负责一件事，就是尽可能把rcbuf写满，至于内核缓冲区是否排空，无所谓，内核缓冲区有残留的话下次epoll_wait会再次触发
//连接正常读取：返回值>=0
//连接关闭：返回值 = -2
//连接出现致命错误：返回值=-1
int recv_clear(Conn* conn) {
	int got = 0;
	while (1) {
		int space = RCBUF_SIZE - conn->rclen;
		//写满了，就到这吧
		if (space == 0) {
			return got;
		}
		int n = recv(conn->fd, conn->rcbuf + conn->rclen, space, 0);
		if (n == 0) {
			if (got > 0)return got;
			return -2;
		}
		else if (n < 0) {
			//内核缓冲区排空
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				//正常情况下到这里是缓冲区发完了，返回前面已经发送的数据量
				//但是有可能一开始内核缓冲区就是空的，这里got为0
				return got;
			}
			else if (errno == EINTR) {
				//系统打断，重试
				continue;
			}
			else {
				//错误处理
				perror("recv");
				return -1;
			}
		}
		else if (n > 0) {
			conn->rclen += n;
			got += n;
		}
	}
}

//将一条数据完整的搬进conn，并进行指针偏移
void msg_cpy(char* buf, int length, Conn* conn) {
	//如果连接的用户态缓冲区不足，就直接放弃这个包,执行静默丢包
	//不可以说能塞多少塞多少，这会导致没塞下的包被丢失后，客户端协议永远没办法解析完整这个包，实际上导致解析错乱，把别的数据包的消息头拿去解析了
	if (SDBUF_SIZE - conn->sdlen < length) {
		printf("Client:%d space no enough,drop packet", conn->fd);
		return;
	}

	memcpy(conn->sdbuf + conn->sdlen, buf, length);
	conn->sdlen += length;
}

//发送：三种结果，内核缓冲区满send终止，sdbuf完全发送，或者两种同时发生
//三种返回值代表三种情况
//1.返回0，代表发送的数据为0
//2.返回>0，代表有数据正常发送，但不一定是将sdbuf里所有东西都发送了，不代表发送了一个完整的数据包
//  这种情况要么就是发送缓冲区已满，要么就是sdbuf被清空了
//3.返回-1，代表出错
int send_clear(Conn* conn) {
	//fd==-1其实应该由上一层判断，这里为防御性设计
	if (conn->fd == -1)return 0;
	if (conn->sdlen == 0)return 0;
	int sent = 0;
	while (conn->sdlen > sent) {
		int n = send(conn->fd, conn->sdbuf + sent, conn->sdlen - sent, MSG_NOSIGNAL);
		if (n < 0) {
			//内核缓冲区满了，已经尽力发送
			//内核缓冲区在可能第一次调用send之前就满了，导致send_clear直接触发EAGAIN直接返回为0的sent
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				conn->sdlen -= sent;
				//发送后不仅要调整sdlen指针，还要消除数据
				memmove(conn->sdbuf, conn->sdbuf + sent, conn->sdlen);
				return sent;
			}
			//系统打断应该继续尝试
			else if (errno == EINTR) {
				continue;
			}
			else {
				//其他情况
				//此处并不应该clean，应该交给上级
				//消费掉出错之前发送的数据，虽然没什么必要，因为一出错，就挂断，数据已经没有再次使用的可能性了
				conn->sdlen -= sent;
				memmove(conn->sdbuf, conn->sdbuf + sent, conn->sdlen);
				return -1;
			}
		}
		//在send里并不代表关闭
		else if (n == 0) {
			return -1;
		}

		sent += n;
	}
	//触发conn->sdlen == sent，即所有的数据都被发出，此时内核缓冲区可能没满（循环直接结束了，没再次调用send就不知道什么情况）
	//这里无须调整sdbuf，指针自0开始，新数据会覆盖旧数据
	conn->sdlen -= sent;
	return sent;
}

int flush(Conn* conn) {
	//返回-1代表这个链接已经关闭，不可再用，上级程序应该采取其他操作
	if (conn->fd == -1)return -1;
	int sd = send_clear(conn);
	if (sd < 0) {
		perror("send");
		//返回-1代表这个链接已经关闭，不可再用，上级程序应该采取其他操作
		return -1;
	}
	struct epoll_event ev;
	ev.data.ptr = conn;

	//不能直接sd==0判断，sd只代表发送了多少数据，发送了0个数据并不代表没数据可发送
	//也可能代表内核缓冲区在第一次调用send之前就满了，导致send_clear直接触发EAGAIN直接返回0
	if (conn->sdlen == 0) {
		//缓冲区为空，恢复可接收状态，避免LT模式下epoll_wait一直返回该连接可用
		ev.events = EPOLLIN ;
	}
	else if (conn->sdlen > 0) {

		//sdbuf用户态缓冲区还有数据没办法发出去，就注册可发送状态
		ev.events = EPOLLIN | EPOLLOUT ;
	}
	epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev);
	//返回1代表正常
	return 1;
}
//服务器特供回信,函数签名为[连接][Type][char*数据][数据长度]，函数内部会根据这些信息自动构成数据包并发送
//返回1为执行成功，-1为致命失败
int notice(Conn* conn, uint8_t type, const char* buf, int Len) {
	//检查
	if (Len > SDBUF_SIZE - conn->sdlen - 5) {
		printf("Unable to notice: conn's send space no enough");
		return 1;//这里的1不代表完成任务，只代表运行正常，但确实是无法达到结果
	}

	char* p = conn->sdbuf;
	p += conn->sdlen;

	//构造消息头
	uint32_t len = htonl(Len);
	memcpy(p, &len, sizeof(len));
	p += sizeof(len);

	//注册消息类型
	*p = type;
	p += 1;

	//注入消息体
	memcpy(p, buf, Len);
	conn->sdlen += sizeof(len) + 1 + Len;

	//发送
	int n = flush(conn);
	if (n == -1)return -1;
	return 1;
}


int main(int argc,char*argv[]) {
	/*
	char* dir = "stress_test_result";
	char path[300];
	snprintf(path, sizeof(path), "./%s/%s", dir, argv[1]);
	int result_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	*/

	//fd=-1来表示conns的空位，status为生命和业务状态
	for (int i = 0; i < MAX_CLIENTS; i++) {
		conns[i].sdlen = 0;
		conns[i].rclen = 0;
		conns[i].status = STA_UNCONNECT;
		conns[i].fd = -1;
	}

	epfd = epoll_create(1);

	int client_number = atoi(argv[1]);

	//创建指定数目的连接
	for (int i = 0; i < client_number; i++) {

		//创建一个TCP连接，IPV4地址族，端口8888，本地回环IP地址
		client_fd = socket(AF_INET, SOCK_STREAM, 0);
		struct sockaddr_in addr;
		addr.sin_family = AF_INET;
		addr.sin_port = htons(8888);
		inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

		//连接TCP
		if (connect(client_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
			perror("connect");
			return 1;
		}

		//放进大名单，每个连接有独自的数据管理
		Conn* new_conn = conn_insert(client_fd);
		if (new_conn == NULL) {
			printf("Conns full");
			return 0;
		}

		//设置非阻塞
		int flag = fcntl(client_fd, F_GETFL, 0);
		fcntl(client_fd, F_SETFL, flag | O_NONBLOCK);

		//为这个连接创建EPOLL实例
		struct epoll_event  client_ev;
		client_ev.data.ptr = new_conn;
		client_ev.events = EPOLLIN;
		//放入epoll
		epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);
	}

	struct epoll_event events[client_number];

	while (1) {
		int n = epoll_wait(epfd, events, client_number, -1);
		if (n < 0) {
			if (errno == EINTR)continue;
			perror("epoll_wait");
			break;
		}
		for (int i = 0; i < n; i++) {
			Conn* conn = events[i].data.ptr;
			if (events[i].events & EPOLLIN) {
				int x = recv_clear(conn);
				if (x == -1) {
					printf("error");
					return 0;
				}
				if (x == -2) {
					printf("Server close");
					return 0;
				}

				//是否能读取出一个头
				while (conn->rclen >= 5) {

					//获得数据头
					uint32_t len;
					memcpy(&len, conn->rcbuf, 4);
					len = ntohl(len);

					//是否有一个完整的数据包
					if (conn->rclen - 5 < len)break;

					//消费数据头
					conn->rclen -= 4;
					memmove(conn->rcbuf, conn->rcbuf + 4, conn->rclen);

					//获取类型
					uint8_t type;
					memcpy(&type, conn->rcbuf, 1);
					//消费类型
					conn->rclen -= 1;
					memmove(conn->rcbuf, conn->rcbuf + 1, conn->rclen);

					if (type == MSG_REGIS) {
						conn->status = STA_UNREADY;
						printf("%.*s\n", len, conn->rcbuf);
						//自动登录
						char* name = "Beth";
						char finalname[900];
						snprintf(finalname, sizeof(finalname), "%s%d", name, conn->fd);
						notice(conn, MSG_REGIS, finalname, strlen(finalname));
					}
					else if (type == MSG_RECOGISE) {
						conn->status = STA_READY;
						printf("%.*s\n", len, conn->rcbuf);
						connect_number++;

					}
					else if (type == MSG_NOTICE) {
						printf("%.*s\n", conn->rclen, conn->rcbuf);
					}
					else if (type == MSG_CHAT) {
						/*uint32_t name_len;
						memcpy(&name_len, conn->rcbuf, 4);
						name_len = ntohl(name_len);

						char result[900];

						uint64_t time;
						memcpy(&time, conn->rcbuf + len - 8, 8);
						time = be64toh(time);
						uint64_t curr_time = current_us();
						uint64_t distant_time = curr_time - time;

						int lenght = snprintf(result, sizeof(result), "%.*s:%d\n", name_len,conn->rcbuf + 4, (int)distant_time);

						write(result_fd, result, lenght);
						*/
						uint32_t name_len;
						memcpy(&name_len, conn->rcbuf, 4);
						name_len = ntohl(name_len);
						printf("%.*s:%.*s\n", name_len, conn -> rcbuf + 4, len - 4 - name_len, conn->rcbuf + 4 + name_len);
					}
					//消费消息
					conn->rclen -= len;
					memmove(conn->rcbuf, conn->rcbuf + len, conn->rclen);
				}
			}
			if (events[i].events & EPOLLOUT) {
				int x = flush(conn);
				if (x < 0) {
					printf("Server disconnect\n");
					return 0;
				}
			}
		}


		
		/*if (connect_number==atoi(argv[1])) {
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
		}*/
	}
}