#include<stdio.h>
#include<unistd.h>
#include<sys/epoll.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<string.h>
#include<errno.h>
#include<fcntl.h>
#include<stdlib.h>
#include<signal.h>
/*
* 协议头：消息体的长度，通过uint32储存，用n-h转型
* 消息体：具体的数据包
* 消息头与消息体是一体数据包
* 协议层应该在先读取一次协议头，再按协议头读取协议体
* 必须在读取到消息头要求的消息体，才能进行广播，这期间消息头和消息体会被存在conn
*/

#define MAX_EVENTS 128
#define MAX_CLIENTS 1024
#define MAX_LEN 1024
int epfd;

//结构体设计rclen和sdlen是显式设计，因为不太好给二进制数据包设定哨兵，这种做法能使得更安全，代码编写更方便
typedef struct Conn {
	int fd;
	//接收缓冲区
	char rcbuf[1024];
	//接收缓冲区的数据字节长度
	int rclen;
	//发送缓冲区
	char sdbuf[1024];
	//发送缓冲区的长度
	int sdlen;
}Conn;

Conn conns[1024];
//非阻塞IO设置
void set_nonblock(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}
//清理
void clean(Conn* conn) {
	//先除名，再关闭，防止fd被复用但又被除名
	epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, NULL);
	int fd = conn->fd;
	conn->fd = -1;
	conn->rclen = 0;
	conn->sdlen = 0;
	close(fd);
	printf("Client:%d is disconnect\n", fd);
}
//返回值为-1的情况代表错误和客户端关闭，这两种都需要进行clean，所以被归为一类
//正常情况返回值为rclen的长度
int recv_clear(Conn* conn) {
	while (1) {
		int space = MAX_LEN - conn->rclen;
		if (space == 0) {
			return conn->rclen;
		}
		int n = recv(conn->fd, conn->rcbuf + conn->rclen, space, 0);
		if (n == 0)return -1;
		else if (n < 0) {
			if (errno == EAGAIN ) {
				return conn->rclen;
			}
			else if (errno == EINTR) {
				continue;
			}
			else {
				perror("recv");
				return -1;
			}
		}
		else if (n > 0) {
			conn->rclen += n;
		}
	}
}

//消息消费,将c1缓冲区内的一条数据完整的搬进c2，并进行指针偏移
void msg_cpy(Conn* c1, Conn* c2, int len) {
	//谨记不是每个数组都是空的，可能有旧数据存留
	memcpy(c2->sdbuf + c2->sdlen, c1->rcbuf, len);
	c2->sdlen += len;

	//可以留一个日志打印fprint/write
	//存在问题：conn2缓冲区sdbuf不足处理
}

//发送：两种情况，内核缓冲区满send终止&&sdbuf完全发送
//三种返回值代表三种情况
//1.返回0，代表可发送的数据为0
//2.返回>0，代表有数据正常发送，但不一定是将sdbuf里所有东西都发送了，不代表发送了一个完整的数据包
//2.1这种情况要么就是发送缓冲区已满，要么就是sdbuf被清空了
//3.返回-1，代表出错
int send_clear(Conn* conn) {
	//fd==-1其实应该由上一层判断，这里为防御性设计
	if (conn->fd == -1)return 0;
	if (conn->sdlen == 0)return 0;
	int sent = 0;
	while (conn->sdlen > sent) {
		int n = send(conn->fd, conn->sdbuf+sent, conn->sdlen-sent, MSG_NOSIGNAL);
		if (n <= 0) {
			if (errno == EAGAIN) {
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
				//其他情况，包括n==0，在send里并不代表客户端关闭，是真的error
				//此处并不应该clean，应该交给上级
				conn->sdlen -= sent;
				memmove(conn->sdbuf, conn->sdbuf + sent, conn->sdlen);
				return -1;
			}
		}

		sent += n;
	}
	//这里无须调整sdbuf，指针自0开始，新数据会覆盖旧数据
	conn->sdlen -= sent;
	return sent;
}
//当broadcast的时候调用，用来尝试发送数据，如果发送不完，而唯一发不完的正常情况就是内核发送缓冲区满了
//当main调用的时候，就是复核，这时候的发送的内核缓冲区可用，再次尝试发送
void flush(Conn* conn) {
	if (conn->fd == -1)return;
	int sd = send_clear(conn);
	if (sd <= 0) {
		perror("send");
		clean(conn);
		return;
	}
	struct epoll_event ev;
	ev.data.fd = conn->fd;
	if (conn->sdlen > 0) {
		ev.events = EPOLLIN | EPOLLOUT;
	}
	else {
		ev.events = EPOLLIN;
	}
	epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev);
}

//广播
//将conn拷贝到每个可用连接的sdbuf上，再让他们进行发送
//发送完成后，清理原conn的rcbuf进行排空
void broadcast(Conn* conn,int len) {
	//将conn的消息拷贝到各个客户端的缓冲区上
	for (int i = 0; i < MAX_CLIENTS; i++) {
		if (conns[i].fd != -1&&conns[i].fd!=conn->fd) {	
			//如果send的用户态缓冲区不足，那就拜拜了
			if (1024 - conns[i].sdlen < len) {
				clean(&conns[i]);
				continue;
			}
			//按照长度拷贝之后，进行传输
			msg_cpy(conn, &conns[i], len);

			flush(&conns[i]);
		}
	}
	//广播完毕，原消息缓冲区清理
	printf("%d:%.*s\n", conn->fd, len - 4, conn->rcbuf + 4);
	memmove(conn->rcbuf, conn->rcbuf + len, conn->rclen - len);
	conn->rclen -= len;
}

//协议解析
//recv和send板块只负责尽可能从缓冲区拿出和放入数据，而拿出数据是不是一条数据包，发送的是不是一条数据包他们不处理
//协议解析层负责：
//判断rcbuf是否有一个完整的数据包，有的话就把他拿出来，放给各个客户端，要求他们发送

void handler(Conn* conn) {
	int n = recv_clear(conn);
	if (n == -1) {		//连接失败或关闭
		clean(conn);
		return;
	}
	//else if (conn->rclen < 4 && conn->rclen>0) {		//半个头
	//	return;
	//}
	while(conn->rclen >= 4) {		//头完整，再进入消息体判断
		uint32_t net_len;

		//将rcbuf里取出头4个字节放进net_len
		memcpy(&net_len, conn->rcbuf, 4);
		uint32_t len = ntohl(net_len);

		if (len > MAX_LEN - 4) {
			printf("Invalid package!\n");
			clean(conn);
			break;
		}

		//判断消息体长度是否为消息头要求的长度
		if (conn->rclen >= len + 4) {
			broadcast(conn, len + 4);
		}

		else if (conn->rclen < len + 4) {
		//非完整数据包
		break;
		}
	}
}

int main() {
	//遇到信号EPIPE就忽略
	signal(SIGPIPE, SIG_IGN);
	//创建服务器文件描述符，IPV4,TCP协议
	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	//创建IP,IPV4，端口8888（转网络短型），监视所有网口
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(8888);
	addr.sin_addr.s_addr = INADDR_ANY;
	//绑定IP与服务器接口
	if ((bind(server_fd, (struct sockaddr*)&addr, sizeof(addr))) < 0) {
		perror("bind");
		return 1;
	}
	//建立监听，accept等待队列128
	if ((listen(server_fd, 128)) < 0) {
		perror("listen");
		return 1;
	}

	//epoll TL建立
	epfd = epoll_create(1);
	//初始化一个epoll实例，用来将服务器接听口放进epoll
	struct epoll_event ev;
	ev.data.fd = server_fd;
	ev.events = EPOLLIN;
	epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);
	//创建事件列表
	struct epoll_event events[MAX_EVENTS];
	//连接名单，初始化
	for (int i = 0; i < MAX_CLIENTS; i++) {
		conns[i].fd = -1;
		conns[i].sdlen = 0;
		conns[i].rclen = 0;
	}
	printf("Server Online\n");
	while (1) {
		int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
		if (n < 0) {
			if (errno == EINTR)continue;
			perror("epoll_wait");
			continue;
		}
		for (int i = 0; i < n; i++) {
			int fd = events[i].data.fd;
			//有新连接
			if (fd == server_fd) {
				int client_fd = accept(server_fd, NULL, NULL);
				if (client_fd < 0) {
					perror("accept");
					continue;
				}
				//放入epoll
				struct epoll_event cli;
				cli.data.fd = client_fd;
				cli.events = EPOLLIN;
				epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cli);
				//放入名单，初始化，清除可能的旧数据
				conns[client_fd].fd = client_fd;
				conns[client_fd].rclen = 0;
				conns[client_fd].sdlen = 0;
				//设置非阻塞IO
				set_nonblock(client_fd);
				//新连接就绪
				printf("Client:%d connected\n", client_fd);
			}
			else {
				if (events[i].events & EPOLLIN) {
					handler(&conns[fd]);
				}
				if (events[i].events & EPOLLOUT) {
					flush(&conns[fd]);
				}
			}
		}
	}
	//暂未设定如何关闭服务器
	printf("Server offline\n");
	close(server_fd);
	return 0;
}