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
int epfd = -1;

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
//清理模块，解耦设计
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
//缓冲区完整读取模块
int recv_clear(Conn* conn) {
	while (1) {
		int n = recv(conn->fd, conn->rcbuf + conn->rclen, 1023 - conn->rclen, 0);
		if (n < 0) {
			if (errno == EAGAIN) {
				//正常退出出口
				return conn->rclen;
			}
			//坏了
			else return -1;
		}
		//客户端关闭
		else if (n == 0)return 0;
		//正常读取到数据
		else if (n > 0)conn->rclen += n;
	}
}
//现在留有一个问题，我在想遇到问题的时候，是当场解决问题，还是返回上级调用交还
//就比如这个clean，我是应该handler解决，还是recv的时候就解决


//void msg_cpy(Conn* conn,int mes_len) {
//	int len = 4 + mes_len;
//	for (int i = 0; i < MAX_CLIENTS; i++) {
//		//确认不是空位，且不是发消息来的客户端
//		if (conns[i].fd != -1 && conns[i].fd != conn->fd) {
//			//谨记不是每个数组都是空的，可能有旧数据存留
//			memcpy(conns[i].sdbuf+conns[i].sdlen, conn->rcbuf, len);
//			conns[i].sdlen += len;
//		}
//	}
//	int remain = conn->rclen - len;
//	memmove(conn->rcbuf, conn->rcbuf + len, remain);
//	conn->rclen -= len;
//}

//消息消费,将c1缓冲区内的一条数据完整的搬进c2，并进行指针偏移
void msg_cpy(Conn* c1, Conn* c2, int len) {
	//谨记不是每个数组都是空的，可能有旧数据存留
	memcpy(c2->sdbuf + c2->sdlen, c1->rcbuf, len);
	c2->sdlen += len;

	//可以留一个日志打印fprint/write
	//存在问题：conn2缓冲区sdbuf不足处理
}
//广播
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

			int n = send(conns[i].fd, conns[i].sdbuf, conns[i].sdlen, MSG_NOSIGNAL);
			if (n < 0) {
				//如果EAGAIN就代表内存缓冲区满了，如果是EINTR就是被系统打断,就以后再来吧
				if (errno == EAGAIN || errno == EINTR)continue;
				printf("send error on fd:%d error:%s\n", conns[i].fd, strerror(errno));
				clean(&conns[i]);
				continue;
			}
			memmove(conns[i].sdbuf, conns[i].sdbuf + n, conns[i].sdlen - n);
			conns[i].sdlen -= n;
		}
	}
	//广播完毕，原消息缓冲区清理
	memmove(conn->rcbuf, conn->rcbuf + len, conn->rclen - len);
	conn->rclen -= len;
}

//啥都干模块
void handler(Conn* conn) {
	int n = recv_clear(conn);
	if (n == -1 ) {		//连接失败，直接下一个
		perror("recv");
		clean(conn);
		break;
	}
	
	else if (n == 0) {		//连接关闭
		clean(conn);
		break;
	}
	else if (n < 4&&n>0) {		//半个头
		//进入不完整情况处理，待完善
	}
	else if (n >= 4) {		//头完整，再进入消息体判断
		uint32_t net_len;
		//将rcbuf里取出头4个字节放进net_len
		memcpy(&net_len, conn->rcbuf, 4);
		uint32_t len = ntohl(net_len);
		//判断消息体长度是否为消息头要求的长度
		if (conn->rclen >= len + 4) {
			broadcast(conn,len+4);
		}
		else if (conn->rclen < len + 4) {
			//不完整情况处理，待完善
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
	epfd = epoll_create(0);
	//初始化一个epoll实例，用来将服务器接听口放进epoll
	struct epoll_event ev;
	ev.data.fd = server_fd;
	ev.event = EPOLLIN;
	epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);
	//创建事件列表
	struct epoll_event events[MAX_EVENTS];
	//连接名单
	for (int i = 0; i < MAX_CLIENTS; i++) {
		conns[i].fd = -1;
	}

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
					close(cline_fd);
					perror("accept");
					continue;
				}
				//放入epoll
				struct epoll_event cli;
				cli.data.fd = client_fd;
				cli.event = EPOLLIN;
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
				//拿到数据，一次性获取缓冲区全部的数据，协议解析判断，尝试发送，可能再次发送
			}
		}
	}
}