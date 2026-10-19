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
#include<stdint.h>
#include "log.h"
/*
协议定义：

数据包的类型有所不同，但所有数据包都使用固定的统一格式:
[数据头][数据类型][数据体]

[数据头]数据头Header，uint32_t的4字节32位二进制，用来表示数据体的长度，所以又称“数据体长度”
[数据类型]数据类型Type，uint_t的1字节8位二进制，用来表示数据体的类型
[数据体]数据体是有效数据，他根据不同的type有不同的结构，需要做不同的操作

简单来说数据头是讲数据体有多长的，数据类型是讲数据是哪一类的、用来干什么的，数据体就是要被拿来使用的数据本身
但数据体实际上内部结构也有所不同，需要根据不同的类型进行其他的操作，可能还需要进一步解析等等

几种不同的Type对应的数据体结构：

	MSG_CHAT:
		服务器接收的MSG_CHAT类型[数据体]:
		[文本消息]
		服务器发送的MSG_CHAT类型[数据体]:
		[用户名长度][用户名][文本消息]
		[用户名长度]和[用户名]在客户端初始链接时通过一系列操作建立后保存在服务器内，发送时通过服务器内部代码加工进发送的数据包
	
	MSG_NOTICE:
		MSG_NOTICE类型是服务器专用回信类型，专门用来给客户端发送提示用的
		MSG_NOTICE的[数据体]:
		[文本消息]
	
	MSG_REGIS:
		MSG_REGIS类型是新客户端用于登录时的类型，同时被服务器与客户端使用
		MSG_REGIS类型的[数据体]:
		[文本消息]

	MSG_RECOGISE:
		用于回应客户端确认其完成登录，进入可用状态
		MSG_RECOGISE类型的[数据体];
		[文本消息]
*/

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

int epfd;

#define STA_ERROR -1
#define STA_DISCONNECT 0
#define STA_UNREADY 1
#define STA_READY 2

//结构体设计rclen和sdlen是显式设计，因为不太好给二进制数据包设定哨兵，这种做法能使得更安全，代码编写更方便
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
	//客户端用名
	char name[MAX_NAME];
	//名字长度
	uint32_t name_len;
	//连接状态
	int status;		//-1故障  0未启用  1已连接	2已可完成登录，可启用
}Conn;

//已连接客户端
Conn conns[MAX_CLIENTS];

//失效列表
//失效链接处理机制：
//无论是什么链接失效了，都要放进这个列表里，等待事件响应轮结束后统一管理
//链接失效分两种情况：1.链接正常断开   2.链接异常
//在处理失效链接列表时会根据对应链接的情况进行不同的报告，但处理目前还是相同的
Conn* fail_list[MAX_CLIENTS];

int notice(Conn* conn, uint8_t type, const char* buf, int len);
int packet_dispatch(Conn* conn);

//非阻塞IO设置
void set_nonblock(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

//先在整个conns名单遍历一遍，找到空位后将这个连接初始化，再放入，这个放入的位置idx几乎是随机，只是按顺序找空位，他所处的位置不再代表任何含义
Conn* conn_insert(int fd) {
	for (int i = 0; i < MAX_CLIENTS; i++) {
		if (conns[i].fd == -1) {
			conns[i].fd = fd;
			conns[i].rclen = 0;
			conns[i].sdlen = 0;
			memcpy(conns[i].name, "unknown", 7);
			conns[i].name_len = 7;
			conns[i].status = STA_UNREADY;
			return &conns[i];
		}
	}
	return NULL;
}

//失效链接入队
void report(Conn* conn) {
	for (int i = 0; i < MAX_CLIENTS; i++) {
		//检查这个位置是否有空，如果非空的话就检查是否是conn
		if (fail_list[i] != NULL) {
			if (fail_list[i]->fd == conn->fd)break;
		}
		//该链接是否为空位
		if (fail_list[i] == NULL){
			fail_list[i] = conn;
			LOG(LOG_DEBUG, conn->fd, "Client:%.*s has been push into fail_list", conn->name_len, conn->name);
			break;
		}
	}
}

//清理
void clean(Conn* conn) {
	if (conn->fd == -1) {
		LOG(LOG_INFO, conn->fd, "Can't close fd:-1");
		return;
	}
	//先除名，再关闭，防止fd被复用但又被除名
	LOG(LOG_INFO, conn->fd, "Client:%.*s disconnect,status %d -> STA_DISCONNECT", conn->name_len, conn->name,conn->status);
	epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, NULL);
	int fd = conn->fd;
	conn->fd = -1;
	conn->rclen = 0;
	conn->sdlen = 0;
	conn->status = STA_DISCONNECT;
	memcpy(conn->name, "unknown", 7);
	conn->name_len = 7;
	close(fd);
}
//返回值为-1的情况代表错误和客户端关闭，这两种都需要进行处理，所以被归为一类
//正常情况返回值为got
//recv_clear只负责一件事，就是尽可能把rcbuf写满，至于内核缓冲区是否排空，无所谓，内核缓冲区有残留的话下次epoll_wait会再次触发
//连接正常读取：返回值>=0
//连接关闭：返回值=-2
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
			//当客户端发完消息并直接关闭时，客户端发送的FIN可能会紧跟在上一条消息的后面
			//而我们在while循环读取，读取完最后一条消息时got能正常获取消息，
			//但在收到FIN后TCP状态就已经变成EOF
			//下一轮循环会读取到n==0，直接触发return -2，关闭连接，而最后一条消息还没消费
			//在这里加一层判断，如果还有消息没处理，socket的EOF状态我们读取到了但暂不处理，虽然我们前面recv了n==0，但关闭是一种状态，不会因为前面读取了n==0，状态就消失
			//而这个EOF状态会继续触发epoll_wait，在下一轮epoll被我们收到，这依赖LT模式
			if (got > 0)return got;
			return -2; 
		}
		else if (n < 0) {
			//内核缓冲区排空
			if (errno == EAGAIN||errno == EWOULDBLOCK ) {
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
void msg_cpy(char*buf, int length, Conn* conn) {
	//如果连接的用户态缓冲区不足，就直接放弃这个包,执行静默丢包
	//不可以说能塞多少塞多少，这会导致没塞下的包被丢失后，客户端协议永远没办法解析完整这个包，实际上导致解析错乱，把别的数据包的消息头拿去解析了
	if (SDBUF_SIZE - conn->sdlen < length) {
		LOG(LOG_WARN,conn->fd,"Client:%.*s space no enough,drop packet", conn->name_len, conn->name);
		return;
	}

	//谨记不是每个数组都是空的，可能有旧数据存留
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
		int n = send(conn->fd, conn->sdbuf+sent, conn->sdlen-sent, MSG_NOSIGNAL);
		if (n < 0) {
			//内核缓冲区满了，已经尽力发送
			//内核缓冲区在可能第一次调用send之前就满了，导致send_clear直接触发EAGAIN直接返回为0的sent
			if (errno == EAGAIN||errno==EWOULDBLOCK) {
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
		//在send里并不代表客户端关闭
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
//当broadcast的时候调用，用来尝试发送数据，如果发送不完，而唯一发不完的正常情况就是内核发送缓冲区满了导致sdbuf堆积严重
//当main调用的时候，就是复核，这时候的发送的内核缓冲区可用，再次尝试发送
int flush(Conn* conn) {
	//返回-1代表这个链接已经关闭，不可再用，上级程序应该采取其他操作
	if (conn->fd == -1)return -1;
	if (conn->status == STA_ERROR || conn->status == STA_DISCONNECT)return -1;
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
		ev.events = EPOLLIN | EPOLLRDHUP;
	}
	else if(conn->sdlen>0){

		//sdbuf用户态缓冲区还有数据没办法发出去，就注册可发送状态
		ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;
	}
	epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev);
	//返回1代表正常
	return 1;
}

//广播
//遍历整个conns列表，如果不是空的，也不是消息发送者本人，就进行发送，用flush能更好的发送（使用send清理sdbuf，还可以进行状态注册）
void broadcast(int fd,char*buf,int length){
	for (int i = 0; i < MAX_CLIENTS; i++) {
		//检查链接状态
		if (conns[i].status == STA_READY) {
			//检查槽位是否有链接
			if (conns[i].fd != -1 && conns[i].fd != fd) {
				msg_cpy(buf, length, &conns[i]);
				int n = flush(&conns[i]);
				if (n == -1) {
					conns[i].status = STA_ERROR;
					report(&conns[i]);
				}
			}
		}
	}
}

//recv和send板块只负责尽可能从缓冲区拿出和放入数据，而拿出数据是不是一条数据包，发送的是不是一条数据包他们不处理

//handler是所有数据包的审查中心，他负责接管原数据包的全部问题
//先调用接收器执行连接的EPOLLIN响应请求，接受完数据就进入数据包审查
//审查阶段会对数据包进行解析，判断其是否为一个完整的数据包，数据包是否非法，是否为空包
//通过审查后，将数据包原封不动地交给packet_dispatch进行分拣，再由分拣将数据包交给各个功能模块
//在功能模块完成工作后，将处理结果上报回handler，除了handler之外，不允许任何来自客户端的数据包被擅自处理，而是统一归到handler处理
//注：部分由服务器构造的数据包会直发至客户端，不经过handler，这类数据包通常不需要通过消费来进行对内存空间的调整
void handler(Conn* conn) {
	//检查当前链接状态
	if (conn->status == STA_ERROR || conn->status == STA_DISCONNECT) {
		return;
	}

	int n = recv_clear(conn);
	if (n == -1) {		//连接失败
		LOG(LOG_ERROR, conn->fd, "Client:%.*s connection error,status %d -> STA_ERROR", conn->name_len, conn->name, conn->status);
		conn->status = STA_ERROR;
		report(conn);
		return;
	}
	else if (n == -2) {		//连接关闭
		LOG(LOG_INFO, conn->fd, "Client:%.*s disconnect,status %d -> STA_DISCONNECT",conn->name_len, conn->name, conn->status);
		conn->status = STA_DISCONNECT;
		report(conn);
		return;
	}
	while(conn->rclen >= 5) {		//头完整，再进入数据体判断
		uint32_t body_len;

		//将rcbuf里取出头4个字节放进body_len
		memcpy(&body_len, conn->rcbuf, 4);
		body_len = ntohl(body_len);

		//非法大包，直接关闭违规客户端
		if (body_len > MAX_PACKET - 5) {
			char* buf = "Your package is over size";
			//可不检测notice是否产生致命错误，因为最后都要关闭连接
			notice(conn, MSG_NOTICE, buf, strlen(buf));
			LOG(LOG_INFO, conn->fd, "Client:%.*s Invalid package, status %d -> STA_ERROR", conn->name_len, conn->name, conn->status);
			conn->status = STA_ERROR;
			report(conn);
			break;
		}

		//防止空包,空包直接清除这个数据包
		if (body_len == 0) {
			conn->rclen -= 5;
			memmove(conn->rcbuf, conn->rcbuf + 5, conn->rclen);
			LOG(LOG_INFO, conn->fd, "Client:%.*s Empty packet", conn->name_len, conn->name);
			continue;
		}

		//判断数据体长度是否为数据头要求的长度
		if (conn->rclen - 5 >= body_len) {
			int X = packet_dispatch(conn);
			//现将数据包执行结果分为两种，一种是正常运行，正常运行并不一定指数据包达到了他的目的，即使没办法达到目的也属于正常运行的结果，统一进入消费环节
			//一种是致命失败，致命失败是需要直接进入清理程序的
			if (X == -1) {
				//致命失败，直接关闭链接
				LOG(LOG_ERROR, conn->fd, "Client:%.*s fail,status %d -> STA_ERROR", conn->name_len, conn->name, conn->status);
				conn->status = STA_ERROR;
				report(conn);
				break;
			}
			else if (X == 1) {
				//正常运行，消费数据包
				conn->rclen -= 5 + body_len;
				memmove(conn->rcbuf, conn->rcbuf + body_len + 5, conn->rclen);
			}
		}

		//不完整数据包
		else if (conn->rclen - 5 < body_len) {
			break;
		}
	}
}

//数据包分拣中心，所有的数据在被确定为一个数据包后,链接conn会进入分拣中心，确认数据包类型，根据数据包类型进行不同操作
int packet_dispatch(Conn* conn) {
	//解析数据体长度
	uint32_t len;
	memcpy(&len, conn->rcbuf, sizeof(len));
	len = ntohl(len);
	//获取数据类型
	uint8_t type;
	memcpy(&type, conn->rcbuf + 4, 1);
	//分拣
	if (type == MSG_REGIS) {
		if (conn->status != STA_UNREADY) {//状态不符合消息管制，拒收丢弃
			LOG(LOG_INFO, conn->fd, "Client:%.*s send a packet out of status ,current status:%d ,drop the packet", conn->name_len, conn->name, conn->status);
			return 1;
		}
		//检查名字是否合规
		if (len > 32) {

			//回信不合规名字
			char* buf1 = "Your name is invalid,try again";
			int X = notice(conn, MSG_NOTICE, buf1,strlen(buf1));
			if (X == -1) {
				LOG(LOG_ERROR, conn->fd, "Notice error");
				return -1;
			}

			//再次要求注册
			char* buf2 = "Please enter your name:";
			X = notice(conn, MSG_REGIS, buf2,strlen(buf2));
			if (X == -1) {
				LOG(LOG_ERROR, conn->fd, "Notice error");
				return -1;
			}
			return 1;
		}
		//[len][MSG_REGIS][NAME]
		memcpy(conn->name, conn->rcbuf + 4 + 1, len);
		conn->name_len = len;
		LOG(LOG_DEBUG, conn->fd, "Client regis name:%.*s,status %d -> STA_READY", len, conn->rcbuf + 5, conn->status);
		conn->status = STA_READY;
		

		//回应确认登录状态
		char buf[MAX_NAME + 8];
		memcpy(buf, "Welcome ", 8);
		memcpy(buf + 8, conn->name, conn->name_len);
		notice(conn, MSG_RECOGISE, buf, conn->name_len + 8);
		return 1;
	}
	else if (type == MSG_CHAT) {
		if (conn->status != STA_READY) {//状态不符合消息管制，拒收丢弃
			LOG(LOG_INFO, conn->fd, "Client:%.*s send a packet out of status ,current status:%d ,drop the packet", conn->name_len, conn->name, conn->status);
			return 1;
		}
		//[len1][type][msg] --> [len2][type][name_len][name][msg]
		//length数据包全长 body_len拼装后数据体长度 len原数据包数据体长度
		
		//若为未注册，则不可发送消息，且消息要被消费
		if (conn->status != STA_READY) {
			return 1;
		}

		//计算新数据体长度
		uint32_t body_len = len + conn->name_len + sizeof(conn->name_len);
		int length = (int)body_len + 5;
		if (length > MAX_PACKET) {
			char* buf = "Your packet is to long,please send a shorter packet!";
			notice(conn, MSG_NOTICE, buf,strlen(buf));//不保证送达
			return 1;
		}
		body_len = htonl(body_len);

		//开始构造新数据包
		char buf[SDBUF_SIZE];
		char* p = buf;

		//制作新消息头
		memcpy(p, &body_len, sizeof(body_len));
		p += sizeof(body_len);

		//放入type
		*p = MSG_CHAT;
		p += 1;

		//放入用户名长度（转大端序）
		uint32_t namelen = htonl(conn->name_len);
		memcpy(p, &namelen, sizeof(namelen));
		p += sizeof(namelen);

		//放入用户名
		memcpy(p, conn->name, conn->name_len);
		p += conn->name_len;

		//放入消息
		memcpy(p, conn->rcbuf + 5, len);
		p += len;

		LOG(LOG_DEBUG,conn->fd,"%.*s:%.*s", conn->name_len, buf + 9, ntohl(body_len) - 4 - conn->name_len, buf + 4 + 1 + 4 + conn->name_len);
		broadcast(conn->fd, buf, length);

		return 1;
	}
	//不明类型数据包，直接挂断链接
	else {
		char* buf = "Your packet is unclear";
		notice(conn, MSG_NOTICE, buf,strlen(buf));
		LOG(LOG_WARN, conn->fd, "Client:%.*s Unknown packet type",conn->name_len,conn->name);
		//无论如何都要clean，notice返回值可以不必检查
		return -1;
	}
}

//服务器特供回信,函数签名为[连接][Type][char*数据][数据长度]，函数内部会根据这些信息自动构成数据包并发送
//返回1为执行成功，-1为致命失败
int notice(Conn* conn, uint8_t type, const char* buf, int Len){
	//检查
	if (Len > SDBUF_SIZE - conn->sdlen - 5) {
		LOG(LOG_WARN, conn->fd, "Unable to notice: conn's send space no enough");
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

void accept_client(Conn* conn) {
	while (1) {
		int client_fd = accept(conn->fd, NULL, NULL);
		if (client_fd < 0) {
			//所有连接都已经连接了，缓冲区为空，触发EAGAIM或EWOULDBLOCK
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				break;
			}
			//被系统打断就重试
			if (errno == EINTR) {
				continue;
			}
			perror("accept");
			LOG(LOG_ERROR, client_fd, "accept error");
			break;
		}

		//创建新链接，初始化链接，并将新链接放入conns
		Conn* new_conn = conn_insert(client_fd);
		//如果连接的客户端满了，直接放弃剩下想要连接的客户端
		if (new_conn == NULL) {
			LOG(LOG_WARN, client_fd, "Connection is full,drop connection");
			close(client_fd);
			break;
		}

		//设置非阻塞IO
		set_nonblock(client_fd);

		//放入epoll
		struct epoll_event cli;
		cli.data.ptr = new_conn;
		cli.events = (EPOLLIN | EPOLLRDHUP);
		epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cli);

		//新连接就绪
		LOG(LOG_INFO, client_fd, "Client:%d connected,status STA_DISCONNECT -> %d", client_fd, new_conn -> status);

		//特供回信:要求注册
		char* buf = "Please enter your name:";
		int X = notice(new_conn, MSG_REGIS, buf,strlen(buf));
		if (X == -1) {
			LOG(LOG_ERROR, new_conn->fd, "Unable to notice,status %d -> STA_ERROR",new_conn->status);
			new_conn->status = -1;
			report(new_conn);
		}
	}
}

int main() {
	//遇到信号EPIPE就忽略
	signal(SIGPIPE, SIG_IGN);

	//设置日志等级
	log_set_level(LOG_DEBUG);

	//创建服务器文件描述符，IPV4,TCP协议
	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	//创建IP,IPV4，端口8888（转网络短型），监视所有网口
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(8888);
	addr.sin_addr.s_addr = INADDR_ANY;

	//启用快速重启
	int opt = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
		perror("setsockopt");
		LOG(LOG_ERROR, server_fd, "setsockopt error");
	}

	//绑定IP与服务器接口
	if ((bind(server_fd, (struct sockaddr*)&addr, sizeof(addr))) < 0) {
		perror("bind");
		LOG(LOG_ERROR, server_fd, "bind error");
		return 1;
	}
	//建立监听，accept等待队列128
	if ((listen(server_fd, 128)) < 0) {
		perror("listen");
		LOG(LOG_ERROR, server_fd, "listen error");
		return 1;
	}

	//连接名单，初始化
	for (int i = 0; i < MAX_CLIENTS; i++) {
		conns[i].fd = -1;
		conns[i].sdlen = 0;
		conns[i].rclen = 0;
		conns[i].status = STA_DISCONNECT;
	}

	for (int i = 0; i < MAX_CLIENTS; i++) {
		fail_list[i] = NULL;
	}

	//设置服务器接听链接非阻塞，可以一口气接听多个连线
	set_nonblock(server_fd);

	//创建一个独立的服务器链接
	Conn server_conn;
	server_conn.fd = server_fd;
	server_conn.rclen = 0;
	server_conn.sdlen = 0;
	server_conn.status = STA_DISCONNECT;

	//epoll TL建立
	epfd = epoll_create(1);

	//初始化一个epoll实例，用来将服务器接听口放进epoll
	struct epoll_event ev;

	//原先的实例event.data.fd改成event.data.ptr，将conn直接放入ptr，将conn整个数据结构作为实例的成员
	ev.data.ptr = &server_conn;
	ev.events = EPOLLIN;

	//将这个服务器实例放入epoll，当server_fd触发这个实例里要求的事件EPOLLIN的时候，将server_fd对应的实例ev放入events名单里
	epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

	//创建事件列表
	struct epoll_event events[MAX_EVENTS];
 
	LOG(LOG_INFO, server_fd, "Server Online");

	while (1) {
		int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
		if (n < 0) {
			if (errno == EINTR)continue;
			perror("epoll_wait");
			LOG(LOG_ERROR, server_fd, "epoll_wait error");
			continue;
		}
		for (int i = 0; i < n; i++) {
			Conn* conn = events[i].data.ptr;
			//新连接请求
			if (conn == &server_conn) {
				accept_client(conn);
			}
			//已连接客户端请求
			else {
				if (conn->fd == -1)continue;

				int e = events[i].events;
				//对端异常关闭
				if (e & (EPOLLERR | EPOLLHUP)) {
					//获取异常socket属性
					int err = 0;
					socklen_t elen = sizeof(err);
					getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &err, &elen);

					LOG(LOG_ERROR, conn->fd, "Client %d %s,status to STA_ERROR", conn->fd, err ? strerror(err) : "hang up");
					conn->status = STA_ERROR;
					report(conn);
				}
				//客户端发送消息
				if (e & EPOLLIN && conn->fd != -1)handler(conn);

				//向客户端发送消息
				if (e & EPOLLOUT && conn->fd != -1) {
					int x = flush(conn);
					if (x == -1) {
						LOG(LOG_ERROR, conn->fd, "Unable to flush,status %d -> STA_ERROR",conn->status);
						conn->status = STA_ERROR;
						report(conn);
					}
				}

				//客户端正常挂断（未触发EPOLLIN特殊情况兜底）
				//必须要添加非EPOLLIN情况判断，客户端发送Fin后系统会立刻上报EPOLLIN|EPOLLRDHUP
				//如果内核缓冲区的数据还没被读取完，而我们直接通过EPOLLRDHUP关闭链接，会导致内核缓冲区的数据丢失
				if (e & EPOLLRDHUP && conn->fd != -1 && !(e & EPOLLIN )) {
					LOG(LOG_INFO, conn->fd, "Client:Name:%.*s disconnect,status %d -> STA_DISCONNECT", conn->name_len, conn->name, conn->status);
					conn->status = STA_DISCONNECT;
					report(conn);
				}
			}
		}
		//事件响应轮结束，故障链接统一清理
		for (int i = 0; i < MAX_CLIENTS; i++) {
			if (fail_list[i] != NULL) {
				clean(fail_list[i]);
				fail_list[i] = NULL;
			}
			else if (fail_list[i] == NULL)break;
		}
	}
	//暂未设定如何关闭服务器
	LOG(LOG_INFO, server_fd, "Server offline\n");
	close(server_fd);
	return 0;
}

//计划

// 改进发送缓冲区不足时的处理机制，不应该再默认丢包
// 此改进可以解决notice不保证送达的问题，更从根源上解决此类“目的不一定达成，运行一定完成”的问题

//完善状态机，补充类型，宏定义，状态机机制为状态检测判别连接的权限范围，而不直接根据状态执行业务

// 增加半包链接的心跳程序
 
// 安排压力测试
 
// 优化sdbuf成双指针
 
// 增加日志

//思考
//我要怎样对齐服务器与客户端的协议