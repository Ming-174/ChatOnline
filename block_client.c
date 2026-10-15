#include<stdio.h>
#include<string.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<sys/socket.h>
#include<unistd.h>
#include<stdint.h>
#include<pthread.h>
#include<signal.h>

/*
客户端行为准则：
1.刚建立TCP连接	等待服务器响应消息
2.收到REGIS	填写注册消息，发送数据包，等待回应
3.收到notice	播报notice
4.收到recogis	可以进入发送消息
5.收到chat		播报chat
6.在2之后又收到regis（这里不区分是否收到notice，这是错误情况重试要求）		填写注册消息，发送数据包，等待回应
*/

#define MSG_REGIS 0
#define MSG_LOGIN 1
#define MSG_LOGOUT 2
#define MSG_CHAT 3
#define MSG_NOTICE 4
#define MSG_RECOGISE 5

#define MAX_PACKET 1024

#define STA_WAIT 0
#define STA_REGISING 1
#define STA_RECOGISE 2
#define STA_CLOSE -1

#define REQ_NONE -1
#define REQ_RECOGISE 0
#define REQ_REGIS 1

int status = STA_WAIT;
int request = REQ_NONE;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

//保证将buf的size字节发送
int write_exact(int fd, char* buf, size_t size) {
	ssize_t ret = 0;
	while (ret < size) {
		ssize_t n = send(fd, buf + ret, size - ret, 0);
		if (n <= 0)return -1;
		else if (n > 0)ret += n;
	}
	return ret;
}

/*int send_package(int fd, char* buf, int Len); {
	uint32_t len = htonl(Len);
	int r = write_exact(fd, (char*)&len, sizeof(len));
	if (r < 0)return -1;

	r = write_exact(fd, buf, Len);
	if (r < 0)return -1;
	return r;
}*/

//从内核缓冲区拿出size字节
int read_exact(int fd, char* buf, size_t size) {
	ssize_t got = 0;
	while (got < size) {
		ssize_t n = recv(fd, buf + got, size - got, 0);
		if (n > 0)got += n;
		else if (n == 0)return 0;
		else return -1;
	}
	return (int)got;
}
//所以我需要手动将网络格式转回来
//recv_packet会首先解析一次数据包，获得数据头并解析长度后，再根据数据头，将数据类型和数据体收取入buf内
int recv_package(int fd, char* buf) {
	uint32_t net_len;
	//读取4字节数据头
	int r = read_exact(fd, (char*)&net_len, sizeof(net_len));
	if (r <= 0)return r;

	//转码4字节数据头
	uint32_t len = ntohl(net_len);
	if (len > MAX_PACKET - 5)return -2;

	//读取数据类型和数据体
	r = read_exact(fd, buf, len + 1);
	if (r <= 0)return r;

	return r;
}

//收到的buf是这样的[Type][msg],不包含Header，Header[body_len][Type]是recv_package的返回值
void* handler(void* arg) {
	int client_fd = (int)(intptr_t)arg;
	while (1) {
		char buf[MAX_PACKET] = { 0 };
		int len = recv_package(client_fd, buf);

		if (len == 0) {
			printf("Server closed\n");
			break;
		}
		if (len == -1) {
			perror("read_exact ERROR");
			break;
		}
		if (len == -2) {
			printf("package is over size\n");
			break;
		}

		//收取到的是读取到的数据类型和数据体，为了方便使用我们直接先计算数据体的长度
		len -= 1;

		int type = buf[0];
		if (type == MSG_REGIS) {
			pthread_mutex_lock(&mutex);

			request=REQ_REGIS;
			printf("%.*s\n", len, buf + 1);
			pthread_cond_signal(&cond);
			pthread_mutex_unlock(&mutex);
		}
		//MSG_RECOGISE对客户端至关重要，但是服务器不保证此类消息送达，因为在缓冲区不足时会静默丢包
		else if (type == MSG_RECOGISE) {
			pthread_mutex_lock(&mutex);
			request = REQ_RECOGISE;
			pthread_cond_signal(&cond);
			pthread_mutex_unlock(&mutex);

			printf("%.*s\n", len, buf + 1);
		}
		else if (type == MSG_NOTICE) {
			printf("%.*s\n", len, buf + 1);
		}
		else if (type == MSG_CHAT) {
			//[type][name_len][name][message]
			uint32_t name_len;
			memcpy(&name_len, buf + 1, 4);
			name_len = ntohl(name_len);
			printf("%.*s:", name_len, buf + 5);
			printf("%.*s\n", len - name_len - 4, buf + 5 + name_len);
		}
	}
}

int regis(int client_fd) {
	char name[33];
	scanf("%32s", name);

	uint32_t length = htonl(strlen(name));
	char buf[MAX_PACKET];
	char* p = buf;

	memcpy(buf, &length, 4);
	p += 4;

	*p = MSG_REGIS;
	p += 1;

	memcpy(p, name, strlen(name));

	int w = write_exact(client_fd, buf, 5 + strlen(name));
	if (w < 0) {
		perror("Disconnect");
		return -1;
	}
	return 1;
}

//当前的运行机制：
//接收线程收到消息，根据消息有两种执行
//1.打印任务，直接执行
//2.请求任务，通过request与主线程交流
//主线程收到request后，根据数值，判断其任务类型
//1.一次性任务，直接调用模块执行
//2.长期任务，修改status，后续业务根据status运行
//其实也就两种工作，request->status->work && request->work

int main() {
	signal(SIGPIPE, SIG_IGN);

	int client_fd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(8888);
	inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

	connect(client_fd, (struct sockaddr*)&addr, sizeof(addr));

	pthread_t tid;
	pthread_create(&tid, NULL, &handler, (void*)(intptr_t)client_fd);
	pthread_detach(tid);

	//客户端在STA_REGISING阶段需要等待一次服务器发消息，只有在接到消息后才能进行下一步操作
	//在STA_RECOGISE则不需要等待
	while (status!=STA_CLOSE) {
		if (status == STA_WAIT) {
			int req;
			pthread_mutex_lock(&mutex);

			//不能单独依靠cond来判断状态，必须结合自己的状态
			while (request == REQ_NONE) {
				pthread_cond_wait(&cond, &mutex);
			}

			req = request;
			request = REQ_NONE;
			
			pthread_mutex_unlock(&mutex);

			if (req == REQ_REGIS) {
				int x = regis(client_fd);
				if (x == -1) {
					status = STA_CLOSE;
					break;
				}
			}
			else if (req == REQ_RECOGISE) {
				status = STA_RECOGISE;
			}
		}

		while (status == STA_RECOGISE) {
			char msg[33];
			fgets(msg, sizeof(msg), stdin);
			msg[strcspn(msg, "\n")] = '\0';

			uint32_t length = htonl(strlen(msg));
			char buf[MAX_PACKET];
			char* p = buf;

			memcpy(p, &length, 4);
			p += 4;

			*p= MSG_CHAT;
			p += 1;

			memcpy(p, msg, strlen(msg));

			int w = write_exact(client_fd, buf, 5 + strlen(msg));
			if (w < 0) {
				status = STA_CLOSE;
				break;
			}
		}

		//char Tell[MAX_PACKET - 5];
		//
		//Tell[strcspn(Tell, "\n")] = '\0';
		//int s = send_package(client_fd, Tell);
		//if (s < 0) {
		//	perror("Disconnect");
		//	return 1;
		//}
	}
}