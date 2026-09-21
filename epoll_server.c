#include<stdio.h>
#include<unistd.h>
#include<sys/epoll.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<string.h>
#include<errno.h>
typedef struct Conn {
	int fd;
	char int_buf[1024];
	int in_len;
	char out_buf[1024];
	int out_len;
}Conn;
#define MAX_CLIENTS 128
int main() {
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
	int epfd = epoll_create(0);
	//初始化一个epoll实例，用来将服务器接听口放进epoll
	struct epoll_event ev;
	ev.data.fd = server_fd;
	ev.event = EPOLLIN;
	epoll_ctl = (epfd, EPOLL_CTL_ADD, server_fd, &ev);
	//创建事件列表
	struct epoll_event events[MAX_EVENTS];
	Conn conn[1024];
	while (1) {
		int n = epoll_wait(epfd, MAX_CLIENTS, -1);
		for (int i = 0; i < n; i++) {
			int fd = events[i].data.fd;
			//有新连接
			if (fd == server_fd) {
				int client_fd = accept(server_fd, NULL, NULL);
				if (client_fd < 0) {
					if (errno == EINR)continue;
					close(client_fd);
					perror("accept")continue;
				}
			}
			struct epoll_event cli;
			cli.data.fd = client_fd;
			cli.event = EPOLLIN;
			epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cli);
			conn[i].fd = client_fd;
		}
	}
}