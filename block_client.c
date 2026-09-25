#include<stdio.h>
#include<string.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<sys/socket.h>
#include<unistd.h>
#include<stdint.h>
#include<pthread.h>
#include<signal.h>
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
int write_exact(int fd, char* buf, size_t size) {
	ssize_t ret = 0;
	while (ret < size) {
		ssize_t n = send(fd, buf + ret, size - ret, 0);
		if (n <= 0)return -1;
		else if (n > 0)ret += n;
	}
	return ret;
}

int send_package(int fd, char* buf) {
	uint32_t len = htonl(strlen(buf));
	int r = write_exact(fd, (char*)&len, sizeof(len));
	if (r < 0)return -1;

	r = write_exact(fd, buf, strlen(buf));
	if (r < 0)return -1;
	return r;
}

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
//发送的时候，是将h转网络格式，read_exact并不会帮我转会来，只会照样读取
//所以我需要手动将网络格式转回来
int recv_package(int fd, char* buf, int max_len) {
	uint32_t net_len;
	int r = read_exact(fd, (char*)&net_len, sizeof(net_len));
	if (r <= 0)return r;

	uint32_t len = ntohl(net_len);
	if (len >= max_len)return -2;

	r = read_exact(fd, buf, len);
	if (r <= 0)return r;

	return r;
}

void* handler(void* arg) {
	int client_fd = (int)(intptr_t)arg;
	while (1) {
		char buf[1024] = { 0 };
		int len = recv_package(client_fd, buf, sizeof(buf));

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
		buf[len] = '\0';
		printf("%s\n", buf);
	}
}
int main() {
	signal(SIGPIPE, SIG_IGN);

	int client_fd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(8888);
	inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

	connect(client_fd, (struct sockaddr*)&addr, sizeof(addr));

	char name[32];
	//printf("Please enter your name:");
	//fgets(name, sizeof(name), stdin);
	//name[strcspn(name, "\n")] = '\0';
	//int s = send_package(client_fd, name);
	//if (s < 0) {
	//	perror("Disconnect");
	//	return 1;
//	}

	pthread_t tid;
	pthread_create(&tid, NULL, &handler, (void*)(intptr_t)client_fd);
	pthread_detach(tid);
	while (1) {
		char Tell[1024];
		fgets(Tell, sizeof(Tell), stdin);
		Tell[strcspn(Tell, "\n")] = '\0';
		int s = send_package(client_fd, Tell);
		if (s < 0) {
			perror("Disconnect");
			return 1;
		}
	}
}