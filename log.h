// 如果定义过LOG_H,就跳过接下来的这些东西
#ifndef LOG_H
#define LOG_H

#define LOG_DEBUG 0 // 调试时私密信息专用
#define LOG_INFO 1  // 普通日志消息
#define LOG_WARN 2  // 警告
#define LOG_ERROR 3 // 错误

void log_set_level(int level);
void log_row(int level, int fd, const char *file, int line, const char *fmt,
             ...);

#define LOG(level, fd, fmt, ...)                                               \
  log_row(level, fd, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif