#include "log.h"
#include <stdarg.h>
#include <stdio.h>
#include <time.h>

static const char *level_list[] = {"DEBUG", "INFO ", "WARN ", "ERROR"};
static int min_level = LOG_DEBUG;

void log_set_level(int level) { min_level = level; }

// 获取并计算毫秒级时间戳
static void get_time(char *buf, size_t size) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);

  struct tm tm;
  localtime_r(&ts.tv_sec, &tm);
  snprintf(buf, size, "%04d-%02d-%02d %02d:%02d:%02d.%03ld", tm.tm_year + 1900,
           tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
           ts.tv_nsec / 1000000);
}

// 普通日志打印
void log_row(int level, int fd, const char *file, int line, const char *fmt,
             ...) {
  // 检测限制等级,如果 事件等级 小于 最小事件等级，就跳过打印
  if (min_level > level)
    return;

  // 获得时间戳
  char time_buf[32] = {0};
  get_time(time_buf, sizeof(time_buf));
  // 打印前置消息[time] [level] [fd]
  fprintf(stderr, "[%s] [%s] [fd:%d] ", time_buf, level_list[level], fd);

  // 打印信息
  // 创建可变参数指针
  va_list ap;
  // 指针指向fmt的第一个可变参数
  va_start(ap, fmt);
  // 可变参数专用打印
  vfprintf(stderr, fmt, ap);
  // 回收
  va_end(ap);

  // 打印来源文件与产生打印的代码行数
  fprintf(stderr, " (%s:%d)\n", file, line);
}