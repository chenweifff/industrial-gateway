/**
 * @file    logger.h
 * @brief   工业数据采集与上报网关 —— 日志管理模块（公开接口）
 *
 * 功能（对应需求 3.7）：
 *   - 四级日志：DEBUG、INFO、WARN、ERROR
 *   - 同时输出到控制台和文件
 *   - 按文件大小自动滚动（日志文件超限后自动重命名，开启新文件）
 *   - 线程安全（多线程同时调用不会交叉写入）
 *
 * 依赖：
 *   - common.h（返回码）
 *   - pthread（互斥锁，保证线程安全）
 *
 * 典型用法：
 *   logger_init("{\"level\":\"INFO\", \"file\":\"./gateway.log\", \"max_size_mb\":10}");
 *   log_info("网关启动完成");
 *   log_error("MQTT 连接失败: %s", strerror(errno));
 *   logger_destroy();
 */

#ifndef LOGGER_H
#define LOGGER_H

#include "common.h"    /* RET_OK 等 */
#include <stdio.h>     /* FILE, vsnprintf */
#include <stdarg.h>    /* va_list */

/* ---- 日志级别 ---- */
typedef enum {
    LOG_LEVEL_DEBUG = 0,   /* 调试信息：详细的运行细节 */
    LOG_LEVEL_INFO  = 1,   /* 一般信息：启动、退出、连接等关键事件 */
    LOG_LEVEL_WARN  = 2,   /* 警告：异常但可恢复（如重试、降级） */
    LOG_LEVEL_ERROR = 3    /* 错误：严重故障（如连接失败、写入失败） */
} log_level_t;

/* ---- 日志配置（由 logger_init 从 JSON 解析后填入） ---- */
typedef struct {
    log_level_t level;         /* 当前启用的最低日志级别 */
    char        file[MAX_PATH_LEN];  /* 日志文件路径 */
    int         max_size_mb;   /* 单个日志文件最大大小（MB） */
    int         max_files;     /* 保留的历史日志文件数量 */
    int         console_enable;/* 是否同步输出到控制台（1=是，0=否） */
} logger_config_t;

/* ================================================================
 *  API 函数
 * ================================================================ */

/**
 * @brief 初始化日志系统
 * @param config_json  JSON 格式的配置字符串，可包含：
 *        { "level":"INFO", "file":"gateway.log", "max_size_mb":10,
 *          "max_files":5, "console":true }
 *        所有字段均可省略（使用默认值）。
 * @return RET_OK 成功，负数失败
 */
int logger_init(const char *config_json);

/**
 * @brief 关闭日志系统，刷新缓冲区并关闭文件
 */
void logger_destroy(void);

/**
 * @brief 写 DEBUG 级别日志
 * @param fmt   printf 风格格式串
 * @param ...   可变参数
 */
void log_debug(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/**
 * @brief 写 INFO 级别日志
 */
void log_info(const char *fmt, ...)  __attribute__((format(printf, 1, 2)));

/**
 * @brief 写 WARN 级别日志
 */
void log_warn(const char *fmt, ...)  __attribute__((format(printf, 1, 2)));

/**
 * @brief 写 ERROR 级别日志
 */
void log_error(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/**
 * @brief 通用日志写入函数（带文件/行号），供宏使用
 * @param level     日志级别
 * @param file      源文件名
 * @param line      行号
 * @param fmt       格式串
 * @param ...       可变参数
 */
void log_write(log_level_t level, const char *file, int line,
               const char *fmt, ...) __attribute__((format(printf, 4, 5)));

/*
 * 便捷宏：自动捕获 __FILE__ 和 __LINE__，
 * 使每条日志都能追溯到具体代码位置，方便调试。
 */
#define LOG_DEBUG(fmt, ...) \
    log_write(LOG_LEVEL_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  \
    log_write(LOG_LEVEL_INFO,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  \
    log_write(LOG_LEVEL_WARN,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) \
    log_write(LOG_LEVEL_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif /* LOGGER_H */
