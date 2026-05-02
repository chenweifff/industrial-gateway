/**
 * @file    logger.c
 * @brief   工业数据采集与上报网关 —— 日志管理模块（实现）
 *
 * 实现要点（初学者参考）：
 *   1. 全局单例模式 —— 整个进程只有一个日志实例，用静态变量保存状态。
 *   2. 线程安全 —— 用 pthread_mutex_t 保护所有写操作，
 *      多线程同时调用 log_xxx() 不会导致日志内容交叉。
 *   3. 文件滚动 —— 当日志文件超过 max_size_mb 时，自动将旧文件
 *      重命名为 .1 / .2 / ...，然后打开新文件继续写。
 *   4. 双输出 —— 同时写到控制台（stdout/stderr）和文件，
 *      控制台带颜色区分级别（终端支持时）。
 *   5. 变参函数 —— 利用 va_list 将 printf 风格接口转发到 vsnprintf。
 */

#include "logger.h"
#include "cJSON.h"         /* 第三方 JSON 解析库（仅一个 .h + .c） */

#include <string.h>        /* strlen, strncpy, strrchr */
#include <stdlib.h>        /* malloc, free */
#include <errno.h>         /* errno */
#include <sys/stat.h>      /* stat（获取文件大小） */
#include <unistd.h>        /* rename, access */
#include <pthread.h>       /* 互斥锁 */
#include <time.h>          /* localtime, strftime */

/* ---- 终端颜色（ANSI 转义码）----
 * 控制台输出时用不同颜色区分日志级别，提高可读性。
 * 如果终端不支持或输出到文件，颜色码会被忽略（只是显示为普通文本）。 */
#define COLOR_RESET   "\033[0m"
#define COLOR_DEBUG   "\033[36m"   /* 青色 */
#define COLOR_INFO    "\033[32m"   /* 绿色 */
#define COLOR_WARN    "\033[33m"   /* 黄色 */
#define COLOR_ERROR   "\033[31m"   /* 红色 */

/* ---- 内部状态结构体 ---- */
typedef struct {
    logger_config_t config;     /* 当前配置（副本） */
    FILE           *file;       /* 日志文件句柄，NULL 表示未打开 */
    pthread_mutex_t mutex;      /* 互斥锁：保护 file 和配置的并发访问 */
    int             initialized;/* 是否已初始化（0/1） */
} logger_state_t;

/* ---- 全局单例 ---- */
static logger_state_t g_logger = {0};

/* ---- 内部函数声明 ---- */
static int  open_log_file(void);
static void close_log_file(void);
static void rotate_log_file(void);
static const char *level_str(log_level_t level);
static const char *level_color(log_level_t level);
static void parse_config(const char *config_json, logger_config_t *cfg);

/* ================================================================
 *  公开 API
 * ================================================================ */

int logger_init(const char *config_json)
{
    /* 防止重复初始化 */
    if (g_logger.initialized) {
        return RET_ERR_ALREADY_EXISTS;
    }

    /* 设置默认值 */
    memset(&g_logger.config, 0, sizeof(g_logger.config));
    g_logger.config.level          = LOG_LEVEL_INFO;
    g_logger.config.max_size_mb    = 10;
    g_logger.config.max_files      = 5;
    g_logger.config.console_enable = 1;
    common_strncpy(g_logger.config.file, "./gateway.log", MAX_PATH_LEN);

    /* 如果提供了 JSON 配置，解析并覆盖默认值 */
    if (config_json && strlen(config_json) > 0) {
        parse_config(config_json, &g_logger.config);
    }

    /* 初始化互斥锁 */
    if (pthread_mutex_init(&g_logger.mutex, NULL) != 0) {
        fprintf(stderr, "[LOGGER] 互斥锁初始化失败: %s\n", strerror(errno));
        return RET_ERR;
    }

    /* 打开日志文件 */
    if (open_log_file() != RET_OK) {
        fprintf(stderr, "[LOGGER] 无法打开日志文件: %s\n",
                g_logger.config.file);
        pthread_mutex_destroy(&g_logger.mutex);
        return RET_ERR_IO;
    }

    g_logger.initialized = 1;

    /* 第一条日志：标记系统启动 */
    LOG_INFO("========== 日志系统初始化完成 ==========");
    LOG_INFO("日志级别: %s, 文件: %s, 最大: %d MB",
             level_str(g_logger.config.level),
             g_logger.config.file,
             g_logger.config.max_size_mb);

    return RET_OK;
}

void logger_destroy(void)
{
    if (!g_logger.initialized) {
        return;
    }

    LOG_INFO("========== 日志系统关闭 ==========");

    pthread_mutex_lock(&g_logger.mutex);
    g_logger.initialized = 0;

    close_log_file();
    pthread_mutex_unlock(&g_logger.mutex);

    pthread_mutex_destroy(&g_logger.mutex);
}

void log_write(log_level_t level, const char *file, int line,
               const char *fmt, ...)
{
    va_list args;
    char    msg[4096];         /* 用户消息缓冲区 */
    char    line_buf[5120];    /* 完整一行的缓冲区 */
    char    time_buf[32];      /* 时间戳字符串缓冲区 */
    struct  tm tm_info;
    struct  timespec ts;
    int     msg_len;
    const char *short_file;

    /* 1. 级别过滤：低于当前配置级别的日志直接丢弃 */
    if (!g_logger.initialized || level < g_logger.config.level) {
        return;
    }

    /* 2. 格式化用户消息 */
    va_start(args, fmt);
    msg_len = vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);
    if (msg_len < 0) {
        return;  /* 格式化失败 */
    }

    /* 3. 获取带毫秒的时间戳 */
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm_info);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm_info);

    /* 4. 截取文件名（去掉目录前缀，只保留 basename） */
    short_file = strrchr(file, '/');
    if (short_file) {
        short_file++;  /* 跳过 '/' */
    } else {
        short_file = file;
    }

    /* 5. 组装完整日志行
     * 格式：[时间.毫秒] [级别] [文件:行号] 消息 */
    snprintf(line_buf, sizeof(line_buf),
             "[%s.%03ld] [%s] [%s:%d] %s\n",
             time_buf,
             ts.tv_nsec / 1000000,
             level_str(level),
             short_file, line,
             msg);

    /* 6. 加锁，写入控制台和文件
     * 临界区尽量短 —— 只在写文件时加锁，前面格式化不加锁 */
    pthread_mutex_lock(&g_logger.mutex);

    /* 6a. 输出到控制台（带颜色） */
    if (g_logger.config.console_enable) {
        FILE *out = (level >= LOG_LEVEL_WARN) ? stderr : stdout;
        fprintf(out, "%s%s%s",
                level_color(level),
                line_buf,
                COLOR_RESET);
        fflush(out);
    }

    /* 6b. 写入文件 */
    if (g_logger.file) {
        /* 写入前检查文件大小，超过阈值则滚动 */
        long file_size = ftell(g_logger.file);
        if (file_size >= 0 &&
            file_size > (long)g_logger.config.max_size_mb * 1024 * 1024) {
            rotate_log_file();
        }
        fputs(line_buf, g_logger.file);
        fflush(g_logger.file);
    }

    pthread_mutex_unlock(&g_logger.mutex);
}

/* ---- 便捷函数 ----
 * 保留直接函数调用方式，但推荐使用 LOG_DEBUG() 等宏。
 * 宏版本会自动注入 __FILE__ 和 __LINE__，方便定位问题。
 * 以下函数 file/line 参数填 NULL/0，在日志输出中显示为 "?:0"。
 */

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-security"
#endif

void log_debug(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    log_write(LOG_LEVEL_DEBUG, "?", 0, fmt);
    va_end(args);
}

void log_info(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    log_write(LOG_LEVEL_INFO, "?", 0, fmt);
    va_end(args);
}

void log_warn(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    log_write(LOG_LEVEL_WARN, "?", 0, fmt);
    va_end(args);
}

void log_error(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    log_write(LOG_LEVEL_ERROR, "?", 0, fmt);
    va_end(args);
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

/* ================================================================
 *  内部辅助函数
 * ================================================================ */

/**
 * @brief 打开日志文件。如果文件已存在，以追加模式打开。
 * @return RET_OK 成功，RET_ERR_IO 失败
 *
 * fopen 的 "a" 模式（追加）：
 *   - 文件存在 → 从末尾接着写
 *   - 文件不存在 → 创建新文件
 *   - 每次 fputs 后自动 flush
 */
static int open_log_file(void)
{
    g_logger.file = fopen(g_logger.config.file, "a");
    if (!g_logger.file) {
        return RET_ERR_IO;
    }
    /* 设置为行缓冲模式：每写一行自动 flush */
    setvbuf(g_logger.file, NULL, _IOLBF, 0);
    return RET_OK;
}

/**
 * @brief 关闭日志文件
 */
static void close_log_file(void)
{
    if (g_logger.file) {
        fflush(g_logger.file);
        fclose(g_logger.file);
        g_logger.file = NULL;
    }
}

/**
 * @brief 滚动日志文件
 *
 * 滚动策略（按文件大小，最多保留 max_files 个历史文件）：
 *   1. 关闭当前文件
 *   2. 删除最旧的文件 file.N（如 gateway.log.5）
 *   3. 从 N-1 到 1 依次重命名（gateway.log.4 → gateway.log.5, ...）
 *   4. 当前文件重命名为 gateway.log.1
 *   5. 创建新的空日志文件
 *
 * 注意：此函数必须在持有 mutex 的情况下调用。
 */
static void rotate_log_file(void)
{
    int   i;
    char  old_name[MAX_PATH_LEN + 32];
    char  new_name[MAX_PATH_LEN + 32];

    close_log_file();

    /* 删除最旧的文件（序号最大的备份） */
    snprintf(old_name, sizeof(old_name), "%s.%d",
             g_logger.config.file, g_logger.config.max_files);
    remove(old_name);

    /* 文件序号递推：file.4 → file.5, file.3 → file.4, ... */
    for (i = g_logger.config.max_files - 1; i >= 1; i--) {
        snprintf(old_name, sizeof(old_name), "%s.%d",
                 g_logger.config.file, i);
        snprintf(new_name, sizeof(new_name), "%s.%d",
                 g_logger.config.file, i + 1);
        rename(old_name, new_name);
    }

    /* 当前文件 → .1 */
    snprintf(new_name, sizeof(new_name), "%s.1", g_logger.config.file);
    rename(g_logger.config.file, new_name);

    /* 打开新文件 */
    open_log_file();
}

/**
 * @brief 日志级别 → 字符串
 */
static const char *level_str(log_level_t level)
{
    switch (level) {
        case LOG_LEVEL_DEBUG: return "DEBUG";
        case LOG_LEVEL_INFO:  return "INFO ";
        case LOG_LEVEL_WARN:  return "WARN ";
        case LOG_LEVEL_ERROR: return "ERROR";
        default:              return "????";
    }
}

/**
 * @brief 日志级别 → ANSI 颜色码
 */
static const char *level_color(log_level_t level)
{
    switch (level) {
        case LOG_LEVEL_DEBUG: return COLOR_DEBUG;
        case LOG_LEVEL_INFO:  return COLOR_INFO;
        case LOG_LEVEL_WARN:  return COLOR_WARN;
        case LOG_LEVEL_ERROR: return COLOR_ERROR;
        default:              return "";
    }
}

/**
 * @brief 从 JSON 字符串解析日志配置
 *
 * 参数说明：
 *   json 示例: {"level":"DEBUG", "file":"gateway.log",
 *               "max_size_mb":5, "max_files":3, "console":true}
 *   所有字段均可省略，已有默认值。
 */
static void parse_config(const char *config_json, logger_config_t *cfg)
{
    cJSON *root = cJSON_Parse(config_json);
    if (!root) {
        return;  /* JSON 解析失败，保持默认值 */
    }

    /* 日志级别 */
    cJSON *item = cJSON_GetObjectItem(root, "level");
    if (item && cJSON_IsString(item)) {
        const char *s = item->valuestring;
        if (strcasecmp(s, "DEBUG") == 0)      cfg->level = LOG_LEVEL_DEBUG;
        else if (strcasecmp(s, "INFO") == 0)  cfg->level = LOG_LEVEL_INFO;
        else if (strcasecmp(s, "WARN") == 0)  cfg->level = LOG_LEVEL_WARN;
        else if (strcasecmp(s, "ERROR") == 0) cfg->level = LOG_LEVEL_ERROR;
    }

    /* 日志文件路径 */
    item = cJSON_GetObjectItem(root, "file");
    if (item && cJSON_IsString(item)) {
        common_strncpy(cfg->file, item->valuestring, MAX_PATH_LEN);
    }

    /* 单个文件最大大小（MB） */
    item = cJSON_GetObjectItem(root, "max_size_mb");
    if (item && cJSON_IsNumber(item)) {
        cfg->max_size_mb = item->valueint;
        if (cfg->max_size_mb < 1) cfg->max_size_mb = 1;  /* 至少 1MB */
    }

    /* 保留的历史文件数 */
    item = cJSON_GetObjectItem(root, "max_files");
    if (item && cJSON_IsNumber(item)) {
        cfg->max_files = item->valueint;
        if (cfg->max_files < 1) cfg->max_files = 1;
    }

    /* 是否输出到控制台 */
    item = cJSON_GetObjectItem(root, "console");
    if (item && cJSON_IsBool(item)) {
        cfg->console_enable = cJSON_IsTrue(item) ? 1 : 0;
    }

    cJSON_Delete(root);
}
