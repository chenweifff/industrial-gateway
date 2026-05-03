/**
 * @file    storage.c
 * @brief   工业数据采集与上报网关 —— 本地存储模块（SQLite 实现）
 *
 * SQLite 基础知识（初学者参考）：
 *
 *   1. SQLite 是一个嵌入式数据库，不是独立的服务进程，数据存在单个 .db 文件里。
 *      程序通过 libsqlite3 库直接读写这个文件，不需要安装数据库服务。
 *
 *   2. 关键 API：
 *      sqlite3_open()      —— 打开/创建数据库文件
 *      sqlite3_exec()      —— 执行 SQL 语句（无返回值时用）
 *      sqlite3_prepare_v2() —— 编译 SQL 为"预处理语句"
 *      sqlite3_bind_xxx()  —— 向预处理语句绑定参数（防 SQL 注入）
 *      sqlite3_step()      —— 执行预处理语句
 *      sqlite3_finalize()  —— 释放预处理语句
 *      sqlite3_close()     —— 关闭数据库
 *
 *   3. WAL 模式（Write-Ahead Logging）：
 *      - 默认的 DELETE 模式下，读操作会被写操作阻塞
 *      - WAL 模式下读和写可以同时进行，性能更好
 *      - 命令: PRAGMA journal_mode=WAL;
 *
 *   4. 事务：
 *      - BEGIN TRANSACTION ... COMMIT 可以让批量写入快 10~100 倍
 *      - INSERT 慢在每次都要 fsync，事务把大量写入合并为一次 fsync
 *
 * 数据库表结构：
 *
 *   CREATE TABLE data_cache (
 *       id          INTEGER PRIMARY KEY AUTOINCREMENT,   -- 自增主键
 *       device_id   TEXT    NOT NULL,                    -- 设备标识
 *       point_id    TEXT    NOT NULL,                    -- 点位标识
 *       data_type   INTEGER NOT NULL,                    -- 数据类型（枚举值）
 *       int_value   INTEGER,                             -- 整数/布尔值
 *       float_value REAL,                                -- 浮点值
 *       str_value   TEXT,                                -- 字符串值
 *       timestamp   INTEGER NOT NULL,                    -- 采集时间戳（毫秒）
 *       status_code INTEGER DEFAULT 0,                   -- 采集状态码
 *       quality     INTEGER DEFAULT 0,                   -- 质量标记
 *       data_status INTEGER DEFAULT 0,                   -- 上报状态（0=待上报）
 *       created_at  TEXT    DEFAULT (datetime('now','localtime'))
 *   );
 *
 *   CREATE INDEX idx_pending   ON data_cache(data_status, timestamp);
 *   CREATE INDEX idx_timestamp  ON data_cache(timestamp);
 */

#include "storage.h"
#include "logger.h"

#include <sqlite3.h>     /* SQLite C API */
#include <stdio.h>       /* snprintf */
#include <stdlib.h>      /* malloc, free */
#include <string.h>      /* strlen, memset */

/* ---- 默认参数 ---- */
#define DEFAULT_MAX_CACHE_SIZE    10000    /* 默认最大缓存条数 */
#define DEFAULT_CLEANUP_INTERVAL  3600     /* 清理间隔（秒） */
#define DEFAULT_RETENTION_DAYS    7        /* 保留天数 */

/* ---- 内部上下文 ---- */
typedef struct {
    sqlite3      *db;               /* SQLite 数据库句柄 */
    char          db_path[MAX_PATH_LEN]; /* 数据库文件路径 */
    int           max_cache_size;   /* 最大缓存条数（积压上限） */
    int           cleanup_interval; /* 清理间隔（秒） */
    int           retention_days;   /* 数据保留天数 */
    int           initialized;      /* 是否已初始化 */

    /* 预处理语句（prepare 一次，反复 bind+step，比每次 sqlite3_exec 快） */
    sqlite3_stmt  *stmt_insert;
    sqlite3_stmt  *stmt_query_pending;
    sqlite3_stmt  *stmt_mark_reported;
    sqlite3_stmt  *stmt_mark_failed;
    sqlite3_stmt  *stmt_cleanup;
    sqlite3_stmt  *stmt_count_pending;
    sqlite3_stmt  *stmt_discard_oldest;
} storage_ctx_t;

/* ---- 内部函数声明 ---- */
static int  sto_init(void *ctx, const char *db_path);
static int  sto_insert(void *ctx, const data_point_t *point);
static int  sto_insert_batch(void *ctx, const data_point_t *points, int count);
static int  sto_query_pending(void *ctx, data_point_t *points,
                              int max_count, int *actual);
static int  sto_mark_reported(void *ctx, const int64_t *ids, int count);
static int  sto_mark_failed(void *ctx, int64_t id);
static int  sto_cleanup(void *ctx, int64_t before_timestamp);
static int  sto_get_pending_count(void *ctx, int *count);
static void sto_destroy(void *ctx);

static int  create_tables(sqlite3 *db);
static int  prepare_statements(storage_ctx_t *sto);
static void finalize_statements(storage_ctx_t *sto);
static int  enforce_capacity(storage_ctx_t *sto);
static void bind_data_point(sqlite3_stmt *stmt, const data_point_t *point);
static void read_data_point(sqlite3_stmt *stmt, data_point_t *point);

/* ================================================================
 *  工厂函数
 * ================================================================ */

storage_interface_t *storage_create(void)
{
    storage_interface_t *iface;
    storage_ctx_t *ctx;

    iface = (storage_interface_t *)malloc(sizeof(storage_interface_t));
    if (!iface) return NULL;

    ctx = (storage_ctx_t *)calloc(1, sizeof(storage_ctx_t));
    if (!ctx) {
        free(iface);
        return NULL;
    }

    /* 绑定接口函数 */
    iface->ctx               = ctx;
    iface->init              = sto_init;
    iface->insert            = sto_insert;
    iface->insert_batch      = sto_insert_batch;
    iface->query_pending     = sto_query_pending;
    iface->mark_reported     = sto_mark_reported;
    iface->mark_failed       = sto_mark_failed;
    iface->cleanup           = sto_cleanup;
    iface->get_pending_count = sto_get_pending_count;
    iface->destroy           = sto_destroy;

    return iface;
}

/* ================================================================
 *  init: 打开数据库 + 建表 + 准备语句
 * ================================================================ */

static int sto_init(void *ctx, const char *db_path)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !db_path) {
        return RET_ERR_INVALID_PARAM;
    }

    common_strncpy(sto->db_path, db_path, MAX_PATH_LEN);
    sto->max_cache_size   = DEFAULT_MAX_CACHE_SIZE;
    sto->cleanup_interval = DEFAULT_CLEANUP_INTERVAL;
    sto->retention_days   = DEFAULT_RETENTION_DAYS;

    LOG_INFO("正在打开 SQLite 数据库: %s", db_path);

    /* 打开数据库（不存在则自动创建） */
    rc = sqlite3_open(db_path, &sto->db);
    if (rc != SQLITE_OK) {
        LOG_ERROR("无法打开 SQLite 数据库: %s", sqlite3_errmsg(sto->db));
        return RET_ERR_IO;
    }

    /* 启用 WAL 模式 —— 读写不互斥，Navicat 可同时读取 */
    char *err = NULL;
    rc = sqlite3_exec(sto->db, "PRAGMA journal_mode=WAL;", NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        LOG_WARN("WAL 模式启用失败: %s", err ? err : "未知");
        if (err) sqlite3_free(err);
    } else {
        LOG_INFO("SQLite WAL 模式已启用");
    }

    /* 提高性能的 PRAGMA */
    sqlite3_exec(sto->db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    sqlite3_exec(sto->db, "PRAGMA cache_size=-8000;", NULL, NULL, NULL);  /* 8MB缓存 */
    sqlite3_exec(sto->db, "PRAGMA busy_timeout=5000;", NULL, NULL, NULL); /* 5秒忙等待 */

    /* 建表 */
    if (create_tables(sto->db) != RET_OK) {
        LOG_ERROR("数据库建表失败");
        sqlite3_close(sto->db);
        sto->db = NULL;
        return RET_ERR_IO;
    }

    /* 预编译 SQL 语句 */
    if (prepare_statements(sto) != RET_OK) {
        LOG_ERROR("预编译 SQL 语句失败");
        sqlite3_close(sto->db);
        sto->db = NULL;
        return RET_ERR;
    }

    sto->initialized = 1;
    LOG_INFO("SQLite 存储模块初始化完成");
    return RET_OK;
}

/* ================================================================
 *  insert: 插入单条数据
 * ================================================================ */

static int sto_insert(void *ctx, const data_point_t *point)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !sto->initialized || !point) {
        return RET_ERR_INVALID_PARAM;
    }

    /* 绑定参数到预编译语句 */
    sqlite3_reset(sto->stmt_insert);
    sqlite3_clear_bindings(sto->stmt_insert);
    bind_data_point(sto->stmt_insert, point);

    rc = sqlite3_step(sto->stmt_insert);
    if (rc != SQLITE_DONE) {
        LOG_ERROR("插入数据失败: %s", sqlite3_errmsg(sto->db));
        return RET_ERR_IO;
    }

    /* 容量控制：超过最大缓存数则丢弃最旧数据 */
    enforce_capacity(sto);

    return RET_OK;
}

/* ================================================================
 *  insert_batch: 批量插入（使用事务加速）
 * ================================================================ */

static int sto_insert_batch(void *ctx, const data_point_t *points, int count)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !sto->initialized || !points || count <= 0) {
        return RET_ERR_INVALID_PARAM;
    }

    LOG_DEBUG("批量插入 %d 条数据...", count);

    /* 开启事务 —— 让所有 INSERT 在一次 fsync 中完成 */
    sqlite3_exec(sto->db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    for (int i = 0; i < count; i++) {
        sqlite3_reset(sto->stmt_insert);
        sqlite3_clear_bindings(sto->stmt_insert);
        bind_data_point(sto->stmt_insert, &points[i]);

        rc = sqlite3_step(sto->stmt_insert);
        if (rc != SQLITE_DONE) {
            LOG_ERROR("批量插入第 %d 条失败: %s", i, sqlite3_errmsg(sto->db));
            sqlite3_exec(sto->db, "ROLLBACK;", NULL, NULL, NULL);
            return RET_ERR_IO;
        }
    }

    sqlite3_exec(sto->db, "COMMIT;", NULL, NULL, NULL);

    /* 容量控制 */
    enforce_capacity(sto);

    return RET_OK;
}

/* ================================================================
 *  query_pending: 查询待上报数据
 * ================================================================ */

static int sto_query_pending(void *ctx, data_point_t *points,
                             int max_count, int *actual)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int count = 0;
    int rc;

    if (!sto || !sto->initialized || !points || max_count <= 0 || !actual) {
        if (actual) *actual = 0;
        return RET_ERR_INVALID_PARAM;
    }

    sqlite3_reset(sto->stmt_query_pending);
    sqlite3_clear_bindings(sto->stmt_query_pending);

    /* 绑定 LIMIT 参数 */
    sqlite3_bind_int(sto->stmt_query_pending, 1, max_count);

    while ((rc = sqlite3_step(sto->stmt_query_pending)) == SQLITE_ROW
           && count < max_count) {
        read_data_point(sto->stmt_query_pending, &points[count]);
        points[count].data_status = DATA_STATUS_PENDING;
        count++;
    }

    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
        LOG_ERROR("查询待上报数据失败: %s", sqlite3_errmsg(sto->db));
        *actual = 0;
        return RET_ERR_IO;
    }

    *actual = count;
    return RET_OK;
}

/* ================================================================
 *  mark_reported: 标记为已上报
 * ================================================================ */

static int sto_mark_reported(void *ctx, const int64_t *ids, int count)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !sto->initialized || !ids || count <= 0) {
        return RET_ERR_INVALID_PARAM;
    }

    sqlite3_exec(sto->db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    for (int i = 0; i < count; i++) {
        if (ids[i] <= 0) continue;

        sqlite3_reset(sto->stmt_mark_reported);
        sqlite3_clear_bindings(sto->stmt_mark_reported);
        sqlite3_bind_int64(sto->stmt_mark_reported, 1, (sqlite3_int64)ids[i]);

        rc = sqlite3_step(sto->stmt_mark_reported);
        if (rc != SQLITE_DONE) {
            LOG_ERROR("标记已上报失败 id=%lld: %s",
                      (long long)ids[i], sqlite3_errmsg(sto->db));
        }
    }

    sqlite3_exec(sto->db, "COMMIT;", NULL, NULL, NULL);
    return RET_OK;
}

/* ================================================================
 *  mark_failed: 标记为上报失败
 * ================================================================ */

static int sto_mark_failed(void *ctx, int64_t id)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !sto->initialized || id <= 0) {
        return RET_ERR_INVALID_PARAM;
    }

    sqlite3_reset(sto->stmt_mark_failed);
    sqlite3_clear_bindings(sto->stmt_mark_failed);
    sqlite3_bind_int64(sto->stmt_mark_failed, 1, (sqlite3_int64)id);

    rc = sqlite3_step(sto->stmt_mark_failed);
    if (rc != SQLITE_DONE) {
        LOG_ERROR("标记失败 id=%lld: %s",
                  (long long)id, sqlite3_errmsg(sto->db));
        return RET_ERR_IO;
    }

    return RET_OK;
}

/* ================================================================
 *  cleanup: 清理过期数据
 * ================================================================ */

static int sto_cleanup(void *ctx, int64_t before_timestamp)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !sto->initialized) {
        return RET_ERR_INVALID_PARAM;
    }

    sqlite3_reset(sto->stmt_cleanup);
    sqlite3_clear_bindings(sto->stmt_cleanup);
    sqlite3_bind_int64(sto->stmt_cleanup, 1, (sqlite3_int64)before_timestamp);

    rc = sqlite3_step(sto->stmt_cleanup);
    if (rc != SQLITE_DONE) {
        LOG_ERROR("清理过期数据失败: %s", sqlite3_errmsg(sto->db));
        return RET_ERR_IO;
    }

    int deleted = sqlite3_changes(sto->db);
    if (deleted > 0) {
        LOG_INFO("清理了 %d 条过期数据", deleted);
    }

    return RET_OK;
}

/* ================================================================
 *  get_pending_count: 获取积压数量
 * ================================================================ */

static int sto_get_pending_count(void *ctx, int *count)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    int rc;

    if (!sto || !sto->initialized || !count) {
        if (count) *count = 0;
        return RET_ERR_INVALID_PARAM;
    }

    sqlite3_reset(sto->stmt_count_pending);
    sqlite3_clear_bindings(sto->stmt_count_pending);

    rc = sqlite3_step(sto->stmt_count_pending);
    if (rc == SQLITE_ROW) {
        *count = sqlite3_column_int(sto->stmt_count_pending, 0);
    } else {
        *count = 0;
    }

    return RET_OK;
}

/* ================================================================
 *  destroy: 关闭数据库，释放资源
 * ================================================================ */

static void sto_destroy(void *ctx)
{
    storage_ctx_t *sto = (storage_ctx_t *)ctx;
    if (!sto) return;

    finalize_statements(sto);

    if (sto->db) {
        /* WAL checkpoint —— 将 WAL 写入主数据库文件，确保 Navicat 看到最新数据 */
        sqlite3_exec(sto->db, "PRAGMA wal_checkpoint(TRUNCATE);", NULL, NULL, NULL);
        sqlite3_close(sto->db);
        sto->db = NULL;
    }

    sto->initialized = 0;
    free(sto);

    LOG_INFO("SQLite 存储模块已关闭");
}

/* ================================================================
 *  内部函数
 * ================================================================ */

/**
 * @brief 创建数据库表
 */
static int create_tables(sqlite3 *db)
{
    const char *sql =
        "CREATE TABLE IF NOT EXISTS data_cache ("
        "    id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "    device_id   TEXT    NOT NULL,"
        "    point_id    TEXT    NOT NULL,"
        "    data_type   INTEGER NOT NULL,"
        "    int_value   INTEGER,"
        "    float_value REAL,"
        "    bool_value  INTEGER,"
        "    str_value   TEXT,"
        "    timestamp   INTEGER NOT NULL,"
        "    status_code INTEGER DEFAULT 0,"
        "    quality     INTEGER DEFAULT 0,"
        "    data_status INTEGER DEFAULT 0,"
        "    created_at  TEXT    DEFAULT (datetime('now','localtime'))"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_pending "
        "    ON data_cache(data_status, timestamp);"
        "CREATE INDEX IF NOT EXISTS idx_timestamp "
        "    ON data_cache(timestamp);";

    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        LOG_ERROR("建表失败: %s", err);
        sqlite3_free(err);
        return RET_ERR_IO;
    }

    return RET_OK;
}

/**
 * @brief 预编译所有 SQL 语句
 *
 * sqlite3_prepare_v2() 将 SQL 文本编译为字节码，后续只 bind + step，
 * 避免每次执行都重新解析 SQL，大幅提升性能。
 */
static int prepare_statements(storage_ctx_t *sto)
{
    const char *sql;

    sql = "INSERT INTO data_cache (device_id, point_id, data_type, "
          "int_value, float_value, bool_value, str_value, "
          "timestamp, status_code, quality, data_status) "
          "VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11);";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_insert, NULL) != SQLITE_OK)
        return RET_ERR;

    sql = "SELECT id, device_id, point_id, data_type, "
          "int_value, float_value, bool_value, str_value, "
          "timestamp, status_code, quality "
          "FROM data_cache WHERE data_status = 0 "
          "ORDER BY timestamp ASC LIMIT ?1;";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_query_pending, NULL) != SQLITE_OK)
        return RET_ERR;

    sql = "UPDATE data_cache SET data_status = 1 WHERE id = ?1;";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_mark_reported, NULL) != SQLITE_OK)
        return RET_ERR;

    sql = "UPDATE data_cache SET data_status = 2 WHERE id = ?1;";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_mark_failed, NULL) != SQLITE_OK)
        return RET_ERR;

    sql = "DELETE FROM data_cache WHERE timestamp < ?1 "
          "AND data_status IN (1, 4);";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_cleanup, NULL) != SQLITE_OK)
        return RET_ERR;

    sql = "SELECT COUNT(*) FROM data_cache WHERE data_status = 0;";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_count_pending, NULL) != SQLITE_OK)
        return RET_ERR;

    sql = "UPDATE data_cache SET data_status = 4 WHERE id = "
          "(SELECT id FROM data_cache WHERE data_status = 0 "
          "ORDER BY timestamp ASC LIMIT 1);";
    if (sqlite3_prepare_v2(sto->db, sql, -1, &sto->stmt_discard_oldest, NULL) != SQLITE_OK)
        return RET_ERR;

    return RET_OK;
}

/**
 * @brief 释放所有预编译语句
 */
static void finalize_statements(storage_ctx_t *sto)
{
    if (sto->stmt_insert)         sqlite3_finalize(sto->stmt_insert);
    if (sto->stmt_query_pending)  sqlite3_finalize(sto->stmt_query_pending);
    if (sto->stmt_mark_reported)  sqlite3_finalize(sto->stmt_mark_reported);
    if (sto->stmt_mark_failed)    sqlite3_finalize(sto->stmt_mark_failed);
    if (sto->stmt_cleanup)        sqlite3_finalize(sto->stmt_cleanup);
    if (sto->stmt_count_pending)  sqlite3_finalize(sto->stmt_count_pending);
    if (sto->stmt_discard_oldest) sqlite3_finalize(sto->stmt_discard_oldest);
}

/**
 * @brief 容量控制：待上报数据超过上限时丢弃最旧的数据
 *
 * 循环执行直到积压数降到上限以下，每次丢弃最旧的一条。
 * 同时记录日志，便于运维发现积压异常。
 */
static int enforce_capacity(storage_ctx_t *sto)
{
    int pending;
    int discarded = 0;

    while (1) {
        if (sto_get_pending_count(sto, &pending) != RET_OK) break;
        if (pending <= sto->max_cache_size) break;

        sqlite3_reset(sto->stmt_discard_oldest);
        int rc = sqlite3_step(sto->stmt_discard_oldest);
        if (rc != SQLITE_DONE) {
            LOG_ERROR("容量控制——丢弃最旧数据失败");
            break;
        }
        discarded++;
    }

    if (discarded > 0) {
        LOG_WARN("缓存积压：已丢弃 %d 条最旧数据（当前待上报: %d/上限: %d）",
                 discarded, pending, sto->max_cache_size);
    }

    return RET_OK;
}

/**
 * @brief 将 data_point_t 的各字段绑定到 INSERT 预编译语句的参数
 *
 * sqlite3_bind_xxx() 系列函数用于将 C 变量值填入 SQL 中的 ? 占位符。
 * 参数索引从 1 开始（不是 0）。
 */
static void bind_data_point(sqlite3_stmt *stmt, const data_point_t *point)
{
    sqlite3_bind_text(stmt, 1, point->device_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, point->point_id,  -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,  3, point->type);

    /* 按类型将值填入对应列，其他列填 NULL */
    switch (point->type) {
    case DATA_TYPE_INT:
        sqlite3_bind_int64(stmt, 4, point->value.int_val);
        sqlite3_bind_null(stmt, 5);
        sqlite3_bind_null(stmt, 6);
        sqlite3_bind_null(stmt, 7);
        break;
    case DATA_TYPE_FLOAT:
        sqlite3_bind_null(stmt, 4);
        sqlite3_bind_double(stmt, 5, point->value.float_val);
        sqlite3_bind_null(stmt, 6);
        sqlite3_bind_null(stmt, 7);
        break;
    case DATA_TYPE_BOOL:
        sqlite3_bind_null(stmt, 4);
        sqlite3_bind_null(stmt, 5);
        sqlite3_bind_int(stmt, 6, point->value.bool_val);
        sqlite3_bind_null(stmt, 7);
        break;
    case DATA_TYPE_STRING:
        sqlite3_bind_null(stmt, 4);
        sqlite3_bind_null(stmt, 5);
        sqlite3_bind_null(stmt, 6);
        sqlite3_bind_text(stmt, 7, point->value.str_val, -1, SQLITE_STATIC);
        break;
    }

    sqlite3_bind_int64(stmt, 8, (sqlite3_int64)point->timestamp);
    sqlite3_bind_int(stmt,   9, point->status_code);
    sqlite3_bind_int(stmt,  10, point->quality);
    sqlite3_bind_int(stmt,  11, point->data_status);
}

/**
 * @brief 从 SELECT 结果行读取数据到 data_point_t
 *
 * sqlite3_column_xxx() 系列函数用于读取查询结果中某列的值。
 * 列索引从 0 开始（与 bind 不同！）。
 */
static void read_data_point(sqlite3_stmt *stmt, data_point_t *point)
{
    memset(point, 0, sizeof(data_point_t));

    point->id = (int64_t)sqlite3_column_int64(stmt, 0);

    const char *s;
    s = (const char *)sqlite3_column_text(stmt, 1);
    if (s) common_strncpy(point->device_id, s, MAX_DEVICE_ID_LEN);

    s = (const char *)sqlite3_column_text(stmt, 2);
    if (s) common_strncpy(point->point_id, s, MAX_POINT_ID_LEN);

    point->type = (data_type_t)sqlite3_column_int(stmt, 3);

    switch (point->type) {
    case DATA_TYPE_INT:
        point->value.int_val = sqlite3_column_int64(stmt, 4);
        break;
    case DATA_TYPE_FLOAT:
        point->value.float_val = sqlite3_column_double(stmt, 5);
        break;
    case DATA_TYPE_BOOL:
        point->value.bool_val = sqlite3_column_int(stmt, 6);
        break;
    case DATA_TYPE_STRING:
        s = (const char *)sqlite3_column_text(stmt, 7);
        if (s) common_strncpy(point->value.str_val, s, MAX_STRING_VALUE_LEN);
        break;
    }

    point->timestamp   = (int64_t)sqlite3_column_int64(stmt, 8);
    point->status_code = sqlite3_column_int(stmt, 9);
    point->quality     = (quality_flag_t)sqlite3_column_int(stmt, 10);
}
