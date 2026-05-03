/**
 * @file    reporter.c
 * @brief   工业数据采集与上报网关 —— MQTT 上报模块（Paho 异步 API 实现）
 *
 * Paho MQTT 异步 API 工作原理（初学者参考）：
 *
 *   同步 vs 异步：
 *     - 同步 API：调用 MQTTClient_connect() 会阻塞直到连接成功或失败
 *     - 异步 API：MQTTAsync_connect() 立刻返回，结果通过回调函数通知
 *       → 网关需要同时做采集和上报，所以用异步 API 避免阻塞
 *
 *   关键回调：
 *     - on_connect_success : 连接成功 → 设置 g_connected = 1，记录日志
 *     - on_connect_failure : 连接失败 → 设置 g_connected = 0，Paho 自动重试
 *     - on_connection_lost : 连接断开 → 设置 g_connected = 0，Paho 自动重连
 *     - on_delivery_complete: 消息送达确认（QoS >= 1 时）
 *
 *   自动重连：
 *     Paho 异步 API 内置自动重连，只需设置 connect_opts.reconnect参数。
 *     断线后 Paho 会自动按间隔尝试重连，无需我们写循环。
 *
 *   消息发送：
 *     MQTTAsync_sendMessage() 立刻返回（不阻塞），消息排队发送。
 *     我们只需检查当前连接状态，断连时返回错误给网关主控，
 *     主控负责缓存数据到 SQLite，下次重连后补发。
 */

#include "reporter.h"
#include "logger.h"
#include "cJSON.h"
#include "MQTTAsync.h"

#include <stdio.h>      /* snprintf */
#include <stdlib.h>     /* malloc, free */
#include <string.h>     /* memset, strlen, strdup */

/* ---- 默认参数 ---- */
#define DEFAULT_BROKER_ADDR        "tcp://localhost:1883"
#define DEFAULT_CLIENT_ID          "gateway-001"
#define DEFAULT_TOPIC              "industrial/gateway/data"
#define DEFAULT_QOS                1
#define DEFAULT_KEEPALIVE_SEC      60
#define DEFAULT_RECONNECT_SEC      5
#define DEFAULT_MAX_RETRY          3
#define DEFAULT_BATCH_SIZE         10
#define DEFAULT_BATCH_INTERVAL_MS  2000

/* ---- 配置结构体 ---- */
typedef struct {
    char broker_addr[MAX_PATH_LEN];  /* MQTT broker 地址，如 tcp://localhost:1883 */
    char client_id[64];             /* 客户端 ID */
    char username[64];              /* 用户名（可为空） */
    char password[64];              /* 密码（可为空） */
    char topic[MAX_TOPIC_LEN];      /* 上报主题 */
    int  qos;                       /* 服务质量（0/1/2） */
    int  keepalive_sec;             /* 心跳间隔（秒） */
    int  reconnect_interval_sec;    /* 重连间隔（秒） */
    int  max_retry_count;           /* 最大重试次数（当前预留） */
    int  batch_size;                /* 批量发送大小 */
    int  batch_interval_ms;         /* 批量发送间隔（毫秒） */
} reporter_config_t;

/* ---- 内部上下文 ---- */
typedef struct {
    MQTTAsync          client;           /* Paho MQTT 异步客户端句柄 */
    reporter_config_t  config;           /* 配置 */
    volatile int       connected;        /* 连接状态（1=已连接，0=未连接，回调中修改） */
    volatile int       stopping;         /* 正在停止标志 */
    int                initialized;      /* 是否已初始化 */
} reporter_ctx_t;

/* ---- 内部函数声明 ---- */
static int  rpt_init(void *ctx, const char *config_json);
static int  rpt_connect(void *ctx);
static int  rpt_disconnect(void *ctx);
static int  rpt_report(void *ctx, const data_point_t *point);
static int  rpt_report_batch(void *ctx, const data_point_t *points, int count);
static int  rpt_is_connected(void *ctx);
static void rpt_destroy(void *ctx);

static int  parse_config(const char *config_json, reporter_config_t *cfg);
static int  send_json_message(MQTTAsync client, const char *topic,
                              int qos, const char *json_str);
static char *serialize_batch(const data_point_t *points, int count);
static char *serialize_single(const data_point_t *point);

/* Paho 异步回调 */
static void on_connect_success(void *context, MQTTAsync_successData *response);
static void on_connect_failure(void *context, MQTTAsync_failureData *response);
static void on_connection_lost(void *context, char *cause);
static int  on_message_arrived(void *context, char *topicName, int topicLen,
                               MQTTAsync_message *message);

/* ================================================================
 *  工厂函数
 * ================================================================ */

reporter_interface_t *reporter_create(void)
{
    reporter_interface_t *iface;
    reporter_ctx_t *ctx;

    iface = (reporter_interface_t *)malloc(sizeof(reporter_interface_t));
    if (!iface) return NULL;

    ctx = (reporter_ctx_t *)calloc(1, sizeof(reporter_ctx_t));
    if (!ctx) {
        free(iface);
        return NULL;
    }

    iface->ctx          = ctx;
    iface->init         = rpt_init;
    iface->connect      = rpt_connect;
    iface->disconnect   = rpt_disconnect;
    iface->report       = rpt_report;
    iface->report_batch = rpt_report_batch;
    iface->is_connected = rpt_is_connected;
    iface->destroy      = rpt_destroy;

    return iface;
}

/* ================================================================
 *  init: 解析配置 + 创建 MQTT 客户端
 * ================================================================ */

static int rpt_init(void *ctx, const char *config_json)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;
    int rc;

    if (!rpt || !config_json) {
        return RET_ERR_INVALID_PARAM;
    }

    LOG_INFO("MQTT 上报模块初始化...");

    /* 设置默认值 */
    memset(&rpt->config, 0, sizeof(rpt->config));
    common_strncpy(rpt->config.broker_addr, DEFAULT_BROKER_ADDR, MAX_PATH_LEN);
    common_strncpy(rpt->config.client_id,   DEFAULT_CLIENT_ID,   64);
    common_strncpy(rpt->config.topic,       DEFAULT_TOPIC,       MAX_TOPIC_LEN);
    rpt->config.qos                 = DEFAULT_QOS;
    rpt->config.keepalive_sec       = DEFAULT_KEEPALIVE_SEC;
    rpt->config.reconnect_interval_sec = DEFAULT_RECONNECT_SEC;
    rpt->config.max_retry_count     = DEFAULT_MAX_RETRY;
    rpt->config.batch_size          = DEFAULT_BATCH_SIZE;
    rpt->config.batch_interval_ms   = DEFAULT_BATCH_INTERVAL_MS;

    /* 解析 JSON 配置 */
    if (strlen(config_json) > 0) {
        if (parse_config(config_json, &rpt->config) != RET_OK) {
            LOG_WARN("MQTT 配置解析失败，使用默认值");
        }
    }

    /* 创建 MQTT 异步客户端 */
    rc = MQTTAsync_create(&rpt->client, rpt->config.broker_addr,
                          rpt->config.client_id,
                          MQTTCLIENT_PERSISTENCE_NONE, NULL);
    if (rc != MQTTASYNC_SUCCESS) {
        LOG_ERROR("MQTT 客户端创建失败，返回码: %d", rc);
        return RET_ERR;
    }

    /* 注册回调函数 */
    rc = MQTTAsync_setCallbacks(rpt->client, rpt,
                                on_connection_lost,
                                on_message_arrived,
                                NULL);  /* delivery_complete 不用 */
    if (rc != MQTTASYNC_SUCCESS) {
        LOG_ERROR("MQTT 回调注册失败，返回码: %d", rc);
        MQTTAsync_destroy(&rpt->client);
        return RET_ERR;
    }

    rpt->initialized = 1;
    LOG_INFO("MQTT 上报模块初始化完成: broker=%s, topic=%s, qos=%d",
             rpt->config.broker_addr, rpt->config.topic, rpt->config.qos);
    return RET_OK;
}

/* ================================================================
 *  connect: 连接到 EMQX broker
 * ================================================================ */

static int rpt_connect(void *ctx)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;
    MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
    int rc;

    if (!rpt || !rpt->initialized) {
        return RET_ERR;
    }

    if (rpt->connected) {
        LOG_WARN("MQTT 已经连接，跳过");
        return RET_OK;
    }

    LOG_INFO("正在连接 MQTT broker: %s ...", rpt->config.broker_addr);

    /* 配置连接选项 */
    conn_opts.keepAliveInterval = rpt->config.keepalive_sec;
    conn_opts.cleansession      = 1;  /* 新连接清除旧会话 */
    conn_opts.automaticReconnect = 1; /* ---- 启用 Paho 内置自动重连 ---- */
    conn_opts.minRetryInterval  = rpt->config.reconnect_interval_sec;
    conn_opts.maxRetryInterval  = rpt->config.reconnect_interval_sec * 2;

    /* 用户名密码（如果有） */
    if (rpt->config.username[0] != '\0') {
        conn_opts.username = rpt->config.username;
    }
    if (rpt->config.password[0] != '\0') {
        conn_opts.password = rpt->config.password;
    }

    /* 连接成功/失败回调 */
    conn_opts.onSuccess = on_connect_success;
    conn_opts.onFailure = on_connect_failure;
    conn_opts.context   = rpt;

    rc = MQTTAsync_connect(rpt->client, &conn_opts);
    if (rc != MQTTASYNC_SUCCESS) {
        LOG_ERROR("MQTT 连接请求失败，返回码: %d", rc);
        return RET_ERR;
    }

    return RET_OK;
}

/* ================================================================
 *  disconnect: 断开连接
 * ================================================================ */

static int rpt_disconnect(void *ctx)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;
    MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer;
    int rc;

    if (!rpt || !rpt->initialized) {
        return RET_ERR;
    }

    if (!rpt->connected) {
        return RET_OK;
    }

    LOG_INFO("正在断开 MQTT 连接...");

    rpt->stopping = 1;
    disc_opts.timeout = 3000;  /* 最多等待 3 秒 */

    rc = MQTTAsync_disconnect(rpt->client, &disc_opts);
    if (rc != MQTTASYNC_SUCCESS) {
        LOG_ERROR("MQTT 断开连接失败，返回码: %d", rc);
    }

    rpt->connected = 0;
    return RET_OK;
}

/* ================================================================
 *  report: 单条上报
 * ================================================================ */

static int rpt_report(void *ctx, const data_point_t *point)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;

    if (!rpt || !rpt->initialized || !point) {
        return RET_ERR_INVALID_PARAM;
    }

    if (!rpt->connected) {
        return RET_ERR_DISCONNECTED;  /* 未连接，由主控缓存到 SQLite */
    }

    /* 序列化单条数据为 JSON */
    char *json_str = serialize_single(point);
    if (!json_str) {
        return RET_ERR_NO_MEMORY;
    }

    int ret = send_json_message(rpt->client, rpt->config.topic,
                                rpt->config.qos, json_str);
    free(json_str);

    if (ret != RET_OK) {
        LOG_WARN("MQTT 单条上报失败: [%s/%s]", point->device_id, point->point_id);
    }

    return ret;
}

/* ================================================================
 *  report_batch: 批量上报
 * ================================================================ */

static int rpt_report_batch(void *ctx, const data_point_t *points, int count)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;

    if (!rpt || !rpt->initialized || !points || count <= 0) {
        return RET_ERR_INVALID_PARAM;
    }

    if (!rpt->connected) {
        return RET_ERR_DISCONNECTED;
    }

    /* 序列化批量数据为 JSON */
    char *json_str = serialize_batch(points, count);
    if (!json_str) {
        return RET_ERR_NO_MEMORY;
    }

    int ret = send_json_message(rpt->client, rpt->config.topic,
                                rpt->config.qos, json_str);
    free(json_str);

    if (ret != RET_OK) {
        LOG_WARN("MQTT 批量上报失败 (%d 条)", count);
    }

    return ret;
}

/* ================================================================
 *  is_connected: 查询连接状态
 * ================================================================ */

static int rpt_is_connected(void *ctx)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;

    if (!rpt) return 0;

    return rpt->connected;
}

/* ================================================================
 *  destroy: 销毁模块
 * ================================================================ */

static void rpt_destroy(void *ctx)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)ctx;
    if (!rpt) return;

    if (rpt->initialized) {
        rpt_disconnect(rpt);

        /* 释放 Paho 客户端资源 */
        MQTTAsync_destroy(&rpt->client);

        rpt->initialized = 0;
    }

    free(rpt);
    LOG_INFO("MQTT 上报模块已销毁");
}

/* ================================================================
 *  Paho 异步回调函数
 * ================================================================ */

/**
 * @brief 连接成功回调
 *
 * 当 MQTTAsync_connect() 成功完成时，Paho 调用此函数。
 */
static void on_connect_success(void *context, MQTTAsync_successData *response)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)context;
    (void)response;

    rpt->connected = 1;
    LOG_INFO("MQTT 连接成功 (broker: %s)", rpt->config.broker_addr);
}

/**
 * @brief 连接失败回调
 *
 * 如果设置了 automaticReconnect，Paho 会自动重试。
 */
static void on_connect_failure(void *context, MQTTAsync_failureData *response)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)context;

    rpt->connected = 0;
    if (response && response->message) {
        LOG_ERROR("MQTT 连接失败: %s (返回码: %d)",
                  response->message, response->code);
    } else {
        LOG_ERROR("MQTT 连接失败 (无详细错误)");
    }

    if (!rpt->stopping) {
        LOG_INFO("Paho 将在 %d 秒后自动重连...",
                 rpt->config.reconnect_interval_sec);
    }
}

/**
 * @brief 连接断开回调
 *
 * 网络中断或 broker 踢出时触发。Paho 自动重建连接。
 */
static void on_connection_lost(void *context, char *cause)
{
    reporter_ctx_t *rpt = (reporter_ctx_t *)context;

    rpt->connected = 0;
    LOG_WARN("MQTT 连接断开: %s", cause ? cause : "未知原因");

    if (!rpt->stopping) {
        LOG_INFO("Paho 正在自动重连...");
    }
}

/**
 * @brief 消息到达回调（预留，本模块不订阅消息）
 */
static int on_message_arrived(void *context, char *topicName, int topicLen,
                              MQTTAsync_message *message)
{
    /* 当前不处理下发消息，仅预留 */
    (void)context;
    (void)topicLen;
    MQTTAsync_freeMessage(&message);
    MQTTAsync_free(topicName);
    return 1;  /* 返回 1 表示消息已处理 */
}

/* ================================================================
 *  配置解析
 * ================================================================ */

static int parse_config(const char *config_json, reporter_config_t *cfg)
{
    cJSON *root = cJSON_Parse(config_json);
    if (!root) {
        return RET_ERR_PARSE;
    }

    cJSON *item;

    item = cJSON_GetObjectItem(root, "broker_address");
    if (item && cJSON_IsString(item)) {
        common_strncpy(cfg->broker_addr, item->valuestring, MAX_PATH_LEN);
    }

    item = cJSON_GetObjectItem(root, "client_id");
    if (item && cJSON_IsString(item)) {
        common_strncpy(cfg->client_id, item->valuestring, 64);
    }

    item = cJSON_GetObjectItem(root, "username");
    if (item && cJSON_IsString(item)) {
        common_strncpy(cfg->username, item->valuestring, 64);
    }

    item = cJSON_GetObjectItem(root, "password");
    if (item && cJSON_IsString(item)) {
        common_strncpy(cfg->password, item->valuestring, 64);
    }

    item = cJSON_GetObjectItem(root, "topic");
    if (item && cJSON_IsString(item)) {
        common_strncpy(cfg->topic, item->valuestring, MAX_TOPIC_LEN);
    }

    item = cJSON_GetObjectItem(root, "qos");
    if (item && cJSON_IsNumber(item)) {
        cfg->qos = item->valueint;
        if (cfg->qos < 0) cfg->qos = 0;
        if (cfg->qos > 2) cfg->qos = 2;
    }

    item = cJSON_GetObjectItem(root, "keepalive_sec");
    if (item && cJSON_IsNumber(item)) {
        cfg->keepalive_sec = item->valueint;
        if (cfg->keepalive_sec < 5) cfg->keepalive_sec = 5;
    }

    item = cJSON_GetObjectItem(root, "reconnect_interval_sec");
    if (item && cJSON_IsNumber(item)) {
        cfg->reconnect_interval_sec = item->valueint;
        if (cfg->reconnect_interval_sec < 1) cfg->reconnect_interval_sec = 1;
    }

    item = cJSON_GetObjectItem(root, "max_retry_count");
    if (item && cJSON_IsNumber(item)) {
        cfg->max_retry_count = item->valueint;
    }

    item = cJSON_GetObjectItem(root, "batch_size");
    if (item && cJSON_IsNumber(item)) {
        cfg->batch_size = item->valueint;
        if (cfg->batch_size < 1) cfg->batch_size = 1;
    }

    item = cJSON_GetObjectItem(root, "batch_interval_ms");
    if (item && cJSON_IsNumber(item)) {
        cfg->batch_interval_ms = item->valueint;
        if (cfg->batch_interval_ms < 100) cfg->batch_interval_ms = 100;
    }

    cJSON_Delete(root);
    return RET_OK;
}

/* ================================================================
 *  JSON 序列化与消息发送
 * ================================================================ */

/**
 * @brief 发送 JSON 字符串到 MQTT broker
 *
 * MQTTAsync_sendMessage() 是异步的：立即返回，消息排队发送。
 * 对于 QoS 0：发送后不关心结果
 * 对于 QoS 1：Paho 会等待 broker 确认后调用 delivery_complete 回调
 */
static int send_json_message(MQTTAsync client, const char *topic,
                             int qos, const char *json_str)
{
    MQTTAsync_message      msg = MQTTAsync_message_initializer;
    MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
    int rc;

    msg.payload   = (void *)json_str;
    msg.payloadlen = (int)strlen(json_str);
    msg.qos       = qos;
    msg.retained  = 0;  /* 不保留消息（retained message） */

    opts.onSuccess = NULL;  /* 不关心单条发送结果 */
    opts.context   = NULL;

    rc = MQTTAsync_sendMessage(client, topic, &msg, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
        LOG_ERROR("MQTT 发送失败，返回码: %d", rc);
        return RET_ERR_IO;
    }

    return RET_OK;
}

/**
 * @brief 批量序列化：多数据点 → JSON 字符串
 *
 * JSON 格式：
 * {
 *   "gateway": "gateway-001",
 *   "ts": 1700000000000,
 *   "count": 3,
 *   "data": [ {...}, {...}, {...} ]
 * }
 */
static char *serialize_batch(const data_point_t *points, int count)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *arr  = cJSON_CreateArray();

    cJSON_AddStringToObject(root, "gateway", "gateway-001");
    cJSON_AddNumberToObject(root, "ts", (double)common_timestamp_ms());
    cJSON_AddNumberToObject(root, "count", count);

    for (int i = 0; i < count; i++) {
        cJSON *item = cJSON_CreateObject();
        const data_point_t *p = &points[i];

        cJSON_AddStringToObject(item, "device_id", p->device_id);
        cJSON_AddStringToObject(item, "point_id",  p->point_id);

        switch (p->type) {
        case DATA_TYPE_INT:
            cJSON_AddStringToObject(item, "type", "int");
            cJSON_AddNumberToObject(item, "value", (double)p->value.int_val);
            break;
        case DATA_TYPE_FLOAT:
            cJSON_AddStringToObject(item, "type", "float");
            cJSON_AddNumberToObject(item, "value", p->value.float_val);
            break;
        case DATA_TYPE_BOOL:
            cJSON_AddStringToObject(item, "type", "bool");
            cJSON_AddNumberToObject(item, "value", p->value.bool_val);
            break;
        case DATA_TYPE_STRING:
            cJSON_AddStringToObject(item, "type", "string");
            cJSON_AddStringToObject(item, "value", p->value.str_val);
            break;
        }

        cJSON_AddNumberToObject(item, "timestamp", (double)p->timestamp);
        cJSON_AddNumberToObject(item, "quality",   p->quality);
        cJSON_AddNumberToObject(item, "status",    p->status_code);

        cJSON_AddItemToArray(arr, item);
    }

    cJSON_AddItemToObject(root, "data", arr);

    char *json_str = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return json_str;
}

/**
 * @brief 单条序列化：一个数据点 → JSON 字符串
 *
 * JSON 格式：
 * {"device_id":"dev-001","point_id":"temp","type":"float","value":25.5,...}
 */
static char *serialize_single(const data_point_t *point)
{
    return serialize_batch(point, 1);
}
