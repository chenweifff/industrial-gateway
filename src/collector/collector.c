/**
 * @file    collector.c
 * @brief   工业数据采集与上报网关 —— 数据采集与统一封装模块（实现）
 *
 * 在系统中的位置（初学者参考）：
 *
 *   [模拟器 / 数据源]  →  [采集模块]  →  [处理模块]  →  [存储 + 上报]
 *     (生成原始数据)      (拉取+封装)    (校验+打包)     (持久化+发送)
 *
 * 采集模块是一个「薄封装层」：
 *   - 它不产生数据，只从数据源拉取
 *   - 它不做复杂校验（交给处理模块），只做基础的采集状态标记
 *   - 它处理采集级别的异常（超时、重试），不做业务级校验
 *
 * 采集状态码约定：
 *    0   : 采集成功
 *   -1   : 设备离线
 *   -2   : 采集超时
 *   -3   : 数据为空
 *   -4   : 数据源返回错误
 */

#include "collector.h"
#include "logger.h"
#include "cJSON.h"

#include <stdlib.h>     /* malloc, free */
#include <string.h>     /* memset */
#include <unistd.h>     /* usleep */

/* ---- 全局状态 ---- */
static collector_config_t g_config = {
    .timeout_ms  = 5000,   /* 默认 5 秒超时 */
    .retry_count = 3       /* 默认重试 3 次 */
};
static int g_initialized = 0;

/* ---- 内部函数声明 ---- */
static int  parse_config(const char *config_json, collector_config_t *cfg);
static void validate_collected_data(data_point_t *points, int count);

/* ================================================================
 *  公开 API
 * ================================================================ */

int collector_init(const char *config_json)
{
    /* 设置默认值 */
    g_config.timeout_ms  = 5000;
    g_config.retry_count = 3;

    if (config_json && strlen(config_json) > 0) {
        if (parse_config(config_json, &g_config) != RET_OK) {
            LOG_WARN("采集模块配置解析失败，使用默认值");
        }
    }

    g_initialized = 1;
    LOG_INFO("采集模块初始化完成: 超时=%dms, 重试=%d次",
             g_config.timeout_ms, g_config.retry_count);
    return RET_OK;
}

void collector_destroy(void)
{
    g_initialized = 0;
    LOG_INFO("采集模块已释放");
}

int collector_collect(data_source_interface_t *source,
                      data_point_t *points, int max_count, int *actual)
{
    int ret;
    int total = 0;
    int attempt;

    if (!source || !points || max_count <= 0 || !actual) {
        if (actual) *actual = 0;
        return RET_ERR_INVALID_PARAM;
    }

    *actual = 0;

    /* ==========================================================
     * 采集流程：
     *   1. 首次尝试调用 source->acquire()
     *   2. 如果获取到数据 → 校验并返回
     *   3. 如果没有数据 → 等待后重试（最多 retry_count 次）
     * ========================================================== */

    for (attempt = 0; attempt <= g_config.retry_count; attempt++) {
        int round_count = 0;

        if (attempt > 0) {
            /* 重试前短暂等待，避免忙轮询 */
            usleep(100000);  /* 100 毫秒 */
            LOG_DEBUG("采集重试 %d/%d", attempt, g_config.retry_count);
        }

        /* 调用数据源的 acquire 接口 */
        ret = source->acquire(source->ctx, points + total,
                              max_count - total, &round_count);
        if (ret != RET_OK) {
            LOG_ERROR("数据源 acquire() 返回错误: %d", ret);
            /* 数据源错误，继续重试 */
            continue;
        }

        if (round_count > 0) {
            total += round_count;

            /* 对采集到的数据进行基础校验 */
            validate_collected_data(points, total);

            *actual = total;
            return RET_OK;
        }

        /* round_count == 0：没有数据，继续重试 */
        LOG_DEBUG("本次采集无数据（尝试 %d/%d）", attempt + 1,
                  g_config.retry_count + 1);
    }

    /* 所有重试均无数据：不是错误，只是没有新数据 */
    LOG_DEBUG("采集完成：所有尝试后无可用数据");
    *actual = 0;
    return RET_OK;
}

const collector_config_t *collector_get_config(void)
{
    return &g_config;
}

/* ================================================================
 *  内部辅助函数
 * ================================================================ */

/**
 * @brief 解析 JSON 配置
 *
 * 支持字段：timeout_ms、retry_count
 */
static int parse_config(const char *config_json, collector_config_t *cfg)
{
    cJSON *root = cJSON_Parse(config_json);
    if (!root) {
        return RET_ERR_PARSE;
    }

    cJSON *item;

    item = cJSON_GetObjectItem(root, "timeout_ms");
    if (item && cJSON_IsNumber(item)) {
        cfg->timeout_ms = item->valueint;
        if (cfg->timeout_ms < 0) cfg->timeout_ms = 0;
    }

    item = cJSON_GetObjectItem(root, "retry_count");
    if (item && cJSON_IsNumber(item)) {
        cfg->retry_count = item->valueint;
        if (cfg->retry_count < 0) cfg->retry_count = 0;
        if (cfg->retry_count > 10) cfg->retry_count = 10;  /* 最多 10 次 */
    }

    cJSON_Delete(root);
    return RET_OK;
}

/**
 * @brief 对采集到的数据进行基础校验和状态标记
 *
 * 这里只做「采集级别」的校验，不做业务级校验（业务级校验在 processor 模块）。
 *
 * 检查项：
 *   1. 设备离线（status_code != 0）→ 质量标记为 UNCERTAIN
 *   2. 时间戳异常（为 0 或超未来）→ 标记为 BAD
 *   3. 空字符串 → 标记为 INVALID
 */
static void validate_collected_data(data_point_t *points, int count)
{
    int64_t now = common_timestamp_ms();

    for (int i = 0; i < count; i++) {
        data_point_t *p = &points[i];

        /* 1. 设备离线 → 质量降级 */
        if (p->status_code != 0 && p->quality == QUALITY_GOOD) {
            p->quality = QUALITY_UNCERTAIN;
        }

        /* 2. 时间戳异常检查 */
        if (p->timestamp <= 0) {
            p->timestamp = now;  /* 修正为当前时间 */
            p->quality = QUALITY_BAD;
            p->status_code = -4;
        } else if (p->timestamp > now + 60000) {
            /* 时间戳在未来 60 秒以上 → 可疑 */
            p->quality = QUALITY_UNCERTAIN;
        } else if (p->timestamp < now - 3600000) {
            /* 时间戳在 1 小时以前 → 过期数据 */
            p->quality = QUALITY_UNCERTAIN;
        }

        /* 3. 空字符串检查 */
        if (p->type == DATA_TYPE_STRING && p->value.str_val[0] == '\0') {
            p->data_status = DATA_STATUS_INVALID;
            p->quality = QUALITY_BAD;
        }
    }
}
