/**
 * @file    processor.c
 * @brief   工业数据采集与上报网关 —— 数据处理模块（实现）
 *
 * 处理流程（初学者参考）：
 *
 *   raw_points[]  （采集模块产出）
 *        │
 *        ▼
 *   ┌─────────────────┐
 *   │ 1. 范围校验       │  检查值是否在约定的 min~max 区间
 *   │   超范围 → INVALID│  超范围的标记 data_status = DATA_STATUS_INVALID
 *   └────────┬────────┘
 *            ▼
 *   ┌─────────────────┐
 *   │ 2. 去重           │  同一(device+point+value+时间窗口)的只保留首次
 *   │   重复 → INVALID  │  重复的标记为 DATA_STATUS_INVALID
 *   └────────┬────────┘
 *            ▼
 *   ┌─────────────────┐
 *   │ 3. 质量过滤       │  quality < filter_threshold → DISCARDED
 *   │   低质 → DISCARDED│
 *   └────────┬────────┘
 *            ▼
 *   输出: 经过校验、去重、过滤的数据点
 *
 * 关于「修改原数组」的设计：
 *   为了减少内存拷贝，processor_process() 原地修改 points 数组的状态字段，
 *   不重新分配数组。调用方通过检查 data_status 判断每条数据的有效性。
 */

#include "processor.h"
#include "logger.h"
#include "cJSON.h"

#include <stdlib.h>     /* malloc, free */
#include <string.h>     /* memset, strcmp, strncmp */
#include <math.h>       /* fabs */
#include <time.h>       /* time */

/* ---- 默认参数 ---- */
#define DEFAULT_DEDUP_WINDOW_MS   5000
#define MAX_DEDUP_CACHE           256     /* 去重缓存最大条数 */
#define MAX_RANGE_RULES           64      /* 最大范围规则数 */

/* ---- 全局状态 ---- */
static processor_config_t g_config = {
    .enable_range_check = 1,
    .enable_dedup       = 1,
    .enable_filter      = 0,
    .filter_threshold   = 0.0,
    .dedup_window_ms    = DEFAULT_DEDUP_WINDOW_MS
};
static int g_initialized = 0;

/* ---- 范围规则 ---- */
static range_rule_t g_range_rules[MAX_RANGE_RULES];
static int          g_range_rule_count = 0;

/* ---- 去重缓存条目 ---- */
typedef struct {
    char   device_id[MAX_DEVICE_ID_LEN];
    char   point_id[MAX_POINT_ID_LEN];
    double float_value;           /* 缓存值（float 或 cast 后的 int/bool） */
    int64_t timestamp;            /* 记录时间戳（毫秒） */
} dedup_entry_t;

static dedup_entry_t g_dedup_cache[MAX_DEDUP_CACHE];
static int           g_dedup_count = 0;

/* ---- 内部函数声明 ---- */
static int  parse_config(const char *config_json, processor_config_t *cfg);
static int  check_range(const data_point_t *point, const range_rule_t *rule);
static int  check_duplicate(const data_point_t *point, int64_t window_ms);
static void add_dedup_entry(const data_point_t *point);
static void serialize_point(cJSON *arr, const data_point_t *point);

/* ================================================================
 *  公开 API
 * ================================================================ */

int processor_init(const char *config_json)
{
    memset(&g_config, 0, sizeof(g_config));
    g_config.enable_range_check = 1;
    g_config.enable_dedup       = 1;
    g_config.enable_filter      = 0;
    g_config.filter_threshold   = 0.0;
    g_config.dedup_window_ms    = DEFAULT_DEDUP_WINDOW_MS;

    if (config_json && strlen(config_json) > 0) {
        if (parse_config(config_json, &g_config) != RET_OK) {
            LOG_WARN("处理器配置解析失败，使用默认值");
        }
    }

    g_range_rule_count = 0;
    g_dedup_count      = 0;
    memset(g_range_rules, 0, sizeof(g_range_rules));
    memset(g_dedup_cache, 0, sizeof(g_dedup_cache));

    g_initialized = 1;
    LOG_INFO("数据处理模块初始化完成: 范围校验=%s, 去重=%s, 过滤=%s",
             g_config.enable_range_check ? "开" : "关",
             g_config.enable_dedup ? "开" : "关",
             g_config.enable_filter ? "开" : "关");
    return RET_OK;
}

int processor_load_range_rules(const char *rules_json)
{
    if (!rules_json || strlen(rules_json) == 0) {
        LOG_WARN("范围规则为空，跳过加载");
        return RET_OK;
    }

    cJSON *arr = cJSON_Parse(rules_json);
    if (!arr || !cJSON_IsArray(arr)) {
        LOG_ERROR("范围规则 JSON 格式错误");
        if (arr) cJSON_Delete(arr);
        return RET_ERR_PARSE;
    }

    int count = cJSON_GetArraySize(arr);
    if (count > MAX_RANGE_RULES) count = MAX_RANGE_RULES;
    g_range_rule_count = 0;

    for (int i = 0; i < count; i++) {
        cJSON *item = cJSON_GetArrayItem(arr, i);
        if (!item) continue;

        range_rule_t *rule = &g_range_rules[g_range_rule_count];

        cJSON *field = cJSON_GetObjectItem(item, "point_type");
        if (field && cJSON_IsString(field)) {
            common_strncpy(rule->point_type, field->valuestring, 64);
        } else {
            continue;  /* 没有 point_type 的规则无效 */
        }

        field = cJSON_GetObjectItem(item, "min");
        if (field && cJSON_IsNumber(field)) {
            rule->min = field->valuedouble;
        } else {
            rule->min = -1e99;  /* 无下限 */
        }

        field = cJSON_GetObjectItem(item, "max");
        if (field && cJSON_IsNumber(field)) {
            rule->max = field->valuedouble;
        } else {
            rule->max = 1e99;   /* 无上限 */
        }

        g_range_rule_count++;
    }

    cJSON_Delete(arr);
    LOG_INFO("已加载 %d 条范围校验规则", g_range_rule_count);
    return RET_OK;
}

int processor_process(data_point_t *points, int count, int *valid_count)
{
    int valid = 0;

    if (!points || count <= 0) {
        if (valid_count) *valid_count = 0;
        return RET_ERR_INVALID_PARAM;
    }

    for (int i = 0; i < count; i++) {
        data_point_t *p = &points[i];

        /* 已标记为无效/丢弃的跳过后续检查 */
        if (p->data_status == DATA_STATUS_INVALID ||
            p->data_status == DATA_STATUS_DISCARDED) {
            continue;
        }

        int keep = 1;  /* 保留标志 */

        /* ==========================================
         * 阶段 1：范围校验
         * ========================================== */
        if (g_config.enable_range_check && g_range_rule_count > 0) {
            for (int r = 0; r < g_range_rule_count; r++) {
                if (strcmp(p->point_id, g_range_rules[r].point_type) == 0) {
                    if (check_range(p, &g_range_rules[r]) != RET_OK) {
                        p->data_status = DATA_STATUS_INVALID;
                        p->quality     = QUALITY_BAD;
                        keep = 0;
                        LOG_DEBUG("范围校验失败: [%s/%s] 值超出范围",
                                  p->device_id, p->point_id);
                    }
                    break;
                }
            }
        }

        if (!keep) continue;

        /* ==========================================
         * 阶段 2：去重检查
         * ========================================== */
        if (g_config.enable_dedup) {
            if (check_duplicate(p, g_config.dedup_window_ms)) {
                p->data_status = DATA_STATUS_INVALID;
                p->quality     = QUALITY_UNCERTAIN;
                keep = 0;
                LOG_DEBUG("重复数据: [%s/%s] 在去重窗口内重复",
                          p->device_id, p->point_id);
                continue;
            }
            /* 不是重复 → 加入去重缓存 */
            add_dedup_entry(p);
        }

        if (!keep) continue;

        /* ==========================================
         * 阶段 3：质量过滤
         * ========================================== */
        if (g_config.enable_filter) {
            if ((int)p->quality < (int)g_config.filter_threshold) {
                p->data_status = DATA_STATUS_DISCARDED;
                continue;
            }
        }

        valid++;
    }

    if (valid_count) *valid_count = valid;
    return RET_OK;
}

int processor_pack_to_json(const data_point_t *points, int count,
                           char *json_out, size_t size)
{
    if (!points || count <= 0 || !json_out || size == 0) {
        return RET_ERR_INVALID_PARAM;
    }

    /* 构建 JSON 对象 */
    cJSON *root  = cJSON_CreateObject();
    cJSON *arr   = cJSON_CreateArray();

    cJSON_AddStringToObject(root, "gateway", "gateway-001");
    cJSON_AddNumberToObject(root, "ts", (double)common_timestamp_ms());
    cJSON_AddNumberToObject(root, "count", count);

    for (int i = 0; i < count; i++) {
        serialize_point(arr, &points[i]);
    }

    cJSON_AddItemToObject(root, "data", arr);

    /* 序列化为字符串 */
    char *json_str = cJSON_PrintUnformatted(root);
    if (!json_str) {
        cJSON_Delete(root);
        return RET_ERR_NO_MEMORY;
    }

    common_strncpy(json_out, json_str, size);
    free(json_str);
    cJSON_Delete(root);
    return RET_OK;
}

void processor_destroy(void)
{
    g_initialized       = 0;
    g_range_rule_count  = 0;
    g_dedup_count       = 0;
    memset(g_range_rules, 0, sizeof(g_range_rules));
    memset(g_dedup_cache, 0, sizeof(g_dedup_cache));
    LOG_INFO("数据处理模块已释放");
}

const processor_config_t *processor_get_config(void)
{
    return &g_config;
}

/* ================================================================
 *  内部辅助函数
 * ================================================================ */

/**
 * @brief 解析 JSON 配置
 */
static int parse_config(const char *config_json, processor_config_t *cfg)
{
    cJSON *root = cJSON_Parse(config_json);
    if (!root) {
        return RET_ERR_PARSE;
    }

    cJSON *item;

    item = cJSON_GetObjectItem(root, "enable_range_check");
    if (item && cJSON_IsBool(item)) {
        cfg->enable_range_check = cJSON_IsTrue(item) ? 1 : 0;
    }

    item = cJSON_GetObjectItem(root, "enable_dedup");
    if (item && cJSON_IsBool(item)) {
        cfg->enable_dedup = cJSON_IsTrue(item) ? 1 : 0;
    }

    item = cJSON_GetObjectItem(root, "enable_filter");
    if (item && cJSON_IsBool(item)) {
        cfg->enable_filter = cJSON_IsTrue(item) ? 1 : 0;
    }

    item = cJSON_GetObjectItem(root, "filter_threshold");
    if (item && cJSON_IsNumber(item)) {
        cfg->filter_threshold = item->valuedouble;
    }

    item = cJSON_GetObjectItem(root, "dedup_window_ms");
    if (item && cJSON_IsNumber(item)) {
        cfg->dedup_window_ms = item->valueint;
        if (cfg->dedup_window_ms < 100) cfg->dedup_window_ms = 100;
    }

    cJSON_Delete(root);
    return RET_OK;
}

/**
 * @brief 范围校验
 *
 * 将 data_point 的值转为 double 后与规则的 min/max 比较。
 *
 * @return RET_OK 通过，RET_ERR 超范围
 */
static int check_range(const data_point_t *point, const range_rule_t *rule)
{
    double value;

    switch (point->type) {
    case DATA_TYPE_INT:
        value = (double)point->value.int_val;
        break;
    case DATA_TYPE_FLOAT:
        value = point->value.float_val;
        break;
    case DATA_TYPE_BOOL:
        value = (double)point->value.bool_val;
        break;
    case DATA_TYPE_STRING:
        /* 字符串类型不做范围校验 */
        return RET_OK;
    default:
        return RET_OK;
    }

    if (value < rule->min || value > rule->max) {
        return RET_ERR;  /* 超范围 */
    }
    return RET_OK;
}

/**
 * @brief 去重检查
 *
 * 遍历去重缓存，查找是否存在相同 (device_id, point_id, value) 的记录，
 * 且缓存记录的时间戳与当前点在 window_ms 窗口内。
 *
 * @return 1=重复, 0=不重复
 *
 * 比较逻辑：
 *   - device_id + point_id 必须完全匹配
 *   - 浮点值在 1e-6 误差内视为相同
 *   - 时间戳差值在 window_ms 内
 */
static int check_duplicate(const data_point_t *point, int64_t window_ms)
{
    for (int i = 0; i < g_dedup_count; i++) {
        dedup_entry_t *e = &g_dedup_cache[i];

        /* 设备 ID 和点位 ID 必须匹配 */
        if (strcmp(e->device_id, point->device_id) != 0) continue;
        if (strcmp(e->point_id, point->point_id) != 0) continue;

        /* 时间窗口检查 */
        int64_t diff = point->timestamp - e->timestamp;
        if (diff < 0) diff = -diff;
        if (diff > window_ms) continue;

        /* 值比较 */
        double cur_val;
        switch (point->type) {
        case DATA_TYPE_INT:    cur_val = (double)point->value.int_val;    break;
        case DATA_TYPE_FLOAT:  cur_val = point->value.float_val;           break;
        case DATA_TYPE_BOOL:   cur_val = (double)point->value.bool_val;    break;
        case DATA_TYPE_STRING:
            /* 字符串类型：比较字符串内容 */
            if (strcmp(point->value.str_val, e->device_id) == 0) {
                /* 这里用 device_id 作占位不妥 —— 需要另外存字符串值 */
            }
            /* 字符串类型暂不去重 */
            return 0;
        default:
            return 0;
        }

        if (fabs(cur_val - e->float_value) < 1e-6) {
            return 1;  /* 重复 */
        }
    }

    return 0;  /* 不重复（或字符串类型） */
}

/**
 * @brief 将数据点加入去重缓存
 *
 * 缓存为环形覆盖：满时覆盖最旧的条目（g_dedup_count 回绕）。
 */
static void add_dedup_entry(const data_point_t *point)
{
    int idx;

    if (g_dedup_count < MAX_DEDUP_CACHE) {
        idx = g_dedup_count;
        g_dedup_count++;
    } else {
        /* 缓存满，覆盖最旧条目（时间戳最小的） */
        idx = 0;
        int64_t oldest = g_dedup_cache[0].timestamp;
        for (int i = 1; i < MAX_DEDUP_CACHE; i++) {
            if (g_dedup_cache[i].timestamp < oldest) {
                oldest = g_dedup_cache[i].timestamp;
                idx = i;
            }
        }
    }

    dedup_entry_t *e = &g_dedup_cache[idx];
    common_strncpy(e->device_id, point->device_id, MAX_DEVICE_ID_LEN);
    common_strncpy(e->point_id, point->point_id, MAX_POINT_ID_LEN);

    switch (point->type) {
    case DATA_TYPE_INT:   e->float_value = (double)point->value.int_val;   break;
    case DATA_TYPE_FLOAT: e->float_value = point->value.float_val;          break;
    case DATA_TYPE_BOOL:  e->float_value = (double)point->value.bool_val;   break;
    default:              e->float_value = 0.0; break;
    }

    e->timestamp = point->timestamp;
}

/**
 * @brief 将单条数据点序列化为 cJSON 对象并追加到数组中
 *
 * JSON 格式示例：
 *   {"device_id":"dev-001","point_id":"temp","type":"float",
 *    "value":25.50,"timestamp":1700000000000,"quality":0,"status":0}
 */
static void serialize_point(cJSON *arr, const data_point_t *point)
{
    cJSON *obj = cJSON_CreateObject();

    cJSON_AddStringToObject(obj, "device_id", point->device_id);
    cJSON_AddStringToObject(obj, "point_id",  point->point_id);

    /* 数据类型和值 */
    switch (point->type) {
    case DATA_TYPE_INT:
        cJSON_AddStringToObject(obj, "type", "int");
        cJSON_AddNumberToObject(obj, "value", (double)point->value.int_val);
        break;
    case DATA_TYPE_FLOAT:
        cJSON_AddStringToObject(obj, "type", "float");
        cJSON_AddNumberToObject(obj, "value", point->value.float_val);
        break;
    case DATA_TYPE_BOOL:
        cJSON_AddStringToObject(obj, "type", "bool");
        cJSON_AddNumberToObject(obj, "value", point->value.bool_val);
        break;
    case DATA_TYPE_STRING:
        cJSON_AddStringToObject(obj, "type", "string");
        cJSON_AddStringToObject(obj, "value", point->value.str_val);
        break;
    }

    cJSON_AddNumberToObject(obj, "timestamp", (double)point->timestamp);
    cJSON_AddNumberToObject(obj, "quality",   point->quality);
    cJSON_AddNumberToObject(obj, "status",    point->status_code);

    cJSON_AddItemToArray(arr, obj);
}
