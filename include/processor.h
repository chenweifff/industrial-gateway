/**
 * @file    processor.h
 * @brief   工业数据采集与上报网关 —— 数据处理模块（公开接口）
 *
 * 功能（对应需求 3.3）：
 *   - 类型转换与格式标准化
 *   - 范围校验（值是否在规定的 min~max 区间）
 *   - 异常值标记（超范围、非法值 → 标记为 INVALID）
 *   - 重复数据判断（相同设备+点位+数值+时间窗口 → 去重）
 *   - 基础滤波（按质量阈值过滤坏数据）
 *   - 按规则打包为 JSON 上报消息
 *
 * 在管道中的位置：
 *   [collector] → [processor] → [storage + reporter]
 *                    ↑
 *            输入原始 data_point_t[]
 *            输出校验、去重、过滤后的 data_point_t[]
 *            + 支持打包为 JSON 字符串
 *
 * 依赖：
 *   - common.h（data_point_t、返回码）
 *   - logger.h（日志记录）
 *   - cJSON.h（JSON 序列化）
 */

#ifndef PROCESSOR_H
#define PROCESSOR_H

#include "common.h"    /* data_point_t */

/**
 * @brief 处理器配置
 */
typedef struct {
    int   enable_range_check;   /* 是否开启范围校验 */
    int   enable_dedup;         /* 是否开启去重 */
    int   enable_filter;        /* 是否开启质量过滤 */
    double filter_threshold;    /* 质量阈值（低于此值的数据被丢弃） */
    int   dedup_window_ms;      /* 去重时间窗口（毫秒），默认 5000 */
} processor_config_t;

/**
 * @brief 单条范围校验规则
 *
 * 用于校验某个点位的数据值是否在合理区间。
 * 规则由外部配置（模拟器的 data_rules）提供。
 */
typedef struct {
    char   point_type[64];  /* 点位类型，如 "temperature" */
    double min;             /* 最小值（含） */
    double max;             /* 最大值（含） */
} range_rule_t;

/* ================================================================
 *  API 函数
 * ================================================================ */

/**
 * @brief 初始化数据处理模块
 * @param config_json  JSON 配置，如:
 *        {"enable_range_check":true, "enable_dedup":true,
 *         "enable_filter":false, "filter_threshold":0.5,
 *         "dedup_window_ms":5000}
 * @return RET_OK 成功
 */
int processor_init(const char *config_json);

/**
 * @brief 加载范围校验规则（从模拟器配置中的 data_rules 解析）
 *
 * 每条规则定义了一个点位类型的合法值区间。
 *
 * @param rules_json  JSON 数组字符串，如:
 *        [{"point_type":"temp","min":0,"max":100},
 *         {"point_type":"press","min":0,"max":10}]
 * @return RET_OK 成功
 */
int processor_load_range_rules(const char *rules_json);

/**
 * @brief 处理一批数据点
 *
 * 依次执行：
 *   1. 范围校验 —— 检查值是否在合法区间，超标则标记为 INVALID
 *   2. 去重       —— 相同设备+点位+值在时间窗口内的仅保留首次
 *   3. 质量过滤   —— quality < filter_threshold 的数据标记为 DISCARDED
 *
 * @param points      [in/out] 待处理的数据点数组（会原地修改状态字段）
 * @param count       [in]     输入数据条数
 * @param valid_count [out]    处理后有效数据条数（含 INVALID/DISCARDED 标记）
 * @return RET_OK 成功
 *
 * 注意：处理后的数组中，被判定为 INVALID 或 DISCARDED 的数据点
 *       不会被删除（保持数组长度不变），调用方根据 data_status 字段自行过滤。
 */
int processor_process(data_point_t *points, int count, int *valid_count);

/**
 * @brief 将数据点数组打包为 JSON 字符串（用于 MQTT 上报）
 *
 * 输出格式：
 *   {
 *     "gateway": "gateway-001",
 *     "ts": 1700000000000,
 *     "count": 3,
 *     "data": [
 *       {"device_id":"dev-001","point_id":"temp","type":"float",
 *        "value":25.50,"timestamp":1700000000000,"quality":0},
 *       ...
 *     ]
 *   }
 *
 * @param points     数据点数组
 * @param count      数据条数
 * @param json_out   [out] JSON 字符串缓冲区
 * @param size       缓冲区大小
 * @return RET_OK 成功
 */
int processor_pack_to_json(const data_point_t *points, int count,
                           char *json_out, size_t size);

/**
 * @brief 销毁处理模块
 */
void processor_destroy(void);

/**
 * @brief 获取处理器配置（只读）
 */
const processor_config_t *processor_get_config(void);

#endif /* PROCESSOR_H */
