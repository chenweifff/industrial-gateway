/**
 * @file    collector.h
 * @brief   工业数据采集与上报网关 —— 数据采集与统一封装模块（公开接口）
 *
 * 功能（对应需求 3.2）：
 *   - 定时从数据源获取数据，封装为统一的 data_point_t 结构
 *   - 支持采集超时检测与重试机制
 *   - 对采集失败、超时和无效数据返回明确的状态码
 *   - 作为数据源与处理器之间的桥梁
 *
 * 设计思路（初学者参考）：
 *   采集模块不直接生成数据，而是通过 data_source_interface_t 调用
 *   底层数据源（当前为模拟器，以后可换成真实串口）。
 *   这种「依赖倒置」的设计使采集逻辑与具体硬件解耦。
 *
 * 依赖：
 *   - common.h（data_source_interface_t, data_point_t）
 *   - logger.h（日志记录）
 *   - cJSON.h（JSON 配置解析）
 */

#ifndef COLLECTOR_H
#define COLLECTOR_H

#include "common.h"    /* data_source_interface_t, data_point_t */

/**
 * @brief 采集模块配置
 */
typedef struct {
    int timeout_ms;      /* 采集超时时间（毫秒），0 = 不设超时 */
    int retry_count;     /* 采集失败后的重试次数 */
} collector_config_t;

/**
 * @brief 初始化采集模块
 * @param config_json  JSON 格式的配置（可选，传 NULL 使用默认值）
 *                    例: {"timeout_ms":5000, "retry_count":3}
 * @return RET_OK 成功
 */
int collector_init(const char *config_json);

/**
 * @brief 销毁采集模块
 */
void collector_destroy(void);

/**
 * @brief 执行一次采集
 *
 * 从数据源拉取数据，内部处理超时和重试。
 *
 * @param source     数据源接口指针（由外部注入，如 simulator_create() 的返回值）
 * @param points     [out] 采集结果缓冲区
 * @param max_count  [in]  缓冲区最大容量
 * @param actual     [out] 实际采集到的数据条数
 * @return RET_OK 成功，负数失败
 *
 * 采集逻辑：
 *   1. 尝试调用 source->acquire() 获取数据
 *   2. 如果返回 0 条数据（或超时），按 retry_count 重试
 *   3. 遍历返回的数据，对异常数据打上状态标记
 *   4. 返回最终结果
 *
 * 注意：重试间隔每次 100ms。
 */
int collector_collect(data_source_interface_t *source,
                      data_point_t *points, int max_count, int *actual);

/**
 * @brief 获取采集模块的配置（只读）
 */
const collector_config_t *collector_get_config(void);

#endif /* COLLECTOR_H */
