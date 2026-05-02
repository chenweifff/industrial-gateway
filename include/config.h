/**
 * @file    config.h
 * @brief   工业数据采集与上报网关 —— 配置管理模块（公开接口）
 *
 * 功能（对应需求 3.6）：
 *   - 从 JSON 文件加载全部运行参数
 *   - 配置项校验（文件存在性、JSON 合法性、必填字段检查）
 *   - 默认值加载（缺失字段自动使用内置默认值）
 *   - 提供分模块获取配置的接口（每个子模块只拿自己需要的部分）
 *   - 预留热更新接口 config_reload()
 *   - 支持持久化保存 config_save()
 *
 * 设计思路（初学者参考）：
 *   配置管理器内部维护一棵 cJSON 树，所有配置以 JSON 格式在内存中存储。
 *   各子模块通过 config_get_section() 获取自己的 JSON 片段，
 *   自行解析所需字段。这样新增配置项时无需修改 config 模块代码，
 *   只需在子模块中添加解析即可。
 *
 * 依赖：
 *   - common.h（返回码）
 *   - logger.h（日志记录）
 *   - cJSON.h（JSON 解析）
 */

#ifndef CONFIG_H
#define CONFIG_H

#include "common.h"      /* RET_OK 等 */
#include <stddef.h>      /* size_t */

/* ================================================================
 *  API 函数
 * ================================================================ */

/**
 * @brief 从 JSON 文件加载配置
 * @param file_path  配置文件路径，如 "config/gateway.json"
 * @return RET_OK 成功，负数失败
 *
 * 加载流程：
 *   1. 打开文件
 *   2. 读取全部内容到内存
 *   3. 用 cJSON 解析为 JSON 树
 *   4. 校验顶层结构（必选字段是否存在）
 *   5. 记录加载成功日志
 */
int config_load(const char *file_path);

/**
 * @brief 重新加载配置（热更新预留）
 * @return RET_OK 成功，负数失败
 *
 * 实现方式：直接再次调用 config_load() 覆盖内存中的配置树。
 * 当前版本仅提供接口框架，后续可扩展为：
 *   - 监听配置文件变化（inotify）
 *   - 部分模块动态更新（如日志级别热修改）
 *   - 推送新配置到各模块
 */
int config_reload(void);

/**
 * @brief 将当前内存配置写回文件（持久化保存）
 * @param file_path  写入目标路径，传 NULL 则写回原文件
 * @return RET_OK 成功，负数失败
 */
int config_save(const char *file_path);

/**
 * @brief 释放配置占用的内存
 * 程序退出前调用。
 */
void config_destroy(void);

/* ================================================================
 *  分模块配置获取（每个模块取自己需要的 JSON 片段）
 * ================================================================ */

/**
 * @brief 获取指定模块的配置（JSON 字符串）
 * @param section   模块名，如 "simulator"、"reporter"、"storage"
 * @param json_out  输出缓冲区，存放 JSON 字符串
 * @param size      输出缓冲区大小
 * @return RET_OK 成功，RET_ERR_NOT_FOUND 模块不存在，其他为错误
 *
 * 调用方拿到 JSON 字符串后，可再调用 cJSON_Parse() 解析自己需要的字段。
 * 例如 reporter 模块调用：
 *   config_get_section("reporter", buf, sizeof(buf));
 *   得到: {"broker_address":"tcp://localhost:1883", ...}
 */
int config_get_section(const char *section, char *json_out, size_t size);

/**
 * @brief 获取单个字符串配置项（全局/顶层字段）
 * @param key       配置键名，如 "log_level"
 * @param default_val 默认值（配置中没有时返回此值）
 * @param out       输出缓冲区
 * @param size      缓冲区大小
 * @return RET_OK 成功，RET_ERR_NOT_FOUND 不存在（此时 out 中为 default_val）
 */
int config_get_string(const char *key, const char *default_val,
                      char *out, size_t size);

/**
 * @brief 获取单个整数配置项（全局/顶层字段）
 * @param key       配置键名
 * @param default_val 默认值
 * @return 配置值或默认值
 */
int config_get_int(const char *key, int default_val);

/**
 * @brief 获取单个布尔配置项（全局/顶层字段）
 * @param key       配置键名
 * @param default_val 默认值
 * @return 配置值或默认值（1=true, 0=false）
 */
int config_get_bool(const char *key, int default_val);

/**
 * @brief 获取配置的原始 cJSON 根节点（高级用法）
 *
 * 返回内存中的整个 JSON 树根节点指针。
 * 调用方可直接操作 cJSON API 查询任意字段。
 * 注意：不可释放此指针，由 config_destroy() 统一管理。
 * 如果配置未加载，返回 NULL。
 */
struct cJSON *config_get_root(void);

#endif /* CONFIG_H */
