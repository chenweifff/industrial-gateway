/**
 * @file    reporter.h
 * @brief   工业数据采集与上报网关 —— MQTT 上报模块（公开接口）
 *
 * 功能（对应需求 3.5）：
 *   - 通过 MQTT 协议将数据上报至 EMQX broker
 *   - 支持配置 broker 地址、端口、ClientId、用户名、密码、Topic、QoS
 *   - 支持单条上报与批量上报（JSON 格式）
 *   - 连接状态实时监测
 *   - 断线自动重连（Paho 异步 API 内置）
 *   - 上报失败时返回错误码，由网关主控决定是否缓存重发
 *
 * 该模块实现了 common.h 中定义的 reporter_interface_t 接口。
 *
 * MQTT 协议基础（初学者参考）：
 *   - MQTT 是发布/订阅模式的轻量级物联网通信协议
 *   - Broker（代理服务器）：接收消息并转发给订阅者，本项目用 EMQX
 *   - Topic（主题）：消息的分类标签，如 "industrial/gateway/data"
 *   - QoS（服务质量）：
 *       0 = 至多一次（可能丢）
 *       1 = 至少一次（可能重复，本项目默认）
 *       2 = 恰好一次（最可靠但最慢）
 *   - ClientId：客户端唯一标识，同一 broker 上不可重复
 *
 * 依赖：
 *   - common.h（reporter_interface_t, data_point_t）
 *   - logger.h（日志记录）
 *   - MQTTAsync.h（Eclipse Paho MQTT C 异步 API）
 *   - cJSON.h（JSON 序列化）
 */

#ifndef REPORTER_H
#define REPORTER_H

#include "common.h"    /* reporter_interface_t, data_point_t */

/**
 * @brief 创建 MQTT 上报模块实例
 *
 * 使用方法：
 *   reporter_interface_t *rp = reporter_create();
 *   rp->init(rp->ctx, config_json);
 *   rp->connect(rp->ctx);
 *   rp->report(rp->ctx, &point);
 *   rp->report_batch(rp->ctx, points, count);
 *   rp->disconnect(rp->ctx);
 *   rp->destroy(rp->ctx);
 *   free(rp);
 *
 * @return 已分配好的接口对象指针
 */
reporter_interface_t *reporter_create(void);

#endif /* REPORTER_H */
