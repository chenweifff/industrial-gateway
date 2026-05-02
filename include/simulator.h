/**
 * @file    simulator.h
 * @brief   工业数据采集与上报网关 —— 模拟数据模块（公开接口）
 *
 * 功能（对应需求 3.1）：
 *   - 模拟多设备、多点位的工业传感器数据生成
 *   - 支持 7 种数据变化规则（固定值、随机、随机漫步、递增、递减、周期波动、触发）
 *   - 支持 4 种数据类型（整数、浮点、布尔、字符串）
 *   - 支持异常数据注入（超范围值、空值、非法格式等）
 *   - 支持设备状态模拟（在线、离线、异常）
 *   - 后台线程定时生成，通过环形缓冲区传递给采集模块
 *
 * 该模块实现了 common.h 中定义的 data_source_interface_t 接口，
 * 上层模块通过该接口调用，不直接依赖 simulator 内部实现。
 *
 * 依赖：
 *   - common.h（抽象接口、数据类型）
 *   - logger.h（日志记录）
 *   - cJSON.h（JSON 配置解析）
 *   - pthread（多线程 + 互斥锁 + 条件变量）
 */

#ifndef SIMULATOR_H
#define SIMULATOR_H

#include "common.h"    /* data_source_interface_t, data_point_t */

/**
 * @brief 创建模拟数据源实例
 *
 * 分配并初始化一个实现 data_source_interface_t 接口的模拟器对象。
 * 返回的接口指针中，ctx 指向内部状态，所有函数指针已就位。
 *
 * 使用方法：
 *   data_source_interface_t *ds = simulator_create();
 *   ds->init(ds->ctx, config_json);
 *   ds->start(ds->ctx);
 *   ds->acquire(ds->ctx, points, 10, &count);
 *   ds->stop(ds->ctx);
 *   ds->destroy(ds->ctx);
 *
 * @return 已分配好函数指针和上下文的接口对象指针，失败返回 NULL
 */
data_source_interface_t *simulator_create(void);

#endif /* SIMULATOR_H */
