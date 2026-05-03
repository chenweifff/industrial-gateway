/**
 * @file    storage.h
 * @brief   工业数据采集与上报网关 —— 本地存储模块（公开接口）
 *
 * 功能（对应需求 3.4）：
 *   - 使用 SQLite 保存待上报数据、上报状态
 *   - 支持数据写入、查询、状态更新和历史清理
 *   - 支持网络异常时的数据暂存与恢复后补发
 *   - 支持容量控制与积压管理（超上限时丢弃最旧数据）
 *
 * 该模块实现了 common.h 中定义的 storage_interface_t 接口，
 * 上层通过接口调用，与 SQLite 具体实现解耦。
 *
 * SQLite WAL 模式：
 *   - 启用 WAL (Write-Ahead Logging)，读写不互斥
 *   - Navicat 通过 SSH 隧道可同时读取，不会被写锁阻塞
 *
 * 依赖：
 *   - common.h（storage_interface_t, data_point_t）
 *   - logger.h（日志记录）
 *   - SQLite3（系统库 libsqlite3）
 */

#ifndef STORAGE_H
#define STORAGE_H

#include "common.h"    /* storage_interface_t, data_point_t */

/**
 * @brief 创建本地存储实例
 *
 * 分配并初始化一个实现 storage_interface_t 接口的 SQLite 存储对象。
 *
 * 使用方法：
 *   storage_interface_t *st = storage_create();
 *   st->init(st->ctx, "./data/gateway.db");
 *   st->insert(st->ctx, &point);
 *   st->query_pending(st->ctx, points, 100, &count);
 *   st->mark_reported(st->ctx, ids, count);
 *   st->destroy(st->ctx);
 *   free(st);
 *
 * @return 已分配好的接口对象指针，失败返回 NULL
 */
storage_interface_t *storage_create(void);

#endif /* STORAGE_H */
