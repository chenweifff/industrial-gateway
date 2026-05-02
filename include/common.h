/**
 * @file    common.h
 * @brief   工业数据采集与上报网关 —— 通用定义模块
 *
 * 本文件是项目的根基，所有其他模块都依赖本文件。
 * 定义了：
 *   1. 统一的返回码（错误码）
 *   2. 数据类型枚举（整数、浮点、布尔、字符串）
 *   3. 网关 / 设备 / 数据 三级状态枚举
 *   4. 采集点的统一数据结构 data_point_t
 *   5. 三个可替换的抽象接口（数据源、存储、上报）
 *
 * 设计思路（初学者可参考）：
 *   - C 语言通过「结构体 + 函数指针」模拟面向对象的抽象接口。
 *   - 上层模块只依赖接口，不关心底层实现，方便以后替换（如模拟->真实设备）。
 */

#ifndef COMMON_H
#define COMMON_H

/*
 * 启用 POSIX.1b 扩展（199309L 对应实时扩展），
 * 使 clock_gettime() / struct timespec 可用。
 * 必须在所有系统头文件 include 之前定义。
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 199309L
#endif

#include <stdint.h>   /* int64_t, uint8_t 等固定宽度整数 */
#include <stddef.h>   /* size_t */
#include <time.h>     /* time_t, clock_gettime(), struct timespec */

/* ================================================================
 *  一、通用返回码
 * ================================================================
 * 所有模块的函数统一返回 int，0 表示成功，负数表示错误类型。
 * 这样调用方只需判断 >= 0 还是 < 0 即可。
 */
#define RET_OK                   0      /* 成功 */
#define RET_ERR                 -1      /* 通用错误 */
#define RET_ERR_NULL_PTR        -2      /* 空指针 */
#define RET_ERR_INVALID_PARAM   -3      /* 无效参数 */
#define RET_ERR_TIMEOUT         -4      /* 超时 */
#define RET_ERR_NO_MEMORY       -5      /* 内存不足 */
#define RET_ERR_NOT_FOUND       -6      /* 未找到（如配置项不存在） */
#define RET_ERR_IO              -7      /* 文件 / 网络 IO 错误 */
#define RET_ERR_PARSE           -8      /* 数据解析失败 */
#define RET_ERR_FULL            -9      /* 缓冲区 / 队列已满 */
#define RET_ERR_DISCONNECTED    -10     /* 连接已断开 */
#define RET_ERR_ALREADY_EXISTS  -11     /* 已存在（如重复数据） */

/* ================================================================
 *  二、数据类型枚举
 * ================================================================
 * 对应需求 3.1：支持生成整数、浮点、布尔、字符串类型数据
 */
typedef enum {
    DATA_TYPE_INT    = 0,   /* 有符号 64 位整数 */
    DATA_TYPE_FLOAT  = 1,   /* 双精度浮点数 */
    DATA_TYPE_BOOL   = 2,   /* 布尔值（0/1） */
    DATA_TYPE_STRING = 3    /* 字符串（最大 255 字符） */
} data_type_t;

/* ================================================================
 *  三、网关运行状态
 * ================================================================
 * 对应需求第 5 节「网关运行状态」：
 *   初始化中 -> 运行中 -> 停止中
 *   运行中可能因网络 / MQTT 断开、数据积压、故障等原因进入子状态
 */
typedef enum {
    GW_STATUS_INITIALIZING    = 0,   /* 初始化中：启动后加载配置、初始化各模块 */
    GW_STATUS_RUNNING         = 1,   /* 运行中：正常采集、处理、上报 */
    GW_STATUS_NETWORK_DISCONNECTED = 2,  /* 网络断开：无法连接 MQTT broker */
    GW_STATUS_MQTT_DISCONNECTED = 3,    /* MQTT 断开：其他原因导致的 MQTT 断连 */
    GW_STATUS_DATA_BACKLOG    = 4,   /* 数据积压中：本地缓存堆积过多 */
    GW_STATUS_FAULT           = 5,   /* 故障状态：严重错误，影响正常运行 */
    GW_STATUS_STOPPING        = 6    /* 停止中：正常退出流程 */
} gateway_status_t;

/* ================================================================
 *  四、设备状态
 * ================================================================
 * 对应需求第 5 节「设备状态」：在线、离线、异常、未知
 */
typedef enum {
    DEV_STATUS_ONLINE   = 0,   /* 在线：设备正常工作 */
    DEV_STATUS_OFFLINE  = 1,   /* 离线：设备无响应 */
    DEV_STATUS_ABNORMAL = 2,   /* 异常：设备有响应但数据异常 */
    DEV_STATUS_UNKNOWN  = 3    /* 未知：无法判断设备状态 */
} device_status_t;

/* ================================================================
 *  五、数据状态
 * ================================================================
 * 对应需求第 5 节「数据状态」：
 *   待上报 -> 已上报 / 上报失败 / 无效 / 已丢弃
 */
typedef enum {
    DATA_STATUS_PENDING        = 0,   /* 待上报：已写入缓存，等待发送 */
    DATA_STATUS_REPORTED       = 1,   /* 已上报：成功发送至 MQTT broker */
    DATA_STATUS_REPORT_FAILED  = 2,   /* 上报失败：尝试发送但未成功 */
    DATA_STATUS_INVALID        = 3,   /* 无效：数据校验未通过 */
    DATA_STATUS_DISCARDED      = 4    /* 已丢弃：超出容量上限或其他原因被丢弃 */
} data_status_t;

/* ================================================================
 *  六、质量标记
 * ================================================================
 * 用于标记数据可信度，附加在每条采集数据上
 */
typedef enum {
    QUALITY_GOOD              = 0,   /* 正常质量 */
    QUALITY_UNCERTAIN         = 1,   /* 不确定（如传感器数据波动） */
    QUALITY_BAD               = 2,   /* 坏数据（如超范围、传感器故障） */
    QUALITY_SIMULATED         = 3    /* 模拟数据标记 */
} quality_flag_t;

/* 字符串长度上限 */
#define MAX_DEVICE_ID_LEN       64    /* 设备标识最大长度 */
#define MAX_POINT_ID_LEN        64    /* 点位标识最大长度 */
#define MAX_STRING_VALUE_LEN   256    /* 字符串类型数据最大长度 */
#define MAX_TOPIC_LEN          256    /* MQTT Topic 最大长度 */
#define MAX_PATH_LEN           512    /* 文件路径最大长度 */

/* ================================================================
 *  七、统一数据点结构体
 * ================================================================
 * 这是整个系统中流转的核心数据结构。
 * 对应需求 3.2：采集结果应包含
 *   - 设备标识 (device_id)    —— 来自哪个设备
 *   - 点位标识 (point_id)     —— 设备上的哪个测量点
 *   - 数据值   (value)        —— 实际数值（union 按类型存取）
 *   - 时间戳   (timestamp)    —— 采集时间（毫秒级 epoch）
 *   - 状态码   (status_code)  —— 自定义状态码
 *   - 质量标记 (quality)       —— 数据质量
 */
typedef struct {
    /* ----- 路由信息 ----- */
    char     device_id[MAX_DEVICE_ID_LEN];   /* 设备唯一标识，如 "dev-001" */
    char     point_id[MAX_POINT_ID_LEN];     /* 点位唯一标识，如 "temp-A"   */

    /* ----- 数据类型与值 -----
     * union 在同一块内存中保存多种类型，但每次只用其中一种。
     * 读取时根据 type 字段判断当前存放的是哪种类型。 */
    data_type_t type;                         /* 数据类型 */
    union {
        int64_t   int_val;                    /* DATA_TYPE_INT    */
        double    float_val;                  /* DATA_TYPE_FLOAT  */
        int       bool_val;                   /* DATA_TYPE_BOOL   （0 或 1） */
        char      str_val[MAX_STRING_VALUE_LEN]; /* DATA_TYPE_STRING */
    } value;

    /* ----- 元信息 ----- */
    int64_t    timestamp;     /* 采集时间戳（Unix 毫秒） */
    int        status_code;   /* 采集状态码：0=成功，其他=异常类型 */
    quality_flag_t quality;   /* 数据质量标记 */

    /* ----- 存储用 ----- */
    int64_t    id;            /* 数据库主键 ID（内存中使用时忽略） */
    data_status_t data_status;/* 上报状态（用于缓存管理） */
} data_point_t;

/* ================================================================
 *  八、数据源抽象接口
 * ================================================================
 * 对应需求第 7 节：数据源接口应可替换（模拟数据、真实串口、文件输入）。
 *
 * C 语言实现多态的方式（初学者参考）：
 *   定义一个结构体，里面全是「函数指针」。
 *   每种具体实现（如 simulator）负责填充这些函数指针。
 *   上层调用时通过指针间接调用，不直接依赖具体模块。
 */
typedef struct data_source_interface {
    /*
     * 内部上下文指针（指向 simulator 或其他数据源的实例数据），
     * 每个接口函数都会收到这个指针，类似 C++ 的 this。
     */
    void *ctx;

    /* 初始化数据源，config_json 为 JSON 格式的配置字符串 */
    int (*init)(void *ctx, const char *config_json);

    /* 启动数据生成（可能开启内部线程） */
    int (*start)(void *ctx);

    /* 停止数据生成，释放内部线程资源 */
    int (*stop)(void *ctx);

    /*
     * 采集数据：
     *   points     [out]  存放采集结果
     *   max_count  [in]   points 数组的容量
     *   actual     [out]  实际采集到的条数
     * 返回 RET_OK 成功，负数失败。
     */
    int (*acquire)(void *ctx, data_point_t *points, int max_count, int *actual);

    /* 销毁数据源，释放所有资源 */
    void (*destroy)(void *ctx);

} data_source_interface_t;

/* ================================================================
 *  九、存储抽象接口
 * ================================================================
 * 对应需求第 7 节：存储接口应与 SQLite 解耦。
 * 目前用 SQLite 实现，将来可能换成其他嵌入式数据库或文件存储。
 */
typedef struct storage_interface {
    void *ctx;

    /* 初始化存储（如打开 SQLite 数据库文件） */
    int (*init)(void *ctx, const char *db_path);

    /* 插入单条数据 */
    int (*insert)(void *ctx, const data_point_t *point);

    /* 批量插入 */
    int (*insert_batch)(void *ctx, const data_point_t *points, int count);

    /*
     * 查询待上报数据：
     *   从缓存中取出最多 max_count 条状态为 DATA_STATUS_PENDING 的数据
     */
    int (*query_pending)(void *ctx, data_point_t *points, int max_count, int *actual);

    /* 标记一批数据为「已上报」 */
    int (*mark_reported)(void *ctx, const int64_t *ids, int count);

    /* 标记单条数据为「上报失败」 */
    int (*mark_failed)(void *ctx, int64_t id);

    /*
     * 清理过期数据：
     *   删除 timestamp 早于 before_timestamp 且已上报 / 已丢弃的数据
     */
    int (*cleanup)(void *ctx, int64_t before_timestamp);

    /* 获取当前待上报数据条数（用于积压监控） */
    int (*get_pending_count)(void *ctx, int *count);

    /* 关闭存储，释放资源 */
    void (*destroy)(void *ctx);

} storage_interface_t;

/* ================================================================
 *  十、上报抽象接口
 * ================================================================
 * 对应需求第 7 节：上报接口应支持 MQTT，为 HTTP 等预留扩展。
 * 当前用 Eclipse Paho MQTT（异步 API）实现。
 */
typedef struct reporter_interface {
    void *ctx;

    /* 初始化上报模块（从 JSON 配置字符串加载 broker 信息等） */
    int (*init)(void *ctx, const char *config_json);

    /* 连接到 MQTT broker */
    int (*connect)(void *ctx);

    /* 断开连接 */
    int (*disconnect)(void *ctx);

    /* 上报单条数据 */
    int (*report)(void *ctx, const data_point_t *point);

    /* 批量上报数据 */
    int (*report_batch)(void *ctx, const data_point_t *points, int count);

    /* 查询当前是否已连接 */
    int (*is_connected)(void *ctx);

    /* 销毁上报模块，释放资源 */
    void (*destroy)(void *ctx);

} reporter_interface_t;

/* ================================================================
 *  十一、工具宏
 * ================================================================ */

/* 获取当前时间戳（毫秒） */
static inline int64_t common_timestamp_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* 安全的字符串拷贝（带截断保护，始终 '\0' 结尾） */
#include <string.h>
static inline void common_strncpy(char *dst, const char *src, size_t size)
{
    if (dst && src && size > 0) {
        strncpy(dst, src, size - 1);
        dst[size - 1] = '\0';
    }
}

#endif /* COMMON_H */
