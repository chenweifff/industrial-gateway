/**
 * @file    simulator.c
 * @brief   工业数据采集与上报网关 —— 模拟数据模块（实现）
 *
 * 内部架构（初学者参考）：
 *
 *   ┌──────────────────────────────────────┐
 *   │         simulator_create()            │
 *   │  分配接口 + 内部状态，返回接口指针      │
 *   └──────────────┬───────────────────────┘
 *                  │
 *   ┌──────────────▼───────────────────────┐
 *   │              init()                   │
 *   │  解析 JSON 配置，创建设备和点位        │
 *   │  初始化环形缓冲区、互斥锁、条件变量     │
 *   └──────────────┬───────────────────────┘
 *                  │
 *   ┌──────────────▼───────────────────────┐
 *   │             start()                   │
 *   │  创建后台线程 generate_thread()       │
 *   │  循环：sleep → 生成数据 → 写入缓冲区   │
 *   └──────────────┬───────────────────────┘
 *                  │
 *   ┌──────────────▼───────────────────────┐
 *   │           acquire()                   │
 *   │  从环形缓冲区取出数据，返回给调用方     │
 *   └──────────────────────────────────────┘
 *
 * 线程安全设计：
 *   - buf_mutex 保护环形缓冲区（生产者-消费者模型）
 *   - buf_cond  条件变量，数据就绪时通知消费者
 *   - running    原子标志位（volatile + 锁保护），控制线程启停
 *
 * 环形缓冲区（ring buffer）：
 *   固定大小的循环数组，生产者写入 head，消费者读取 tail。
 *   当缓冲区满时，丢掉最旧的数据（覆盖 tail），确保不阻塞生成。
 */

#include "simulator.h"
#include "logger.h"
#include "cJSON.h"

#include <stdio.h>       /* snprintf */
#include <stdlib.h>      /* malloc, free, rand, srand */
#include <string.h>      /* memset, strcmp */
#include <math.h>        /* sin, fmod, fabs */
#include <pthread.h>     /* 多线程 */
#include <unistd.h>      /* usleep */
#include <errno.h>       /* errno */

/* ---- 常量和默认值 ---- */
#define DEFAULT_BUFFER_SIZE       1024   /* 环形缓冲区容量（条数） */
#define DEFAULT_SAMPLING_MS       1000   /* 默认采样间隔（毫秒） */
#define MAX_DEVICES               64     /* 最大设备数 */
#define MAX_POINTS_PER_DEVICE     32     /* 每设备最大点数 */
#define STRING_VALUE_POOL_SIZE    8      /* 字符串类型可选值数量 */

/* ---- 数据变化规则枚举 ---- */
typedef enum {
    RULE_FIXED       = 0,   /* 固定值：永远返回配置中的 value */
    RULE_RANDOM      = 1,   /* 随机值：在 [min, max] 内均匀随机 */
    RULE_RANDOM_WALK = 2,   /* 随机漫步：在上一值基础上随机加减 step */
    RULE_INCREMENT   = 3,   /* 递增：每次加 step，超 max 回绕到 min */
    RULE_DECREMENT   = 4,   /* 递减：每次减 step，低过 min 回绕到 max */
    RULE_CYCLE       = 5,   /* 周期波动：正弦波在 [min, max] 之间变化 */
    RULE_TRIGGER     = 6    /* 规则触发：在 min 和 max 之间随机切换 */
} rule_type_t;

/* ---- 单个点位的数据规则 ---- */
typedef struct {
    char        point_type[64];   /* 点位类型名，如 "temperature" */
    data_type_t data_type;        /* 数据类型 */
    rule_type_t rule;             /* 变化规则 */
    double      min;              /* 最小值 */
    double      max;              /* 最大值 */
    double      step;             /* 步长（用于 increment / decrement / random_walk） */
    double      current;          /* 当前值（用于有状态规则） */
    int         cycle_period;     /* 周期长度（采样次数），用于 cycle 规则 */
    int         cycle_counter;    /* 周期内计数器 */
    int         counter;          /* 通用计数器 */
} point_config_t;

/* ---- 模拟设备 ---- */
typedef struct {
    char            device_id[MAX_DEVICE_ID_LEN];   /* 设备标识 */
    device_status_t status;                          /* 当前状态 */
    point_config_t  *points;                         /* 点位配置数组 */
    int             point_count;                     /* 点位数量 */
} simulated_device_t;

/* ---- 环形缓冲区 ---- */
typedef struct {
    data_point_t *data;      /* 数据存储数组 */
    int           capacity;  /* 总容量 */
    int           head;      /* 生产者写入位置 */
    int           tail;      /* 消费者读取位置 */
    int           count;     /* 当前元素数量 */
} ring_buffer_t;

/* ---- 模拟器内部状态 ---- */
typedef struct {
    simulated_device_t *devices;        /* 设备数组 */
    int                 device_count;   /* 设备数量 */
    int                 points_per_device; /* 每设备点数 */
    int                 sampling_interval_ms; /* 采样间隔（毫秒） */
    double              abnormal_probability; /* 异常数据概率 */
    double              offline_probability;  /* 设备离线概率 */

    ring_buffer_t       buffer;         /* 环形缓冲区 */
    pthread_mutex_t     buf_mutex;      /* 缓冲区互斥锁 */
    pthread_cond_t      buf_cond;       /* 条件变量（数据就绪通知） */

    pthread_t           thread;         /* 生成线程 */
    int                 running;        /* 运行标志（1=运行，0=停止） */
    int                 initialized;    /* 是否已初始化 */
} simulator_ctx_t;

/* ---- 内部函数声明 ---- */
static int  sim_init(void *ctx, const char *config_json);
static int  sim_start(void *ctx);
static int  sim_stop(void *ctx);
static int  sim_acquire(void *ctx, data_point_t *points, int max_count, int *actual);
static void sim_destroy(void *ctx);

static int  parse_config(simulator_ctx_t *sim, const char *config_json);
static int  create_devices(simulator_ctx_t *sim, cJSON *rules_array);
static void generate_data_point(const simulated_device_t *dev,
                                const point_config_t *pt, data_point_t *out);
static void apply_rule(point_config_t *pt);
static void inject_abnormality(data_point_t *point, int abnormal);
static device_status_t random_device_status(double offline_prob);
static void *generate_thread(void *arg);

static int  ring_buffer_init(ring_buffer_t *rb, int capacity);
static void ring_buffer_destroy(ring_buffer_t *rb);
static int  ring_buffer_put(ring_buffer_t *rb, const data_point_t *point);
static int  ring_buffer_get(ring_buffer_t *rb, data_point_t *points, int max_count);

/* ---- 字符串值池（用于 STRING 类型模拟）---- */
static const char *string_pool[] = {
    "normal", "warning", "alarm", "error",
    "open", "closed", "running", "stopped"
};

/* ================================================================
 *  公开接口：创建模拟器实例
 * ================================================================ */

data_source_interface_t *simulator_create(void)
{
    data_source_interface_t *iface;
    simulator_ctx_t *ctx;

    /* 分配接口结构体 */
    iface = (data_source_interface_t *)malloc(sizeof(data_source_interface_t));
    if (!iface) {
        return NULL;
    }

    /* 分配内部上下文 */
    ctx = (simulator_ctx_t *)calloc(1, sizeof(simulator_ctx_t));
    if (!ctx) {
        free(iface);
        return NULL;
    }

    /* 绑定函数指针 */
    iface->ctx     = ctx;
    iface->init    = sim_init;
    iface->start   = sim_start;
    iface->stop    = sim_stop;
    iface->acquire = sim_acquire;
    iface->destroy = sim_destroy;

    /* 初始化随机数种子 */
    srand((unsigned int)time(NULL));

    return iface;
}

/* ================================================================
 *  接口实现：init
 * ================================================================ */

static int sim_init(void *ctx, const char *config_json)
{
    simulator_ctx_t *sim = (simulator_ctx_t *)ctx;

    if (!sim || !config_json) {
        return RET_ERR_INVALID_PARAM;
    }

    LOG_INFO("模拟数据模块初始化...");

    /* 设置默认值 */
    sim->device_count         = 3;
    sim->points_per_device    = 4;
    sim->sampling_interval_ms = DEFAULT_SAMPLING_MS;
    sim->abnormal_probability = 0.02;
    sim->offline_probability  = 0.05;

    /* 解析 JSON 配置 */
    if (strlen(config_json) > 0) {
        if (parse_config(sim, config_json) != RET_OK) {
            LOG_ERROR("模拟器配置解析失败，使用默认值");
        }
    }

    /* 初始化环形缓冲区 */
    if (ring_buffer_init(&sim->buffer, DEFAULT_BUFFER_SIZE) != RET_OK) {
        LOG_ERROR("环形缓冲区初始化失败");
        return RET_ERR_NO_MEMORY;
    }

    /* 初始化互斥锁和条件变量 */
    if (pthread_mutex_init(&sim->buf_mutex, NULL) != 0) {
        LOG_ERROR("互斥锁初始化失败");
        ring_buffer_destroy(&sim->buffer);
        return RET_ERR;
    }
    if (pthread_cond_init(&sim->buf_cond, NULL) != 0) {
        LOG_ERROR("条件变量初始化失败");
        pthread_mutex_destroy(&sim->buf_mutex);
        ring_buffer_destroy(&sim->buffer);
        return RET_ERR;
    }

    sim->initialized = 1;
    LOG_INFO("模拟数据模块初始化完成: %d 设备 x %d 点位, 采样间隔 %d ms",
             sim->device_count, sim->points_per_device,
             sim->sampling_interval_ms);
    return RET_OK;
}

/* ================================================================
 *  接口实现：start
 * ================================================================ */

static int sim_start(void *ctx)
{
    simulator_ctx_t *sim = (simulator_ctx_t *)ctx;

    if (!sim || !sim->initialized) {
        return RET_ERR;
    }

    if (sim->running) {
        LOG_WARN("模拟数据生成线程已在运行");
        return RET_ERR_ALREADY_EXISTS;
    }

    sim->running = 1;

    /* 创建数据生成线程 */
    if (pthread_create(&sim->thread, NULL, generate_thread, sim) != 0) {
        sim->running = 0;
        LOG_ERROR("无法创建数据生成线程: %s", strerror(errno));
        return RET_ERR;
    }

    LOG_INFO("模拟数据生成线程已启动");
    return RET_OK;
}

/* ================================================================
 *  接口实现：stop
 * ================================================================ */

static int sim_stop(void *ctx)
{
    simulator_ctx_t *sim = (simulator_ctx_t *)ctx;

    if (!sim || !sim->running) {
        return RET_OK;
    }

    LOG_INFO("正在停止模拟数据生成线程...");

    /* 设置停止标志，并唤醒可能阻塞在条件变量上的线程 */
    pthread_mutex_lock(&sim->buf_mutex);
    sim->running = 0;
    pthread_cond_signal(&sim->buf_cond);
    pthread_mutex_unlock(&sim->buf_mutex);

    /* 等待线程退出 */
    pthread_join(sim->thread, NULL);

    LOG_INFO("模拟数据生成线程已停止");
    return RET_OK;
}

/* ================================================================
 *  接口实现：acquire
 * ================================================================ */

static int sim_acquire(void *ctx, data_point_t *points, int max_count, int *actual)
{
    simulator_ctx_t *sim = (simulator_ctx_t *)ctx;

    if (!sim || !points || max_count <= 0 || !actual) {
        if (actual) *actual = 0;
        return RET_ERR_INVALID_PARAM;
    }

    /* 非阻塞地从环形缓冲区取出数据 */
    pthread_mutex_lock(&sim->buf_mutex);
    *actual = ring_buffer_get(&sim->buffer, points, max_count);
    pthread_mutex_unlock(&sim->buf_mutex);

    return RET_OK;
}

/* ================================================================
 *  接口实现：destroy
 * ================================================================ */

static void sim_destroy(void *ctx)
{
    simulator_ctx_t *sim = (simulator_ctx_t *)ctx;
    if (!sim) return;

    /* 确保线程已停止 */
    sim_stop(sim);

    /* 释放点位配置 */
    if (sim->devices) {
        for (int i = 0; i < sim->device_count; i++) {
            if (sim->devices[i].points) {
                free(sim->devices[i].points);
            }
        }
        free(sim->devices);
    }

    /* 释放环形缓冲区 */
    ring_buffer_destroy(&sim->buffer);

    /* 销毁同步原语 */
    pthread_mutex_destroy(&sim->buf_mutex);
    pthread_cond_destroy(&sim->buf_cond);

    /* 释放接口和上下文 */
    free(sim);

    LOG_INFO("模拟数据模块已销毁");
}

/* ================================================================
 *  数据生成线程
 * ================================================================ */

/**
 * @brief 后台数据生成线程主函数
 *
 * 循环执行：
 *   1. 按采样间隔休眠
 *   2. 遍历所有设备、所有点位
 *   3. 调用 apply_rule() 更新点位值
 *   4. 按概率决定是否注入异常 / 设备离线
 *   5. 生成 data_point_t 并写入环形缓冲区
 */
static void *generate_thread(void *arg)
{
    simulator_ctx_t *sim = (simulator_ctx_t *)arg;
    int total_points = sim->device_count * sim->points_per_device;

    LOG_INFO("数据生成线程运行中，共 %d 个采集点", total_points);

    while (sim->running) {
        for (int di = 0; di < sim->device_count; di++) {
            simulated_device_t *dev = &sim->devices[di];

            /* 按概率模拟设备状态变化 */
            dev->status = random_device_status(sim->offline_probability);

            for (int pi = 0; pi < dev->point_count; pi++) {
                point_config_t *pt = &dev->points[pi];
                data_point_t    dp;

                /* 更新点位值（内部有状态规则） */
                apply_rule(pt);

                /* 生成统一格式的数据点 */
                generate_data_point(dev, pt, &dp);

                /* 按概率注入异常 */
                double r = (double)rand() / RAND_MAX;
                inject_abnormality(&dp, r < sim->abnormal_probability);

                /* 写入环形缓冲区 */
                pthread_mutex_lock(&sim->buf_mutex);
                if (sim->running) {
                    ring_buffer_put(&sim->buffer, &dp);
                }
                pthread_cond_signal(&sim->buf_cond);
                pthread_mutex_unlock(&sim->buf_mutex);
            }
        }

        /* 采样间隔休眠（微秒） */
        usleep((useconds_t)sim->sampling_interval_ms * 1000);
    }

    LOG_INFO("数据生成线程退出");
    return NULL;
}

/* ================================================================
 *  数据生成核心逻辑
 * ================================================================ */

/**
 * @brief 根据规则更新点位的当前值
 * @param pt  点位配置（含状态，函数会修改 current / counter 等字段）
 *
 * 7 种规则的算法说明：
 *
 *   fixed:
 *     永远返回 (min+max)/2（取中间值作为固定值）
 *
 *   random:
 *     在 [min, max] 区间内取随机数
 *     整数类型取整；布尔类型 >=0.5 为 true
 *
 *   random_walk（随机漫步）：
 *     step = random(-step_size, +step_size)
 *     current = clamp(current + step, min, max)
 *     模拟传感器缓慢漂移
 *
 *   increment（递增）：
 *     current += step
 *     超过 max 时回绕到 min
 *
 *   decrement（递减）：
 *     current -= step
 *     低于 min 时回绕到 max
 *
 *   cycle（周期波动）：
 *     phase = (2π * counter) / period
 *     current = center + amplitude * sin(phase)
 *     其中 center = (min+max)/2, amplitude = (max-min)/2
 *
 *   trigger（触发）：
 *     随机选择 min 或 max 之一（模拟开关量）
 */
static void apply_rule(point_config_t *pt)
{
    double range, center, amplitude, phase, r;

    switch (pt->rule) {

    case RULE_FIXED:
        /* 不改变 current，保持初始值 */
        break;

    case RULE_RANDOM:
        r = (double)rand() / RAND_MAX;
        pt->current = pt->min + r * (pt->max - pt->min);
        break;

    case RULE_RANDOM_WALK:
        /* step_size 在 [-step, +step] 之间随机 */
        r = ((double)rand() / RAND_MAX) * 2.0 - 1.0;  /* [-1, 1] */
        pt->current += r * pt->step;
        /* 钳位 */
        if (pt->current > pt->max) pt->current = pt->max;
        if (pt->current < pt->min) pt->current = pt->min;
        break;

    case RULE_INCREMENT:
        pt->current += pt->step;
        if (pt->current > pt->max) pt->current = pt->min;
        break;

    case RULE_DECREMENT:
        pt->current -= pt->step;
        if (pt->current < pt->min) pt->current = pt->max;
        break;

    case RULE_CYCLE:
        range     = pt->max - pt->min;
        center    = pt->min + range / 2.0;
        amplitude = range / 2.0;
        /* 正弦波：phase 在 0 ~ 2π 之间循环 */
        phase = 2.0 * M_PI * pt->cycle_counter / (double)pt->cycle_period;
        pt->current = center + amplitude * sin(phase);
        pt->cycle_counter = (pt->cycle_counter + 1) % pt->cycle_period;
        break;

    case RULE_TRIGGER:
        /* 50% 概率在 min 和 max 间切换 */
        pt->current = (rand() % 2) ? pt->max : pt->min;
        break;

    default:
        break;
    }

    pt->counter++;
}

/**
 * @brief 生成一条完整的 data_point_t
 */
static void generate_data_point(const simulated_device_t *dev,
                                const point_config_t *pt, data_point_t *out)
{
    memset(out, 0, sizeof(data_point_t));

    common_strncpy(out->device_id, dev->device_id, MAX_DEVICE_ID_LEN);
    common_strncpy(out->point_id, pt->point_type, MAX_POINT_ID_LEN);
    out->type      = pt->data_type;
    out->timestamp = common_timestamp_ms();
    out->status_code = (dev->status == DEV_STATUS_ONLINE) ? 0 : -1;
    out->quality   = QUALITY_SIMULATED;
    out->data_status = DATA_STATUS_PENDING;

    /* 根据数据类型填充 union */
    switch (pt->data_type) {
    case DATA_TYPE_INT:
        out->value.int_val = (int64_t)pt->current;
        break;
    case DATA_TYPE_FLOAT:
        out->value.float_val = pt->current;
        break;
    case DATA_TYPE_BOOL:
        out->value.bool_val = (pt->current >= (pt->min + pt->max) / 2.0) ? 1 : 0;
        break;
    case DATA_TYPE_STRING:
        {
            /* 从字符串池中轮转选取 */
            int idx = abs((int)pt->current) % STRING_VALUE_POOL_SIZE;
            common_strncpy(out->value.str_val, string_pool[idx],
                           MAX_STRING_VALUE_LEN);
        }
        break;
    }
}

/**
 * @brief 注入异常数据
 * @param point    要修改的数据点
 * @param abnormal 1=注入异常, 0=正常
 *
 * 异常类型随机选择：
 *   - 超范围值（浮点*100 或设置为边界外）
 *   - 零值 / 空值
 *   - 质量标记为 BAD
 */
static void inject_abnormality(data_point_t *point, int abnormal)
{
    if (!abnormal) return;

    int anomaly_type = rand() % 3;

    switch (anomaly_type) {
    case 0:
        /* 超范围值 */
        if (point->type == DATA_TYPE_INT) {
            point->value.int_val = 999999;
        } else if (point->type == DATA_TYPE_FLOAT) {
            point->value.float_val = -999.99;
        }
        point->status_code = -2;
        point->quality = QUALITY_BAD;
        break;

    case 1:
        /* 空值 / 零值 */
        if (point->type == DATA_TYPE_STRING) {
            point->value.str_val[0] = '\0';
        } else {
            point->value.int_val = 0;
        }
        point->status_code = -3;
        point->quality = QUALITY_BAD;
        break;

    case 2:
        /* 质量差数据（值不变，但标记为 BAD 并附加长延迟时间戳） */
        point->timestamp = common_timestamp_ms() - 60000;  /* 延迟 60 秒 */
        point->quality = QUALITY_BAD;
        break;
    }
}

/**
 * @brief 按概率随机返回设备状态
 * @param offline_prob 离线概率（0~1）
 * @return 设备状态
 *
 * 90% 概率在线，offline_prob 概率离线，(1-offline_prob)/2 概率异常
 */
static device_status_t random_device_status(double offline_prob)
{
    double r = (double)rand() / RAND_MAX;

    if (r < offline_prob) {
        return DEV_STATUS_OFFLINE;
    }
    /* 剩余情况中，大约 5% 为异常（total 10% 非在线） */
    if (r < offline_prob + 0.05) {
        return DEV_STATUS_ABNORMAL;
    }
    return DEV_STATUS_ONLINE;
}

/* ================================================================
 *  环形缓冲区实现
 * ================================================================ */

/**
 * @brief 初始化环形缓冲区
 *
 * 环形缓冲区 (ring buffer / circular buffer)：
 *   使用固定大小的数组，当写指针追上读指针时（缓冲区满），
 *   覆盖最旧的数据（丢弃策略）。
 */
static int ring_buffer_init(ring_buffer_t *rb, int capacity)
{
    if (!rb || capacity <= 0) return RET_ERR_INVALID_PARAM;

    rb->data = (data_point_t *)calloc((size_t)capacity, sizeof(data_point_t));
    if (!rb->data) {
        return RET_ERR_NO_MEMORY;
    }

    rb->capacity = capacity;
    rb->head     = 0;
    rb->tail     = 0;
    rb->count    = 0;

    return RET_OK;
}

static void ring_buffer_destroy(ring_buffer_t *rb)
{
    if (rb && rb->data) {
        free(rb->data);
        rb->data = NULL;
    }
}

/**
 * @brief 写入一条数据到缓冲区（生产者调用）
 * @return RET_OK 成功
 *
 * 满时覆盖最旧数据（head 前进，tail 也前进）—— 宁可丢旧数据也不阻塞生成。
 */
static int ring_buffer_put(ring_buffer_t *rb, const data_point_t *point)
{
    if (!rb || !point) return RET_ERR_INVALID_PARAM;

    rb->data[rb->head] = *point;
    rb->head = (rb->head + 1) % rb->capacity;

    if (rb->count == rb->capacity) {
        /* 缓冲区满：丢弃最旧数据 */
        rb->tail = (rb->tail + 1) % rb->capacity;
    } else {
        rb->count++;
    }

    return RET_OK;
}

/**
 * @brief 读取数据（消费者调用，取最多 max_count 条）
 * @return 实际读取条数
 */
static int ring_buffer_get(ring_buffer_t *rb, data_point_t *points, int max_count)
{
    int taken = 0;

    if (!rb || !points || max_count <= 0 || rb->count == 0) {
        return 0;
    }

    while (taken < max_count && rb->count > 0) {
        points[taken] = rb->data[rb->tail];
        rb->tail = (rb->tail + 1) % rb->capacity;
        rb->count--;
        taken++;
    }

    return taken;
}

/* ================================================================
 *  配置解析
 * ================================================================ */

/**
 * @brief 从 JSON 字符串解析模拟器配置
 */
static int parse_config(simulator_ctx_t *sim, const char *config_json)
{
    cJSON *root = cJSON_Parse(config_json);
    if (!root) {
        return RET_ERR_PARSE;
    }

    cJSON *item;

    /* 设备数量 */
    item = cJSON_GetObjectItem(root, "device_count");
    if (item && cJSON_IsNumber(item)) {
        sim->device_count = item->valueint;
        if (sim->device_count < 1)  sim->device_count = 1;
        if (sim->device_count > MAX_DEVICES) sim->device_count = MAX_DEVICES;
    }

    /* 每设备点位数量 */
    item = cJSON_GetObjectItem(root, "points_per_device");
    if (item && cJSON_IsNumber(item)) {
        sim->points_per_device = item->valueint;
        if (sim->points_per_device < 1) sim->points_per_device = 1;
        if (sim->points_per_device > MAX_POINTS_PER_DEVICE)
            sim->points_per_device = MAX_POINTS_PER_DEVICE;
    }

    /* 采样间隔 */
    item = cJSON_GetObjectItem(root, "sampling_interval_ms");
    if (item && cJSON_IsNumber(item)) {
        sim->sampling_interval_ms = item->valueint;
        if (sim->sampling_interval_ms < 10) sim->sampling_interval_ms = 10;
    }

    /* 异常数据概率 */
    item = cJSON_GetObjectItem(root, "abnormal_data_probability");
    if (item && cJSON_IsNumber(item)) {
        sim->abnormal_probability = item->valuedouble;
    }

    /* 设备离线概率 */
    item = cJSON_GetObjectItem(root, "device_offline_probability");
    if (item && cJSON_IsNumber(item)) {
        sim->offline_probability = item->valuedouble;
    }

    /* 数据规则数组 */
    cJSON *rules = cJSON_GetObjectItem(root, "data_rules");
    if (rules && cJSON_IsArray(rules)) {
        create_devices(sim, rules);
    }

    cJSON_Delete(root);
    return RET_OK;
}

/**
 * @brief 根据数据规则创建模拟设备和点位
 *
 * 为每个设备创建一份点位配置的副本。
 * 设备 ID 格式：dev-001, dev-002, ...
 */
static int create_devices(simulator_ctx_t *sim, cJSON *rules_array)
{
    int rule_count = cJSON_GetArraySize(rules_array);
    if (rule_count <= 0) {
        LOG_WARN("数据规则数组为空");
        return RET_ERR;
    }

    /* 分配设备数组 */
    sim->devices = (simulated_device_t *)calloc(
        (size_t)sim->device_count, sizeof(simulated_device_t));
    if (!sim->devices) {
        return RET_ERR_NO_MEMORY;
    }

    for (int di = 0; di < sim->device_count; di++) {
        simulated_device_t *dev = &sim->devices[di];

        /* 设备 ID */
        snprintf(dev->device_id, MAX_DEVICE_ID_LEN, "dev-%03d", di + 1);
        dev->status = DEV_STATUS_ONLINE;

        /* 分配点位配置数组 */
        dev->point_count = sim->points_per_device;
        dev->points = (point_config_t *)calloc(
            (size_t)dev->point_count, sizeof(point_config_t));
        if (!dev->points) {
            return RET_ERR_NO_MEMORY;
        }

        /* 从规则数组中循环取配置（规则数可能少于每设备点数，循环使用） */
        for (int pi = 0; pi < dev->point_count; pi++) {
            cJSON *rule_item = cJSON_GetArrayItem(rules_array,
                                                  pi % rule_count);
            point_config_t *pt = &dev->points[pi];

            if (!rule_item) continue;

            /* 点位类型名 */
            cJSON *field = cJSON_GetObjectItem(rule_item, "point_type");
            if (field && cJSON_IsString(field)) {
                common_strncpy(pt->point_type, field->valuestring, 64);
            } else {
                snprintf(pt->point_type, 64, "point-%d", pi + 1);
            }

            /* 数据类型 */
            field = cJSON_GetObjectItem(rule_item, "data_type");
            if (field && cJSON_IsString(field)) {
                const char *s = field->valuestring;
                if (strcmp(s, "int") == 0)       pt->data_type = DATA_TYPE_INT;
                else if (strcmp(s, "float") == 0) pt->data_type = DATA_TYPE_FLOAT;
                else if (strcmp(s, "bool") == 0)  pt->data_type = DATA_TYPE_BOOL;
                else if (strcmp(s, "string") == 0) pt->data_type = DATA_TYPE_STRING;
                else                              pt->data_type = DATA_TYPE_FLOAT;
            }

            /* 变化规则 */
            field = cJSON_GetObjectItem(rule_item, "rule");
            if (field && cJSON_IsString(field)) {
                const char *s = field->valuestring;
                if (strcmp(s, "fixed") == 0)          pt->rule = RULE_FIXED;
                else if (strcmp(s, "random") == 0)    pt->rule = RULE_RANDOM;
                else if (strcmp(s, "random_walk") == 0) pt->rule = RULE_RANDOM_WALK;
                else if (strcmp(s, "increment") == 0) pt->rule = RULE_INCREMENT;
                else if (strcmp(s, "decrement") == 0) pt->rule = RULE_DECREMENT;
                else if (strcmp(s, "cycle") == 0)     pt->rule = RULE_CYCLE;
                else if (strcmp(s, "trigger") == 0)   pt->rule = RULE_TRIGGER;
                else                                  pt->rule = RULE_RANDOM;
            }

            /* 最小/最大值 */
            field = cJSON_GetObjectItem(rule_item, "min");
            if (field && cJSON_IsNumber(field)) pt->min = field->valuedouble;
            field = cJSON_GetObjectItem(rule_item, "max");
            if (field && cJSON_IsNumber(field)) pt->max = field->valuedouble;

            /* 计算默认步长 */
            pt->step = (pt->max - pt->min) / 100.0;
            if (pt->step <= 0) pt->step = 1.0;

            /* 初始值设为 min */
            pt->current = pt->min;

            /* 周期长度（cycle 规则使用） */
            pt->cycle_period = 20;
            pt->cycle_counter = pi * 3;  /* 不同点位初始相位不同，避免同步 */
        }
    }

    return RET_OK;
}
