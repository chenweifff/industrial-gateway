/**
 * @file    config.c
 * @brief   工业数据采集与上报网关 —— 配置管理模块（实现）
 *
 * 实现要点（初学者参考）：
 *   1. cJSON 是 C 语言的轻量级 JSON 库，核心概念：
 *      - cJSON_Parse(str)   : 将 JSON 字符串解析为 cJSON 树
 *      - cJSON_Print(root)  : 将 cJSON 树序列化为 JSON 字符串
 *      - cJSON_Delete(root) : 释放 cJSON 树的内存
 *   2. 本模块使用「内存常驻」策略：加载一次，全局持有解析后的 cJSON 树，
 *      后续所有查询都操作这棵树，避免反复读文件。
 *   3. 配置校验采用「宽松策略」：缺失字段使用默认值，不中断加载；
 *      只有 JSON 语法错误或文件不存在才视为加载失败。
 */

#include "config.h"
#include "logger.h"
#include "cJSON.h"

#include <stdio.h>      /* fopen, fread, fclose */
#include <stdlib.h>     /* malloc, free */
#include <string.h>     /* strlen, memset */

/* ---- 全局状态 ---- */
static cJSON *g_config_root = NULL;     /* 内存中的 JSON 配置树 */
static char   g_config_path[MAX_PATH_LEN] = {0};  /* 当前配置文件路径 */

/* ---- 内部函数声明 ---- */
static char *read_file_all(const char *path, long *out_len);
static int   validate_config(cJSON *root);

/* ================================================================
 *  公开 API
 * ================================================================ */

int config_load(const char *file_path)
{
    long  file_len = 0;
    char *raw_json = NULL;
    cJSON *root    = NULL;

    if (!file_path) {
        LOG_ERROR("config_load: 文件路径为空");
        return RET_ERR_INVALID_PARAM;
    }

    LOG_INFO("正在加载配置文件: %s", file_path);

    /* 1. 读取文件全部内容 */
    raw_json = read_file_all(file_path, &file_len);
    if (!raw_json) {
        LOG_ERROR("无法读取配置文件: %s", file_path);
        return RET_ERR_IO;
    }
    if (file_len == 0) {
        LOG_ERROR("配置文件为空: %s", file_path);
        free(raw_json);
        return RET_ERR_PARSE;
    }

    /* 2. 解析 JSON */
    root = cJSON_Parse(raw_json);
    free(raw_json);  /* 解析完成后，原始字符串即可释放 */
    if (!root) {
        LOG_ERROR("配置文件 JSON 格式错误: %s",
                  cJSON_GetErrorPtr() ? cJSON_GetErrorPtr() : "未知错误");
        return RET_ERR_PARSE;
    }

    /* 3. 校验配置结构（必选字段检查、范围检查等） */
    if (validate_config(root) != RET_OK) {
        LOG_WARN("配置校验未完全通过，将使用默认值补充缺失字段");
        /* 校验失败不阻塞加载，只记录警告，继续使用 */
    }

    /* 4. 替换旧的配置树 */
    if (g_config_root) {
        cJSON_Delete(g_config_root);
    }
    g_config_root = root;

    /* 5. 记录当前配置文件路径（为热更新和持久化保存使用） */
    common_strncpy(g_config_path, file_path, MAX_PATH_LEN);

    LOG_INFO("配置文件加载成功: %s (%ld 字节)", file_path, file_len);
    return RET_OK;
}

int config_reload(void)
{
    if (g_config_path[0] == '\0') {
        LOG_ERROR("config_reload: 未曾加载过配置，无法重载");
        return RET_ERR;
    }
    LOG_INFO("正在热更新配置: %s", g_config_path);
    return config_load(g_config_path);
}

int config_save(const char *file_path)
{
    const char *target;
    char *json_str;

    if (!g_config_root) {
        LOG_ERROR("config_save: 没有已加载的配置");
        return RET_ERR;
    }

    target = file_path ? file_path : g_config_path;
    if (!target || target[0] == '\0') {
        LOG_ERROR("config_save: 未指定保存路径");
        return RET_ERR_INVALID_PARAM;
    }

    /* 将 cJSON 树序列化为格式化 JSON 字符串 */
    json_str = cJSON_Print(g_config_root);
    if (!json_str) {
        LOG_ERROR("config_save: JSON 序列化失败");
        return RET_ERR_NO_MEMORY;
    }

    /* 写入文件 */
    FILE *fp = fopen(target, "w");
    if (!fp) {
        LOG_ERROR("config_save: 无法打开文件写入: %s", target);
        free(json_str);
        return RET_ERR_IO;
    }
    fputs(json_str, fp);
    fclose(fp);
    free(json_str);

    LOG_INFO("配置已保存到: %s", target);
    return RET_OK;
}

void config_destroy(void)
{
    if (g_config_root) {
        cJSON_Delete(g_config_root);
        g_config_root = NULL;
    }
    g_config_path[0] = '\0';
    LOG_INFO("配置管理模块已释放");
}

/* ================================================================
 *  分模块配置获取
 * ================================================================ */

int config_get_section(const char *section, char *json_out, size_t size)
{
    cJSON *section_obj;
    char  *json_str;

    if (!g_config_root) {
        LOG_ERROR("config_get_section: 配置未加载");
        return RET_ERR;
    }
    if (!section || !json_out || size == 0) {
        return RET_ERR_INVALID_PARAM;
    }

    section_obj = cJSON_GetObjectItem(g_config_root, section);
    if (!section_obj) {
        LOG_WARN("config_get_section: 模块 '%s' 的配置不存在", section);
        json_out[0] = '\0';
        return RET_ERR_NOT_FOUND;
    }

    /* 将该 JSON 子树序列化为字符串，供调用方解析 */
    json_str = cJSON_PrintUnformatted(section_obj);
    if (!json_str) {
        return RET_ERR_NO_MEMORY;
    }

    common_strncpy(json_out, json_str, size);
    free(json_str);
    return RET_OK;
}

int config_get_string(const char *key, const char *default_val,
                      char *out, size_t size)
{
    cJSON *item;

    if (!g_config_root || !key || !out || size == 0) {
        if (out && size > 0 && default_val) {
            common_strncpy(out, default_val, size);
        }
        return RET_ERR_INVALID_PARAM;
    }

    item = cJSON_GetObjectItem(g_config_root, key);
    if (item && cJSON_IsString(item)) {
        common_strncpy(out, item->valuestring, size);
        return RET_OK;
    }

    /* 不存在或类型不匹配 → 使用默认值 */
    if (default_val) {
        common_strncpy(out, default_val, size);
    } else {
        out[0] = '\0';
    }
    return RET_ERR_NOT_FOUND;
}

int config_get_int(const char *key, int default_val)
{
    cJSON *item;

    if (!g_config_root || !key) {
        return default_val;
    }

    item = cJSON_GetObjectItem(g_config_root, key);
    if (item && cJSON_IsNumber(item)) {
        return item->valueint;
    }

    return default_val;
}

int config_get_bool(const char *key, int default_val)
{
    cJSON *item;

    if (!g_config_root || !key) {
        return default_val;
    }

    item = cJSON_GetObjectItem(g_config_root, key);
    if (item && cJSON_IsBool(item)) {
        return cJSON_IsTrue(item) ? 1 : 0;
    }

    return default_val;
}

struct cJSON *config_get_root(void)
{
    return g_config_root;
}

/* ================================================================
 *  内部辅助函数
 * ================================================================ */

/**
 * @brief 读取文件的全部内容到内存
 * @param path     文件路径
 * @param out_len  [out] 文件长度（字节数）
 * @return 指向文件内容的 malloc 缓冲区指针（调用方负责 free），失败返回 NULL
 *
 * 实现方式：
 *   - 先用 fseek + ftell 获取文件大小
 *   - 分配对应大小的内存
 *   - 用 fread 一次性读入
 */
static char *read_file_all(const char *path, long *out_len)
{
    FILE *fp;
    long  len;
    char *buf;

    fp = fopen(path, "rb");
    if (!fp) {
        return NULL;
    }

    /* 获取文件大小 */
    fseek(fp, 0, SEEK_END);
    len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    if (len <= 0) {
        fclose(fp);
        *out_len = 0;
        return NULL;
    }

    /* 分配内存（+1 留给 '\0'） */
    buf = (char *)malloc((size_t)len + 1);
    if (!buf) {
        fclose(fp);
        return NULL;
    }

    /* 读取全部内容 */
    size_t read_len = fread(buf, 1, (size_t)len, fp);
    fclose(fp);

    if (read_len != (size_t)len) {
        free(buf);
        return NULL;
    }

    buf[len] = '\0';  /* 确保字符串以 NULL 结尾 */
    *out_len = len;
    return buf;
}

/**
 * @brief 校验配置的顶层结构
 * @param root  cJSON 根节点
 * @return RET_OK 通过，负数表示有严重问题
 *
 * 校验规则：
 *   - 配置文件最外层必须是 JSON 对象（{}）
 *   - 检查 simulator、collector、processor、storage、reporter 等关键模块
 *     配置节是否存在，不存在则记录警告（不阻塞加载）
 */
static int validate_config(cJSON *root)
{
    int warning_count = 0;

    if (!root || !cJSON_IsObject(root)) {
        LOG_ERROR("JSON 根节点必须是对象类型");
        return RET_ERR_PARSE;
    }

    /* 检查各模块配置是否存在 */
    static const char *sections[] = {
        "simulator", "collector", "processor", "storage", "reporter", NULL
    };
    for (int i = 0; sections[i] != NULL; i++) {
        if (!cJSON_GetObjectItem(root, sections[i])) {
            LOG_WARN("配置中缺少 '%s' 节，将使用默认值", sections[i]);
            warning_count++;
        }
    }

    if (warning_count > 0) {
        LOG_WARN("配置校验发现 %d 处警告", warning_count);
    }

    return RET_OK;
}
