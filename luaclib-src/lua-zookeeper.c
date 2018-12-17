
#include "skynet.h"
#include "skynet_mq.h"
#include "skynet_server.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <zookeeper/zookeeper.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

struct zk_handle {
    zhandle_t *zh;
    uint32_t opaque;
};

struct zk_context {
    int opaque;
    int session;
    char* watcher;
};

////////////////////////////////////////////////
#define TYPE_NIL 0
#define TYPE_BOOLEAN 1
// hibits 0 false 1 true
#define TYPE_NUMBER 2
// hibits 0 : 0 , 1: byte, 2:word, 4: dword, 6: qword, 8 : double
#define TYPE_NUMBER_ZERO 0
#define TYPE_NUMBER_BYTE 1
#define TYPE_NUMBER_WORD 2
#define TYPE_NUMBER_DWORD 4
#define TYPE_NUMBER_QWORD 6
#define TYPE_NUMBER_REAL 8

#define TYPE_USERDATA 3
#define TYPE_SHORT_STRING 4
// hibits 0~31 : len
#define TYPE_LONG_STRING 5
#define TYPE_TABLE 6

#define MAX_COOKIE 32
#define COMBINE_TYPE(t,v) ((t) | (v) << 3)

#define BLOCK_SIZE 128

struct block {
    struct block * next;
    char buffer[BLOCK_SIZE];
};

struct write_block {
    struct block * head;
    struct block * current;
    int len;
    int ptr;
};

static void wb_init(struct write_block *wb , struct block *b) {
    wb->head = b;
    assert(b->next == NULL);
    wb->len = 0;
    wb->current = wb->head;
    wb->ptr = 0;
}

static void wb_free(struct write_block *wb) {
    struct block *blk = wb->head;
    blk = blk->next;    // the first block is on stack
    while (blk) {
        struct block * next = blk->next;
        skynet_free(blk);
        blk = next;
    }
    wb->head = NULL;
    wb->current = NULL;
    wb->ptr = 0;
    wb->len = 0;
}

inline static struct block *blk_alloc(void) {
    struct block *b = skynet_malloc(sizeof(struct block));
    b->next = NULL;
    return b;
}

inline static void wb_push(struct write_block *b, const void *buf, int sz) {
    const char * buffer = buf;
    if (b->ptr == BLOCK_SIZE) {
_again:
        b->current = b->current->next = blk_alloc();
        b->ptr = 0;
    }
    if (b->ptr <= BLOCK_SIZE - sz) {
        memcpy(b->current->buffer + b->ptr, buffer, sz);
        b->ptr+=sz;
        b->len+=sz;
    } else {
        int copy = BLOCK_SIZE - b->ptr;
        memcpy(b->current->buffer + b->ptr, buffer, copy);
        buffer += copy;
        b->len += copy;
        sz -= copy;
        goto _again;
    }
}

static inline void wb_nil(struct write_block *wb) {
    uint8_t n = TYPE_NIL;
    wb_push(wb, &n, 1);
}

static inline void wb_integer(struct write_block *wb, lua_Integer v) {
    int type = TYPE_NUMBER;
    if (v == 0) {
        uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_ZERO);
        wb_push(wb, &n, 1);
    } else if (v != (int32_t)v) {
        uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_QWORD);
        int64_t v64 = v;
        wb_push(wb, &n, 1);
        wb_push(wb, &v64, sizeof(v64));
    } else if (v < 0) {
        int32_t v32 = (int32_t)v;
        uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
        wb_push(wb, &n, 1);
        wb_push(wb, &v32, sizeof(v32));
    } else if (v<0x100) {
        uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_BYTE);
        wb_push(wb, &n, 1);
        uint8_t byte = (uint8_t)v;
        wb_push(wb, &byte, sizeof(byte));
    } else if (v<0x10000) {
        uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_WORD);
        wb_push(wb, &n, 1);
        uint16_t word = (uint16_t)v;
        wb_push(wb, &word, sizeof(word));
    } else {
        uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
        wb_push(wb, &n, 1);
        uint32_t v32 = (uint32_t)v;
        wb_push(wb, &v32, sizeof(v32));
    }
}

static inline void wb_real(struct write_block *wb, double v) {
    uint8_t n = COMBINE_TYPE(TYPE_NUMBER , TYPE_NUMBER_REAL);
    wb_push(wb, &n, 1);
    wb_push(wb, &v, sizeof(v));
}

static inline void wb_string(struct write_block *wb, const char *str, int len) {
    if (len < MAX_COOKIE) {
        uint8_t n = COMBINE_TYPE(TYPE_SHORT_STRING, len);
        wb_push(wb, &n, 1);
        if (len > 0) {
            wb_push(wb, str, len);
        }
    } else {
        uint8_t n;
        if (len < 0x10000) {
            n = COMBINE_TYPE(TYPE_LONG_STRING, 2);
            wb_push(wb, &n, 1);
            uint16_t x = (uint16_t) len;
            wb_push(wb, &x, 2);
        } else {
            n = COMBINE_TYPE(TYPE_LONG_STRING, 4);
            wb_push(wb, &n, 1);
            uint32_t x = (uint32_t) len;
            wb_push(wb, &x, 4);
        }
        wb_push(wb, str, len);
    }
}

static uint8_t * seri(struct block *b, int len) {
    uint8_t * buffer = skynet_malloc(len);
    uint8_t * ptr = buffer;
    while(len>0) {
        if (len >= BLOCK_SIZE) {
            memcpy(ptr, b->buffer, BLOCK_SIZE);
            ptr += BLOCK_SIZE;
            len -= BLOCK_SIZE;
            b = b->next;
        } else {
            memcpy(ptr, b->buffer, len);
            break;
        }
    }
    return buffer;
}

static void forward_messsage(struct zk_context *wrapper, void *msg, int sz) {
    struct skynet_message message;
    message.source = 0;
    message.session = 0;
    message.data = msg;
    message.sz = sz | ((size_t)PTYPE_TEXT << MESSAGE_TYPE_SHIFT);
    
    if (skynet_context_push(wrapper->opaque, &message)) {
        skynet_free(msg);
    }
}

#define WB_PROPERTY_INTEGER(__NAME__, __VALUE__) \
    wb_string(wb,(__NAME__),strlen(__NAME__));\
    wb_integer(wb, __VALUE__)

#define WB_PROPERTY_STRING(__NAME__, __VALUE__) \
    wb_string(wb,(__NAME__),strlen(__NAME__));\
    wb_string(wb, __VALUE__,strlen(__VALUE__))

static void wb_table_header(struct write_block *wb, int array_size)
{
    if (array_size >= MAX_COOKIE-1) {
        uint8_t n = COMBINE_TYPE(TYPE_TABLE, MAX_COOKIE-1);
        wb_push(wb, &n, 1);
        wb_integer(wb, array_size);
    } else {
        uint8_t n = COMBINE_TYPE(TYPE_TABLE, array_size);
        wb_push(wb, &n, 1);
    }
}

static void wb_table_end(struct write_block *wb)
{
    wb_nil(wb);
}

static void wb_stat(struct write_block *wb, const struct Stat *stat)
{
    if (stat != NULL) {
        wb_table_header(wb, 0);

        WB_PROPERTY_INTEGER("czxid", stat->czxid);
        WB_PROPERTY_INTEGER("mzxid", stat->mzxid);
        WB_PROPERTY_INTEGER("ctime", stat->ctime);
        WB_PROPERTY_INTEGER("mtime", stat->mtime);
        WB_PROPERTY_INTEGER("version", stat->version);
        WB_PROPERTY_INTEGER("cversion", stat->cversion);
        WB_PROPERTY_INTEGER("aversion", stat->aversion);
        WB_PROPERTY_INTEGER("ephemeralOwner", stat->ephemeralOwner);
        WB_PROPERTY_INTEGER("dataLength", stat->dataLength);
        WB_PROPERTY_INTEGER("numChildren", stat->numChildren);
        WB_PROPERTY_INTEGER("pzxid", stat->pzxid);

        wb_table_end(wb);
    }
}

static void wb_strings(struct write_block *wb, const struct String_vector *sv)
{
    if (sv != NULL) {
        wb_table_header(wb, sv->count);
        int i;
        for (i = 0; i < sv->count; ++i) {
            wb_string(wb, sv->data[i],strlen(sv->data[i]));
        }
        wb_table_end(wb);
    }
}

static int wb_acl(struct write_block *wb, const struct ACL_vector *acls)
{
    if (acls != NULL) {
        wb_table_header(wb, acls->count);
        int i;
        for (i = 0; i < acls->count; ++i) {
            wb_table_header(wb, 0);

            WB_PROPERTY_INTEGER("perms", acls->data[i].perms);
            WB_PROPERTY_STRING("scheme", acls->data[i].id.scheme);
            WB_PROPERTY_STRING("id", acls->data[i].id.id);

            wb_table_end(wb);
        }
        wb_table_end(wb);
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////


static FILE *g_zk_log_stream = NULL;
static char g_zk_handle_key = 'k';
#define ZK_METATABLE_NAME "ZK_HANDLE"


#define CREATE_MSG(__wrapper__) \
    struct block temp;\
    temp.next = NULL;\
    struct write_block owb;\
    struct write_block* wb = &owb;\
    wb_init(wb, &temp);\
    wb_string(wb, (__wrapper__)->watcher,strlen((__wrapper__)->watcher));\
    wb_integer(wb, (__wrapper__)->session);\
    wb_table_header(wb, 0)

#define SEND_MSG(__wrapper__) \
    wb_table_end(wb);\
    assert(wb->head == &temp);\
    int sz = wb->len;\
    char* msg = (char*)seri(&temp, sz);\
    wb_free(wb);\
    forward_messsage((__wrapper__), msg, sz)

static void _zk_context_free(struct zk_context* wrapper);

//zookeeper-callback////////////////////////////////////////////////////////////////////
void watcher_dispatch(zhandle_t *zh, int type, int state, const char *path, void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("type",type);
    WB_PROPERTY_INTEGER("state",state);
    if (path){
        wb_string(wb,"path",strlen("path"));
        wb_string(wb,path,strlen(path));
    }
    
    SEND_MSG(wrapper);
}

void void_completion_dispatch(int rc, const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

void data_completion_dispatch(int rc, const char *value, int value_len, const struct Stat *stat, const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);
    if (value){
        wb_string(wb,"value",strlen("value"));
        wb_string(wb,value,value_len);
    }
    if (stat){
        wb_string(wb,"stat",strlen("stat"));
        wb_stat(wb, stat);
    }

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

void stat_completion_dispatch(int rc, const struct Stat *stat, const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);
    if (stat){
        wb_string(wb,"stat",strlen("stat"));
        wb_stat(wb, stat);
    }

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

void string_completion_dispatch(int rc, const char *value, const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);
    if (value){
        wb_string(wb,"value",strlen("value"));
        wb_string(wb,value,strlen(value));
    }

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

void strings_completion_dispatch(int rc, const struct String_vector *strings,const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);
    if (strings){
        wb_string(wb,"strings",strlen("strings"));
        wb_strings(wb, strings);
    }

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

void strings_stat_completion_dispatch(int rc, const struct String_vector *strings, const struct Stat *stat, const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);
    if (strings){
        wb_string(wb,"strings",strlen("strings"));
        wb_strings(wb, strings);
    }
    if (stat){
        wb_string(wb,"stat",strlen("stat"));
        wb_stat(wb, stat);
    }

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

void acl_completion_dispatch(int rc, struct ACL_vector *acl, struct Stat *stat, const void *context)
{
    struct zk_context *wrapper = (struct zk_context *)context;
    CREATE_MSG(wrapper);

    WB_PROPERTY_INTEGER("rc",rc);
    if (acl){
        wb_string(wb,"acl",strlen("acl"));
        wb_acl(wb, acl);
    }
    if (stat){
        wb_string(wb,"stat",strlen("stat"));
        wb_stat(wb, stat);
    }

    SEND_MSG(wrapper);
    _zk_context_free(wrapper);
}

//zookeeper-handle////////////////////////////////////////////////////////////////////
static void _zk_handle_save(lua_State *L, int index)
{
    lua_pushlightuserdata(L, (void *)&g_zk_handle_key);
    lua_pushvalue(L, index);
    lua_settable(L, LUA_REGISTRYINDEX);
}

static void _zk_handle_remove(lua_State *L)
{
    lua_pushlightuserdata(L, (void *)&g_zk_handle_key);
    lua_pushnil(L);
    lua_settable(L, LUA_REGISTRYINDEX);
}

static struct zk_handle *_zk_handle_create(lua_State *L)
{
    struct zk_handle *handle = (struct zk_handle *)lua_newuserdata(L, sizeof(struct zk_handle));
    luaL_getmetatable(L, ZK_METATABLE_NAME);
    lua_setmetatable(L, -2);
    _zk_handle_save(L, -1);
    return handle;
}

static int _zk_handle_check(lua_State *L, struct zk_handle *handle)
{
    if (handle->zh && handle->opaque > 0) {
        return 1;
    } else {
        luaL_error(L, "_zk_handle_check: invalid zookeeper handle.");
        return 0;
    }
}

//zookeeper-context////////////////////////////////////////////////////////////////////
static struct zk_context *_zk_context_create(int opaque, const char *watcher,size_t watcher_len,int session)
{
    struct zk_context* wrapper = (struct zk_context*)skynet_malloc(sizeof(struct zk_context));
    memset(wrapper, 0, sizeof(struct zk_context));
    wrapper->opaque = opaque;
    wrapper->session = session;

    wrapper->watcher = (char*)skynet_malloc(watcher_len+1);
    memset(wrapper->watcher,0,watcher_len+1);
    memcpy(wrapper->watcher, watcher, watcher_len);
    // printf("_zgw_context_init opaque=%d, session=%d\n", wrapper->opaque, wrapper->session);
    return wrapper;
}

static void _zk_context_free(struct zk_context* wrapper)
{
    if (wrapper){
        skynet_free(wrapper->watcher);
        skynet_free(wrapper);
    }
}

//zookeeper-clientid///////////////////////////////////////////////////////
static clientid_t *_zk_clientid_create(lua_State *L, int index)
{
    clientid_t* clientid = (clientid_t *)skynet_malloc(sizeof(clientid_t));
    if (clientid == NULL) {
        luaL_error(L, "out of memory when alloc an internal object.");
    }
    luaL_checktype(L, index, LUA_TTABLE);
    lua_getfield(L, index, "client_id");
    clientid->client_id = luaL_checkinteger(L, -1);

    lua_pop(L, 1);
    lua_getfield(L, index, "passwd");
    size_t passwd_len = 0;
    const char* clientid_passwd = luaL_checklstring(L, -1, &passwd_len);
    memset(clientid->passwd, 0, 16);
    memcpy(clientid->passwd, clientid_passwd, passwd_len);
    lua_pop(L, 1);
    return  clientid;
}

static void _zk_clientid_free(clientid_t **clientid)
{
    if (*clientid != NULL) {
        skynet_free(*clientid);
        *clientid = NULL;
    }
}

//zookeeper-acls///////////////////////////////////////////////////////
static int _zk_acls_create(lua_State *L, int index, struct ACL_vector *acls)
{
    luaL_checktype(L, index, LUA_TTABLE);
    int count = lua_rawlen(L, index);
    acls->count = count;
    acls->data = (struct ACL *)skynet_malloc(count*sizeof(struct ACL));
    if (acls->data == NULL) {
        luaL_error(L, "out of memory when alloc an internal object.");
        return 0;
    }
    int i = 1;
    for (; i <= count; i++) {
        lua_rawgeti(L, index, i);

        lua_pushstring(L, "perms");
        lua_rawget(L, -2);
        acls->data[i-1].perms = lua_tointeger(L, -1);
        lua_pop(L, 1);

        lua_pushstring(L, "scheme");
        lua_rawget(L, -2);
        acls->data[i-1].id.scheme = skynet_strdup(lua_tostring(L, -1));
        lua_pop(L, 1);

        lua_pushstring(L, "id");
        lua_rawget(L, -2);
        acls->data[i-1].id.id = skynet_strdup(lua_tostring(L, -1));
        lua_pop(L, 1);

        lua_pop(L, 1);
    }
    return 1;
}

static int _zk_acls_free(struct ACL_vector *acls)
{
    if (acls == NULL) {
        return -1;
    }
    int i = 0;
    for (; i < acls->count; ++i) {
        skynet_free(acls->data[i].id.id);
        skynet_free(acls->data[i].id.scheme);
    }
    skynet_free(acls->data);
    return 1;
}

//zookeeper-client///////////////////////////////////////////////////////
//class///////////////////////////////////////////////
static int client_set_debug_level(lua_State *L)
{
    int level = luaL_checkinteger(L, -1);
    switch(level) {
        case ZOO_LOG_LEVEL_ERROR:
            zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
            break;
        case ZOO_LOG_LEVEL_WARN:
            zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
            break;
        case ZOO_LOG_LEVEL_INFO:
            zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
            break;
        case ZOO_LOG_LEVEL_DEBUG:
            zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
            break;
        default:
            return luaL_error(L, "invalid arguments: unsupported log level specified.");
    }
    return 0;
}

static int client_set_log_stream(lua_State *L)
{
    size_t file_path_len = 0;
    const char *file_path = luaL_checklstring(L, -1, &file_path_len);
    g_zk_log_stream = fopen(file_path, "w+");
    if (g_zk_log_stream == NULL) return luaL_error(L,"unable to open the specified file %s.", file_path);
    zoo_set_log_stream(g_zk_log_stream);
    return 0;
}

static int client_error(lua_State *L)
{
    int code = luaL_checkinteger(L, -1);
    const char *errstr = zerror(code);
    lua_pushstring(L, errstr);
    return 1;
}

static int client_deterministic_conn_order(lua_State *L)
{
    size_t yesorno_len = 0;
    const char *yesorno = luaL_checklstring(L, 1, &yesorno_len);
    if (strcasecmp(yesorno, "yes") || strcasecmp(yesorno, "true")) {
        zoo_deterministic_conn_order(1);
    } else if (strcasecmp(yesorno, "no") || strcasecmp(yesorno, "false")) {
        zoo_deterministic_conn_order(0);
    } else {
        return luaL_error(L, "invalid argument: please choose(yes, no) or (true, false).");
    }
    return 0;
}

//instance/////////////////////////////////////////
static int client_init(lua_State *L)
{
    int count = lua_gettop(L);
    size_t host_len = 0;
    const char *host = luaL_checklstring(L, 1, &host_len);
    if (host_len <= 0){
        return luaL_error(L, "invalid host");
    }
    int opaque = luaL_checkinteger(L, 2);

    size_t watcher_len = 0;
    const char *watcher = luaL_checklstring(L, 3, &watcher_len);
    if (watcher_len <= 0){
        return luaL_error(L, "invalid watcher");
    }

    struct zk_context* wrapper = _zk_context_create(opaque, watcher, watcher_len, 0);
    struct zk_handle *handle = _zk_handle_create(L);
    handle->opaque = opaque;
    int timeout = luaL_checkinteger(L, 4);

    clientid_t *clientid = NULL;
    int flags = 0;
    switch(count) {
        case 4:
            handle->zh = zookeeper_init(host, watcher_dispatch, timeout, 0, wrapper, 0);
            break;
        case 5:
            luaL_checktype(L, 5, LUA_TTABLE);
            clientid = _zk_clientid_create(L, 5);
            handle->zh = zookeeper_init(host, watcher_dispatch, timeout, clientid, wrapper, 0);
            _zk_clientid_free(&clientid);
            break;
        case 6:
            luaL_checktype(L, 5, LUA_TTABLE);
            clientid = _zk_clientid_create(L, 5);
            flags = luaL_checkinteger(L, 6);
            handle->zh = zookeeper_init(host, watcher_dispatch, timeout, clientid, wrapper, flags);
            _zk_clientid_free(&clientid);
            break;
        default:
            return luaL_error(L, "invalid parameters count");
    }
    return 1;
}

static int client_close(lua_State *L)
{
    int ret = 0;
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (handle->zh != NULL) {
        ret = zookeeper_close(handle->zh);
        handle->zh = NULL;
        _zk_handle_remove(L);
    } else {
        return luaL_error(L, "unable to close the zookeeper handle.");
    }
    if (g_zk_log_stream != NULL) fclose(g_zk_log_stream);
    lua_pushinteger(L, ret);
    return 1;
}

static int client_client_id(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        const clientid_t *clientid = zoo_client_id(handle->zh);
        lua_newtable(L);
        lua_pushstring(L, "client_id");
        lua_pushnumber(L, clientid->client_id);
        lua_settable(L, -3);
        lua_pushstring(L, "passwd");
        lua_pushstring(L, clientid->passwd);
        lua_settable(L, -3);
        return 1;
    } else {
        return luaL_error(L, "unable to get client id.");
    }
}

static int client_recv_timeout(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int recv_timeout = zoo_recv_timeout(handle->zh);
        lua_pushinteger(L, recv_timeout);
        return 1;
    } else {
        return luaL_error(L, "unable to get recv_timeout.");
    }
}

static int client_get_context(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        struct zk_context* wrapper = (struct zk_context*)zoo_get_context(handle->zh);
        assert(wrapper != NULL);
        lua_pushinteger(L, wrapper->opaque);
        lua_pushstring(L, wrapper->watcher);
        return 2;
    } else {
        return luaL_error(L, "unable to get zookeeper handle context.");
    }
}

static int client_set_context(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        struct zk_context* wrapper = (struct zk_context*)zoo_get_context(handle->zh);
        assert(wrapper != NULL);

        int opaque = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        if (watcher_len <= 0){
            return luaL_error(L, "invalid watcher");
        }
        struct zk_context* new_wrapper = _zk_context_create(opaque, watcher, watcher_len, 0);
        zoo_set_context(handle->zh, new_wrapper);

        _zk_context_free(wrapper);
        return 0;
    } else {
        return luaL_error(L, "unable to get zookeeper handle context.");
    }
}

//no need
//watcher_fn zoo_set_watcher(zhandle_t *zh,watcher_fn newFn);

struct test_zhandle {
    int fd;
    char *hostname;
};

static int client_get_connected_host(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        struct sockaddr saddr;
        socklen_t socklen = sizeof(saddr);
        bzero(&saddr, sizeof(saddr));
        struct sockaddr *paddr = zookeeper_get_connected_host(handle->zh, &saddr, &socklen);
        if (paddr != NULL) {
            if (socklen == sizeof(struct sockaddr_in)) {
                char addrstr[INET_ADDRSTRLEN] = {0};
                unsigned short port = ntohs(((struct sockaddr_in *)paddr)->sin_port);
                inet_ntop(AF_INET, &(((struct sockaddr_in *)paddr)->sin_addr), addrstr, INET_ADDRSTRLEN);
                lua_pushstring(L, addrstr);
                lua_pushinteger(L, port);
                return 2;
            } else if (socklen == sizeof(struct sockaddr_in6)) {
                char addr6str[INET6_ADDRSTRLEN] = {0};
                unsigned short port = ntohs(((struct sockaddr_in6 *)paddr)->sin6_port);
                inet_ntop(AF_INET6, &(((struct sockaddr_in6 *)paddr)->sin6_addr), addr6str, INET6_ADDRSTRLEN);
                lua_pushstring(L, addr6str);
                lua_pushinteger(L, port);
                return 2;
            } else {
                return luaL_error(L, "unsupported network sockaddr type.");
            }
        }
    } else {
        return luaL_error(L, "unable to get connected host.");
    }
    return 0;
}

static int client_state(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int ret = zoo_state(handle->zh);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_is_unrecoverable(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int ret = is_unrecoverable(handle->zh);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_add_auth(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        const char *scheme = luaL_checkstring(L, 4);
        size_t cert_len = 0;
        const char *cert = luaL_checklstring(L, 5, &cert_len);

        int ret = zoo_add_auth(handle->zh, scheme, cert, cert_len, void_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aget_children(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        int watch = luaL_checkinteger(L, 5);

        int ret = zoo_aget_children(handle->zh, path, watch, strings_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aget_children2(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        int watch = luaL_checkinteger(L, 5);

        int ret = zoo_aget_children2(handle->zh, path, watch, strings_stat_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_async(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        int ret = zoo_async(handle->zh, path, string_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aget_acl(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);

        int ret = zoo_aget_acl(handle->zh, path, acl_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aset_acl(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);

        int version = luaL_checkinteger(L, 5);

        struct ACL_vector acl;
        if (!_zk_acls_create(L, 6, &acl)){
            return luaL_error(L,"invalid ACL format.");
        }

        int ret = zoo_aset_acl(handle->zh, path, version, &acl, void_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);

        _zk_acls_free(&acl);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aget(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);

        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        int watch = luaL_checkinteger(L, 5);

        int ret = zoo_aget(handle->zh, path, watch, data_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aset(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        size_t value_len = 0;
        const char *value = luaL_checklstring(L, 5, &value_len);
        int version = luaL_checkinteger(L, 6);

        int ret = zoo_aset(handle->zh, path, value, value_len, version, stat_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_acreate(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);

        size_t value_len=0;
        const char *value = luaL_checklstring(L, 5, &value_len);

        struct ACL_vector acl;
        if (!_zk_acls_create(L, 6, &acl)){
            return luaL_error(L,"invalid ACL format.");
        }
        int flags = luaL_checkinteger(L, 7);

        int ret = zoo_acreate(handle->zh, path, value, value_len,
            (const struct ACL_vector *)&acl, flags, string_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        _zk_acls_free(&acl);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_adelete(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        int version = luaL_checkinteger(L, 5);
        int ret = zoo_adelete(handle->zh, path, version, void_completion_dispatch, wrapper);
        lua_pushnumber(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}

static int client_aexists(lua_State *L)
{
    struct zk_handle *handle = luaL_checkudata(L, 1, ZK_METATABLE_NAME);
    if (_zk_handle_check(L, handle)) {
        int session = luaL_checkinteger(L, 2);
        size_t watcher_len = 0;
        const char *watcher = luaL_checklstring(L, 3, &watcher_len);
        struct zk_context* wrapper = _zk_context_create(handle->opaque, watcher, watcher_len, session);

        size_t path_len = 0;
        const char *path = luaL_checklstring(L, 4, &path_len);
        int watch = luaL_checkinteger(L, 5);
        int ret = zoo_aexists(handle->zh, path, watch, stat_completion_dispatch, wrapper);
        lua_pushinteger(L, ret);
        return 1;
    } else {
        return luaL_error(L, "invalid zookeeper handle.");
    }
}


static const luaL_Reg client_libs[] =
{
    {"init", client_init},
    {"close", client_close},

    {"aget", client_aget},
    {"aset", client_aset},
    {"acreate", client_acreate},
    {"adelete", client_adelete},
    {"aexists", client_aexists},
    {"aget_children", client_aget_children},
    {"aget_children2", client_aget_children2},
    {"async", client_async},
    {"aget_acl", client_aget_acl},
    {"aset_acl", client_aset_acl},

    {"client_id", client_client_id},
    {"set_context", client_set_context},
    {"get_context", client_get_context},
    {"recv_timeout", client_recv_timeout},

    {"state", client_state},
    {"add_auth", client_add_auth},
    {"is_unrecoverable", client_is_unrecoverable},
    {"get_connected_host", client_get_connected_host},

    {"error", client_error},
    {"set_log_stream", client_set_log_stream},
    {"set_debug_level", client_set_debug_level},
    {"deterministic_conn_order", client_deterministic_conn_order},

    {NULL, NULL}
};

#define client_register_constant(s)\
    lua_pushinteger(L, s);\
    lua_setfield(L, -2, #s)

int luaopen_zookeeper_client(lua_State *L)
{
    luaL_newmetatable(L, ZK_METATABLE_NAME);
    luaL_newlib(L, client_libs);

    //register zookeeper constants in lua.
    client_register_constant(ZOK);

    //System and server-side errors.
    client_register_constant(ZSYSTEMERROR);
    client_register_constant(ZRUNTIMEINCONSISTENCY);
    client_register_constant(ZCONNECTIONLOSS);
    client_register_constant(ZMARSHALLINGERROR);
    client_register_constant(ZUNIMPLEMENTED);
    client_register_constant(ZOPERATIONTIMEOUT);
    client_register_constant(ZBADARGUMENTS);
    client_register_constant(ZINVALIDSTATE);

    //API errors.
    client_register_constant(ZAPIERROR);
    client_register_constant(ZNONODE);
    client_register_constant(ZNOAUTH);
    client_register_constant(ZBADVERSION);
    client_register_constant(ZNOCHILDRENFOREPHEMERALS);
    client_register_constant(ZNODEEXISTS);
    client_register_constant(ZNOTEMPTY);
    client_register_constant(ZSESSIONEXPIRED);
    client_register_constant(ZINVALIDCALLBACK);
    client_register_constant(ZINVALIDACL);
    client_register_constant(ZAUTHFAILED);
    client_register_constant(ZCLOSING);
    client_register_constant(ZNOTHING);
    client_register_constant(ZSESSIONMOVED);

    //ACL Constants.
    client_register_constant(ZOO_PERM_READ);
    client_register_constant(ZOO_PERM_WRITE);
    client_register_constant(ZOO_PERM_DELETE);
    client_register_constant(ZOO_PERM_ADMIN);
    client_register_constant(ZOO_PERM_ALL);

    //Debug Levels.
    client_register_constant(ZOO_LOG_LEVEL_ERROR);
    client_register_constant(ZOO_LOG_LEVEL_WARN);
    client_register_constant(ZOO_LOG_LEVEL_INFO);
    client_register_constant(ZOO_LOG_LEVEL_DEBUG);

    //Interest Constants.
    client_register_constant(ZOOKEEPER_WRITE);
    client_register_constant(ZOOKEEPER_READ);

    //Create Flags.
    client_register_constant(ZOO_EPHEMERAL);
    client_register_constant(ZOO_SEQUENCE);

    //State Constants.
    client_register_constant(ZOO_EXPIRED_SESSION_STATE);
    client_register_constant(ZOO_AUTH_FAILED_STATE);
    client_register_constant(ZOO_CONNECTING_STATE);
    client_register_constant(ZOO_ASSOCIATING_STATE);
    client_register_constant(ZOO_CONNECTED_STATE);

    //Watch Types.
    client_register_constant(ZOO_CREATED_EVENT);
    client_register_constant(ZOO_DELETED_EVENT);
    client_register_constant(ZOO_CHANGED_EVENT);
    client_register_constant(ZOO_CHILD_EVENT);
    client_register_constant(ZOO_SESSION_EVENT);
    client_register_constant(ZOO_NOTWATCHING_EVENT);

    return 1;
}
