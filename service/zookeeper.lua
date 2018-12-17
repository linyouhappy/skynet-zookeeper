
local skynet = require "skynet"
local client = require "zookeeper.client"

local zookeeper = nil
local query_queue = {}
local session_uid = 0
local watchers = {}

local function response_data(session, cmd, result)
    local response = query_queue[session]
    if response then
        query_queue[session] = nil
        response(true, result)
    end
    -- skynet.error("response_data==>> session=",session,"cmd=",cmd)
    -- skynet.error(table.tostring({...}))
end

local function query_data(session)
    query_queue[session] = skynet.response()
end

local function genid()
    session_uid = session_uid+1
    return session_uid
end 

--外部API--------------------------------------------------------
local command = {}

function command.create(source, path, value, acls, flags)
    local session = genid()
    local ret = client.acreate(zookeeper, session, "acreate", path, value, acls, flags)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.delete(source, path, version)
    local session = genid()
    local ret = client.adelete(zookeeper, session, "adelete", path, version)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.exists(source, path, watch)
    local session = genid()
    local ret = client.aexists(zookeeper, session, "aexists", path, watch)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.get(source, path, watch)
    local session = genid()
    local ret = client.aget(zookeeper, session, "aget", path, watch)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.set(source, path, value, version)
    local session = genid()
    value = tostring(value)
    local ret = client.aset(zookeeper, session, "aset", path, value, version)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.get_children(source, path, watch)
    local session = genid()
    local ret = client.aget_children(zookeeper, session, "aget_children", path, watch)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.get_children2(source, path, watch)
    local session = genid()
    local ret = client.aget_children2(zookeeper, session, "aget_children2", path, watch)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.sync(source, path)
    local session = genid()
    local ret = client.async(zookeeper, session, "async", path)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.get_acl(source, path)
    local session = genid()
    local ret = client.aget_acl(zookeeper, session, "aget_acl", path)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.set_acl(source, path, version, acls)
    local session = genid()
    local ret = client.aset_acl(zookeeper, session, "aset_acl", path, version, acls)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.add_auth(source, scheme, cert)
    local session = genid()
    local ret = client.add_auth(zookeeper, session, "add_auth", scheme, cert)
    if ret ~= client.ZOK then
        return true
    end
    query_data(session)
end

function command.client_id(source)
    return true, client.client_id(zookeeper)
end

function command.is_unrecoverable(source)
    return true, client.is_unrecoverable(zookeeper)
end

-- function command.set_context(source,opaque, watcher)
--     return client.set_context(zookeeper,opaque, watcher)
-- end

-- function command.get_context(source)
--     return client.get_context(zookeeper)
-- end

function command.recv_timeout(source)
    return true, client.recv_timeout(zookeeper)
end

function command.state(source)
    return true, client.state(zookeeper)
end

function command.get_connected_host(source)
    return true, client.get_connected_host(zookeeper)
end

---------------------------------------------------
function command.addwatcher(source, method)
    watchers[source] = method
    return true
end

function command.removewatcher(source)
    watchers[source] = nil
    return true
end

----------------------------------------------------------
function command.set_debug_level(source, debug_level)
    return true, client.set_debug_level(debug_level)
end

function command.set_log_stream(source, log_stream)
    return true, client.set_log_stream(log_stream)
end

function command.error(source, code)
    assert(code)
    return true, client.error(code)
end

function command.deterministic_conn_order(source, yesorno)
    return true, client.deterministic_conn_order(yesorno)
end
    

--内部API------------------------------------------------------
local response = {}

-- state= 1 连接建立中 ZOO_CONNECTING_STATE
-- state= 2 (暂时不清楚如何理解这个状态,ZOO_ASSOCIATING_STATE)
-- state= 999 无连接状态

local function print_watcher_log(result)
    if result.state == client.ZOO_CONNECTED_STATE then
        skynet.error("连接已建立状态")
    elseif result.state == client.ZOO_EXPIRED_SESSION_STATE then
        skynet.error("会话超时状态")
    elseif result.state == client.ZOO_AUTH_FAILED_STATE then
        skynet.error("认证失败状态")
    elseif result.state == client.ZOO_CONNECTING_STATE then
        skynet.error("连接建立中")
    elseif result.state == client.ZOO_ASSOCIATING_STATE then
        skynet.error("关联中状态")
    else
        skynet.error("未知状态 state=",result.state)
    end
    
    if result.type == client.ZOO_SESSION_EVENT then
        skynet.error("会话事件")
    elseif result.type == client.ZOO_CREATED_EVENT then
        skynet.error("节点创建事件")
    elseif result.type == client.ZOO_DELETED_EVENT then
        skynet.error("节点删除事件")
    elseif result.type == client.ZOO_CHANGED_EVENT then
        skynet.error("节点数据改变事件")
    elseif result.type == client.ZOO_CHILD_EVENT then
        skynet.error("子节点列表改变事件")
    elseif result.type == client.ZOO_NOTWATCHING_EVENT then
        skynet.error("watch移除事件")
    else
        skynet.error("未知事件")
    end
end

function response.global_watcher(result)
    -- print_watcher_log(result)
    for source,method in pairs(watchers) do
        skynet.send(source, "lua", method, result)
    end 
    -- skynet.error("response.global_watcher =")
    -- skynet.error(table.tostring(result))
end

-----------------------------------------------
local function close_zookeeper()
    assert(zookeeper)
    local ret = client.close(zookeeper)
    zookeeper = nil
    return ret
end

local function init_zookeeper()
    assert(zookeeper == nil)
    local host = assert(skynet.getenv("zookeeper_host"))
    local timeout = assert(skynet.getenv("zookeeper_timeout"))
    local opaque = skynet.self()
    local watcher = "global_watcher"
    local clientid = {
        client_id = 0,
        passwd = "",
    }
    local flags = 0
    zookeeper = client.init(host, opaque, watcher, timeout, clientid, flags)
    skynet.error("zookeeper init success host:",host)
end

skynet.register_protocol {
    name = "text",
    id = skynet.PTYPE_TEXT,
    pack = function(...) return assert(false) end,
    unpack = skynet.unpack
}

skynet.start(function()
    skynet.dispatch("text", function(_, _, cmd, session, ...)
        if session == 0 then
            local f = response[cmd]
            if f then
                f(...)
            else
                skynet.error("no implement response cmd=",cmd)
                for k,v in pairs({...}) do
                    skynet.error(k,v)
                end
            end
        else
            response_data(session, cmd, ...)
        end
    end)

    skynet.dispatch("lua", function(session,source,cmd,...)
        local ret = {command[cmd](source, ...)}
        if ret[1] then
            table.remove(ret, 1)
            skynet.ret(skynet.pack(table.unpack(ret)))
        end
    end)
    init_zookeeper()
end)

