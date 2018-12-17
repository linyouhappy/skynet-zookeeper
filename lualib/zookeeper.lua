local skynet = require "skynet"
local client = require "zookeeper.client"

local service
skynet.init(function()
	service = skynet.uniqueservice "zookeeper"
end)

local zookeeper = {}

--创建一个之前不存在的节点。
-- 如果设置ZOO_EPHEMERAL，客户端会话失效，节点将自动删除；
-- 如果设置ZOO_SEQUENCE，一个唯一的自动增加的序列号附加到路径名，
-- 序列号宽度是10个数字的宽度，不足用0填充
function zookeeper.create(path, value, acls, flags)
	return skynet.call(service, "lua", "create", path, value, acls, flags)
end

--删除一个节点
function zookeeper.delete(path, version)
	return skynet.call(service, "lua", "delete", path, version)
end

-- 检查一个节点是否存在，它允许开启watcher监听
function zookeeper.exists(path, watch)
	return skynet.call(service, "lua", "exists", path, watch)
end

--获取节点数据(legacy方式)。它允许开启watcher监听
-- 其rc参数可能是以下参数：ZOK-完成，ZNONODE-节点不存在，ZNOAUTH-客户端无权限
function zookeeper.get(path, watch)
	return skynet.call(service, "lua", "get", path, watch)
end

--设置节点数据
function zookeeper.set(path, value, version)
	return skynet.call(service, "lua", "set", path, value, version)
end

--获取子节点列表+无状态
function zookeeper.get_children(path, watch)
    return skynet.call(service, "lua", "get_children", path, watch)
end

--获取子节点列表+状态
function zookeeper.get_children2(path, watch)
    return skynet.call(service, "lua", "get_children2", path, watch)
end

--同步该节点
function zookeeper.sync(path)
    return skynet.call(service, "lua", "sync", path)
end

--获取节点的ACL。ACL描述了操作该节点所需具备的条件，
-- 即哪些人(id)具备哪些权限后才允许对节点执行哪些操作。
function zookeeper.get_acl(path)
    return skynet.call(service, "lua", "get_acl", path)
end

--设置节点的ACL
function zookeeper.set_acl(path, version, acls)
    return skynet.call(service, "lua", "set_acl", path, version, acls)
end

--为应用程序指定证书。调用此函数用于认证的证书。服务端用scheme指定的安全服务对客户端连接进行认证。
-- 如果认证失败，将与服务端断开连接，watcher触发，状态码是ZOO_AUTH_FAILED_STATE
function zookeeper.add_auth(scheme, cert)
	return skynet.call(service, "lua", "add_auth", scheme, cert)
end

function zookeeper.client_id()
    return skynet.call(service, "lua", "client_id")
end

--检查zookeeper连接是否可恢复
function zookeeper.is_unrecoverable()
    return skynet.call(service, "lua", "is_unrecoverable")
end

----------------------------------------------------------
--返回会话超时时间，仅在于服务端连接正常时有效，该值在与服务器重连后可能改变
function zookeeper.recv_timeout()
    return skynet.call(service, "lua", "recv_timeout")
end

--返回句柄状态
function zookeeper.state()
    return skynet.call(service, "lua", "state")
end

--返回服务端的网络地址，仅在与服务端连接正常是有效
function zookeeper.get_connected_host()
    return skynet.call(service, "lua", "get_connected_host")
end

--开启接收监听watcher的消息
function zookeeper.addwatcher(cmd_func_name)
	skynet.call(service, "lua", "addwatcher",cmd_func_name)
end

function zookeeper.removewatcher()
	skynet.call(service, "lua", "removewatcher")
end

--设置调试级别
function zookeeper.set_debug_level(debug_level)
    return skynet.call(service, "lua", "set_debug_level", debug_level)
end

--设置用于记录日志的文件流。默认使用stderr。若logStream为NULL，则使用默认值stderr。
function zookeeper.set_log_stream(log_file)
	return skynet.call(service, "lua", "set_log_stream", log_file)
end

--返回错误信息
function zookeeper.error(code)
    return skynet.call(service, "lua", "error")
end

-- 用于启用或停用quarum端点的随机化排序，通常仅在测试时使用。
-- 如果非0，使得client连接到quarum端按照被初始化的顺序。
-- 如果是0，zookeeper_init将变更端点顺序，使得client连接分布在更优的端点上。
function zookeeper.deterministic_conn_order(yesorno)
	return skynet.call(service, "lua", "deterministic_conn_order", yesorno)
end

--代码名称
function zookeeper.get_code_name(code)
    for k,v in pairs(zookeeper) do
        if type(v) == "number" and code == v then
            return k
        end
    end
end

--C constants register
for k,v in pairs(client) do
	if type(v) == "number" then
		zookeeper[k] = v
	end
end

-- * register zookeeper constants in lua.
-- ZOK

-- * System and server-side errors.
-- ZSYSTEMERROR
-- ZRUNTIMEINCONSISTENCY
-- ZCONNECTIONLOSS
-- ZMARSHALLINGERROR
-- ZUNIMPLEMENTED
-- ZOPERATIONTIMEOUT
-- ZBADARGUMENTS
-- ZINVALIDSTATE

--  * API errors.
-- ZAPIERROR
-- ZNONODE
-- ZNOAUTH
-- ZBADVERSION
-- ZNOCHILDRENFOREPHEMERALS
-- ZNODEEXISTS
-- ZNOTEMPTY
-- ZSESSIONEXPIRED
-- ZINVALIDCALLBACK
-- ZINVALIDACL
-- ZAUTHFAILED
-- ZCLOSING
-- ZNOTHING
-- ZSESSIONMOVED

--  * ACL Constants.
-- ZOO_PERM_READ
-- ZOO_PERM_WRITE
-- ZOO_PERM_DELETE
-- ZOO_PERM_ADMIN
-- ZOO_PERM_ALL

--  * Debug Levels.
-- ZOO_LOG_LEVEL_ERROR
-- ZOO_LOG_LEVEL_WARN
-- ZOO_LOG_LEVEL_INFO
-- ZOO_LOG_LEVEL_DEBUG

--  * Interest Constants.
-- ZOOKEEPER_WRITE
-- ZOOKEEPER_READ

--  * Create Flags.
-- ZOO_EPHEMERAL
-- ZOO_SEQUENCE

--  * State Constants.
-- ZOO_EXPIRED_SESSION_STATE
-- ZOO_AUTH_FAILED_STATE
-- ZOO_CONNECTING_STATE
-- ZOO_ASSOCIATING_STATE
-- ZOO_CONNECTED_STATE

--  * Watch Types.
-- ZOO_CREATED_EVENT
-- ZOO_DELETED_EVENT
-- ZOO_CHANGED_EVENT
-- ZOO_CHILD_EVENT
-- ZOO_SESSION_EVENT
-- ZOO_NOTWATCHING_EVENT


return zookeeper