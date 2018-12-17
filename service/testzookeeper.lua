---------------------------------------------------------------------
---------------------------------------------------------------------
local service       = require "service"
local skynet        = require "skynet"
local zookeeper     = require "common.zookeeper"


local function __gtab(depth)
    local retval = {}
    for i = 1, depth do
        table.insert(retval,"\t")
    end
    return table.concat(retval,"")
end

local function __dump(t, depth)
    if type(t) ~= "table" then return tostring(t) end
    local retval = {}
    depth = depth + 1
    if depth > 20 then
        table.insert(retval,"...")
    else
        table.insert(retval,"{\n")
        for k, v in pairs(t) do
            if k ~= "board" then
                if type(v) == "number" or type(v) == "table" or type(v) == "string" or type(v) == "nil" then
                    table.insert(retval, string.format("%s%s = %s,\n", __gtab(depth), k, __dump(v, depth)))
                end
            end
        end
        table.insert(retval, string.format("%s}", __gtab(depth - 1)))
    end
    return table.concat(retval,"")
end

function Tostring(t)
    return type(t) ~= "table" and t or __dump(t, 0)
end

---------------------------------------------------------------------
--- 服务导出业务接口
---------------------------------------------------------------------
local command = {}


function command.zookeeperwatcher(source,result)
    skynet.error("command.zookeeperwatcher==========>>")
    skynet.error(Tostring(result))

    --监听改动只有一次，下次需要监听改动，需设置监听
    if result.path then
        zookeeper.exists(result.path, 1)
    end
end

local function print_log(ret, ...)
    skynet.error("[test]==========================>>rc:",zookeeper.get_code_name(ret.rc),...)
    skynet.error(Tostring(ret))
    skynet.error("[test]<<========================")
    skynet.error("")
end

function command.test_auth()
     --权限
    -- zookeeper.ZOO_PERM_READ
    -- zookeeper.ZOO_PERM_WRITE
    -- zookeeper.ZOO_PERM_CREATE
    -- zookeeper.ZOO_PERM_DELETE
    -- zookeeper.ZOO_PERM_ADMIN
    -- zookeeper.ZOO_PERM_ALL
    skynet.error("测试 get_acl")
    local aclret = zookeeper.get_acl("/helloroot")
    print_log(aclret,"zookeeper.get_acl")

    local acls = {
        {
            perms = zookeeper.ZOO_PERM_READ|zookeeper.ZOO_PERM_WRITE,
            scheme = "digest",
            id = "user1:HYGa7IZRm2PUBFiFFu8xY2pPP/s="
        }
    }

    local aversion = aclret.stat.aversion
    skynet.error("测试set_acl")
    local aret = zookeeper.set_acl("/helloroot", aversion, acls)
    print_log(aret,"zookeeper.set_acl")

    local aclret = zookeeper.get_acl("/helloroot")
    print_log(aclret,"zookeeper.get_acl")

    if (sret.rc == zookeeper.ZNOAUTH) then
        skynet.error("无权限zookeeper.set")
    else
        assert(sret.rc == zookeeper.ZOK)
    end

    skynet.error("测试add_auth")
    local aret = zookeeper.add_auth("digest", "user1:123456")
    print_log(aret,"zookeeper.add_auth")

    assert(aret.rc == zookeeper.ZOK)

    skynet.error("测试set")
    local version = eret.stat.version
    local sret = zookeeper.set("/helloroot", "set_value", version)
    print_log(sret,"zookeeper.set")

    if (sret.rc == zookeeper.ZNOAUTH) then
        skynet.error("无权限zookeeper.set")
    else
        assert(sret.rc == zookeeper.ZOK)
    end
end

function command.test()
    skynet.error("测试zookeeper")

    zookeeper.set_log_stream("./zookeeper.log")
    zookeeper.set_debug_level(zookeeper.ZOO_LOG_LEVEL_ERROR)

    --regrister watcher event
    skynet.error("注册监听watcher事件")
    zookeeper.addwatcher("zookeeperwatcher")

    skynet.error("zookeeper.client_id()=",zookeeper.client_id())
    skynet.error("zookeeper.is_unrecoverable()=",zookeeper.is_unrecoverable())
    skynet.error("zookeeper.recv_timeout()=",zookeeper.recv_timeout())
    skynet.error("zookeeper.state()=",zookeeper.get_code_name(zookeeper.state()))
    skynet.error("zookeeper.get_connected_host()=",zookeeper.get_connected_host())

   
    local acls = {
        {
            perms = zookeeper.ZOO_PERM_ALL,
            scheme = "world",
            id = "anyone"
        }
    }

    local flags = 0
    local watch = 1

    ----------------------------------------------
    skynet.error("测试exists")
    local eret = zookeeper.exists("/root", watch)
    print_log(eret,"zookeeper.exists")

    if eret.rc == zookeeper.ZNONODE then
        skynet.error("测试create")
        local cret = zookeeper.create("/root", "root_value", acls, flags)
        print_log(cret,"zookeeper.create")

        local path = cret.value
        eret = zookeeper.get(path, watch)
        print_log(eret,"zookeeper.get")

        assert(eret.rc == zookeeper.ZOK)
    else
        assert(eret.rc == zookeeper.ZOK)
    end

    ------------------------------------------------
    skynet.error("测试set")
    local version = eret.stat.version
    local sret = zookeeper.set("/root", "set_value", version)
    print_log(sret,"zookeeper.set")

    skynet.error("测试get")
    local gret = zookeeper.get("/root", watch)
    print_log(gret,"zookeeper.get")

    skynet.error("测试create child")
    local cret = zookeeper.create("/root/child1", "child1_value", acls, flags)
    print_log(cret,"zookeeper.create")
    local cret = zookeeper.create("/root/child2", "child2_value", acls, flags)
    print_log(cret,"zookeeper.create")

    local gret = zookeeper.get_children("/root", watch)
    print_log(gret,"zookeeper.get_children")
    local gret = zookeeper.get_children2("/root", watch)
    print_log(gret,"zookeeper.get_children2")

    skynet.error("测试delete")
    local version = gret.stat.version
    local dret = zookeeper.delete("/root", version)
    print_log(dret,"zookeeper.delete")

end

---------------------------------------------------------------------
--- 服务事件回调（底层事件通知）
---------------------------------------------------------------------
local server = {}

-- 服务构造通知
-- 1. 构造参数
function server.on_init(config)

end

-- 服务退出通知
function server.on_exit()
end

-- 业务指令调用
-- 1. 指令来源
-- 2. 指令名称
-- 3. 执行参数
function server.on_command(source, cmd, ...)
    local fn = command[cmd]
    if fn then
        return fn(source, ...)
    else
        ERROR("ucenterd : command[%s] not found!!!", cmd)
    end
end

-- 启动服务对象
service.simple.start(server)


