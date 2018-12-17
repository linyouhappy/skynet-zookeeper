# skynet-zookeeper客户端
	专为skynet开发的zookeeper客户端，版本：zookeeper-3.4.13
## Test
	1.先启动zookeeper服务器
	2.配置
	--Linux动态库，放入到luaclib文件夹
	--(已提供源文件luaclib-src/lua-zookeeper.c，参考MakeFile配置)
	luaclib/zookeeper.so
	--skynet的service
	service/zookeeper.lua
	--导出api
	lualib/zookeeper.lua

	--测试代码
	service/testzookeeper.lua

	测试代码片段(skynet环境下测试)
	```

	local zookeeper  = require "zookeeper"

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
	```

	## About
	借鉴了zklua
	交流QQ群：703359062
