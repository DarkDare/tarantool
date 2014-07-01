fiber = require('fiber')
space = box.schema.create_space('tweedledum')
space:create_index('primary', { type = 'tree'})
box.schema.user.grant('guest', 'read,write,execute', 'universe')
remote = box.net.box.new('localhost', string.gsub(box.cfg.primary_port, '^.*:', ''), '0.5')
type(remote)
remote:ping()
remote:ping()
box.net.box.ping(remote)
space:insert{123, 'test1', 'test2'}
space:select{123}
space:get{123}
remote:select(space.id, 123)
remote:get(space.id, 123)

internal = require('box.internal')
function test(...) return box.tuple.new({ 123, 456 }) end
f, a = internal.call_loadproc('test')
type(f)
type(a)

remote:call('test')
function test(...) return box.tuple.new({ ... }) end
remote:call('test', 123, 345, { 678, 910 })
function test(...) return box.tuple.new({ ... }), box.tuple.new({ ... }) end
remote:call('test', 123, 345, { 678, 910 })
test = { a = 'a', b = function(self, ...) return box.tuple.new(123) end }
remote:call('test:b')
test.b = function(self, ...) return box.tuple.new({self.a, ...}) end
f, a = internal.call_loadproc('test:b')
type(f)
type(a)
a.a
f, a = internal.call_loadproc('test.b')
type(f)
type(a)

remote:call('test:b')
remote:call('test:b', 'b', 'c')
remote:call('test:b', 'b', 'c', 'd', 'e')
remote:call('test:b', 'b', { 'c', { d = 'e' } })


test = { a = { c = 1, b = function(self, ...) return { self.c, ... } end } }
f, a = internal.call_loadproc('test.a:b')
type(f)
type(a)
a.c
f, a = internal.call_loadproc('test.a.b')
type(f)
type(a)

remote:call('test.a:b', 123)

box.space.tweedledum:get(123)
box.space.tweedledum:get({123})
remote:call('box.space.tweedledum:get', 123)
remote:call('box.space.tweedledum:get', {123})

box.space.tweedledum:select(123)
box.space.tweedledum:select({123})
remote:call('box.space.tweedledum:select', 123)
remote:call('box.space.tweedledum:select', {123})

slf, foo = require('box.internal').call_loadproc('box.net.self:select')
type(slf)
type(foo)

space:update(123, {{'=', 2, 'test1-updated'}})
remote:update(space.id, 123, {{'=', 2, 'test2-updated'}})

space:insert{123, 'test1', 'test2'}
remote:insert(space.id, {123, 'test1', 'test2'})

remote:insert(space.id, {345, 'test1', 'test2'})
remote:get(space.id, {345})
remote:select(space.id, {345})
remote:call('box.space.tweedledum:select', 345)
space:get{345}
space:select{345}

remote:put(space.id, {345, 'test1-replaced', 'test3-replaced'})
space:get{345}
space:select{345}

remote:replace(space.id, {345, 'test1-replaced', 'test2-replaced'})
space:get{345}
space:select{345}

space:select({}, { iterator = 'GE', limit = 1000 })
box.net.self:select(space.id, {}, { iterator = 'GE', limit = 1000 })
remote:select(space.id, {}, { limit = 1000, iterator = 'GE' })
space:get{345}
space:select{345}
remote:get(space.id, {345})
remote:select(space.id, {345})
remote:timeout(0.5):get(space.id, {345})
remote:timeout(0.5):select(space.id, {345})



box.net.self:insert(space.id, {12345, 'test1', 'test2'})
box.net.self:replace(space.id, {12346, 'test1', 'test2'})
box.net.self:update(space.id, 12345, {{ '=', 1, 'test11' }})
box.net.self:update(space.id, 12347, {{ '=', 1, 'test11' }})
box.net.self:delete(space.id, 12346)


remote:call('fiber.sleep', .01)
remote:timeout(0.01):call('fiber.sleep', 10)

--# setopt delimiter ';'
pstart = fiber.time();
parallel = {};
function parallel_foo(id)
    fiber.sleep(math.random() * .05)
    return id
end;
parallel_foo('abc');
for i = 1, 20 do
    fiber.resume(
        fiber.create(
            function()
                fiber.detach()
                local s = string.format('%07d', i)
                local so = remote:call('parallel_foo', s)
                table.insert(parallel, s == so[1][1])
            end
        )
    )
end;
for i = 1, 20 do
    if #parallel == 20 then
        break
    end
    fiber.sleep(0.1)
end;
--# setopt delimiter ''
parallel
#parallel
fiber.time() - pstart < 0.5



box.net.self.rpc.box.space.tweedledum.index.primary:get(12345)
box.net.self.rpc.box.space.tweedledum.index.primary:select(12345)
remote.rpc.box.space.tweedledum.index.primary:get(12345)
remote.rpc.box.space.tweedledum.index.primary:select(12345)

remote:close()
remote:close()
remote:ping()

space:drop()
box.schema.user.revoke('guest', 'read,write,execute', 'universe')

fiber = nil
