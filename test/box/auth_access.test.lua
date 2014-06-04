session = require('session')
--
-- Check a double create space
--
s = box.schema.create_space('test')
s = box.schema.create_space('test')
--
-- Check a double drop space
--
s:drop()
s:drop()
--
-- Check double create user
--
box.schema.user.create('testus')
box.schema.user.create('testus')

s = box.schema.create_space('admin_space')
s:create_index('primary', {type = 'hash', parts = {0, 'NUM'}})
s:insert({1})
s:insert({2})
--
-- Check double grant and read access
--
box.schema.user.grant('testus', 'read', 'space', 'admin_space')
box.schema.user.grant('testus', 'read', 'space', 'admin_space')

session.su('testus')
s:select(1)
s:insert({3})
s:delete(1)
s:drop()
--
-- Check double revoke
--
session.su('admin')
box.schema.user.revoke('testus', 'read', 'space', 'admin_space')
box.schema.user.revoke('testus', 'read', 'space', 'admin_space')

session.su('testus')
s:select(1)
session.su('admin')
--
-- Check write access on space
-- 
box.schema.user.grant('testus', 'write', 'space', 'admin_space')

session.su('testus')
s:select(1)
s:delete(1)
s:insert({3})
s:drop()
session.su('admin')
--
-- Check double drop user
--
box.schema.user.drop('testus')
box.schema.user.drop('testus')
--
-- Check 'guest' user
--
session.su('guest')
session.uid()
box.space._user:select(1)
s:select(1)
s:insert({4})
s:delete({3})
s:drop()
gs = box.schema.create_space('guest_space')
box.schema.func.create('guest_func')

session.su('admin')
s:select()
--
-- Create user with universe read&write grants
-- and create this user session
--
box.schema.user.create('uniuser')
box.schema.user.grant('uniuser', 'read, write, execute', 'universe')
session.su('uniuser')
if session.uid() < 2 then return 'uid error' end
--
-- Check universal user
-- Check delete currently authenticated user
--
box.schema.user.drop('uniuser')
--
--Check create, call and drop function
--
box.schema.func.create('uniuser_func')
function uniuser_func() return 'hello' end
uniuser_func()
box.schema.func.drop('uniuser_func')
--
-- Check create and drop space
--
us = box.schema.create_space('uniuser_space')
us:drop()
--
-- Check create and drop user
--
box.schema.user.create('uniuser_testus')
box.schema.user.drop('uniuser_testus')
--
-- Check access system and any spaces
--
box.space.admin_space:select()
box.space._user:select(1)
box.space._space:select(280)

us = box.schema.create_space('uniuser_space')
box.schema.func.create('uniuser_func')

session.su('admin')
box.schema.user.create('someuser')
box.schema.user.grant('someuser', 'read, write, execute', 'universe')
session.su('someuser')
--
-- Check drop objects of another user
--
s:drop()
us:drop()
box.schema.func.drop('uniuser_func')
box.schema.user.drop('uniuser_testus')

session.su('admin')
box.schema.func.drop('uniuser_func')
box.schema.user.drop('someuser')
box.schema.user.drop('uniuser_testus')
box.schema.user.drop('uniuser')
box.space._user:delete(2)
s:drop()
--
-- Check write grant on _user
--
box.schema.user.create('testuser')

box.schema.user.grant('testuser', 'write', 'space', '_user')
session.su('testuser')
box.space._user:delete(2)
box.space._user:select(1)
box.space._user:insert{3,'','someone'}
box.space._user:delete(3)

session.su('admin')
box.space._user:select(1)
box.space._user:delete(3)
box.schema.user.revoke('testuser', 'write', 'space', '_user')
--
-- Check read grant on _user
--
box.schema.user.grant('testuser', 'read', 'space', '_user')
session.su('testuser')
box.space._user:delete(2)
box.space._user:select(1)
box.space._user:insert{4,'','someone2'}

session.su('admin')
--
-- Check read grant on _index
--
box.schema.user.grant('testuser', 'read', 'space', '_index')
session.su('testuser')
box.space._index:select(272)
box.space._index:insert{512, 1,'owner','tree', 1, 1, 0,'num'}


session.su('admin')
--
-- Check max function limit
--
--# setopt delimiter ';'
function func_limit()
    local i = 1
    while true do
        box.schema.func.create('func'..i)
        i = i + 1
    end
    return i
end;
function drop_limit_func()
    local i = 1
    while true do
        box.schema.func.drop('func'..i)
        i = i + 1
    end
end;
func_limit();
drop_limit_func();
box.schema.user.grant('testuser', 'read, write, execute', 'universe');
session.su('testuser');
func_limit();
drop_limit_func();
--# setopt delimiter ''

session.su('admin')
box.schema.user.revoke('testuser', 'read, write, execute', 'universe')
--
-- Check that itertors check privileges
--
s = box.schema.create_space('glade') 
box.schema.user.grant('testuser', 'read', 'space', 'glade')
s:create_index('primary', {unique = true, parts = {0, 'NUM', 1, 'STR'}})
s:insert({1, 'A'})
s:insert({2, 'B'})
s:insert({3, 'C'})
s:insert({4, 'D'})

t = {}
for key, v in s.index.primary:pairs(3, {iterator = 'GE'}) do table.insert (t, v) end 
t
t = {}
session.su('testuser')
s:select()
for key, v in s.index.primary:pairs(3, {iterator = 'GE'}) do table.insert (t, v) end 
t
t = {}
session.su('admin')
box.schema.user.grant('testuser', 'write', 'space', 'glade')
session.su('testuser')
s:select()
for key, v in s.index.primary:pairs(1, {iterator = 'GE'}) do table.insert (t, v) end 
t
t = {}
session.su('admin')
box.schema.user.grant('testuser', 'read, write, execute', 'space', 'glade')
session.su('testuser')
s:select()
for key, v in s.index.primary:pairs(3, {iterator = 'GE'}) do table.insert (t, v) end 
t
t = {}

session.su('guest')
s:select()
for key, v in s.index.primary:pairs(3, {iterator = 'GE'}) do table.insert (t, v) end 
t
t = {}

session.su('guest')
s:select()
for key, v in s.index.primary:pairs(3, {iterator = 'GE'}) do table.insert (t, v) end 
t

session.su('admin')

s:drop()

box.space._user:select()
box.space._space:select()
box.space._func:select()

session = nil
