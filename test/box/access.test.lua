session = require('session')
-- user id for a Lua session is admin - 1
session.uid()
-- extra arguments are ignored
session.uid(nil)
-- admin
session.user()
-- extra argumentes are ignored
session.user(nil)
-- password() is a function which returns base64(sha1(sha1(password))
-- a string to store in _user table
box.schema.user.password('test')
box.schema.user.password('test1')
-- admin can create any user
box.schema.user.create('test', { password = 'test' })
-- su() let's you change the user of the session
-- the user will be unabe to change back unless he/she
-- is granted access to 'su'
session.su('test')
-- you can't create spaces unless you have a write access on
-- system space _space
-- in future we may  introduce a separate privilege
box.schema.create_space('test')
-- su() goes through because called from admin
-- console, and it has no access checks
-- for functions
session.su('admin')
box.schema.user.grant('test', 'write', 'space', '_space')

--# setopt delimiter ';'
function usermax()
    local i = 1
    while true do
        box.schema.user.create('user'..i)
        i = i + 1
    end
end;
usermax();
function usermax()
    local i = 1
    while true do
        box.schema.user.drop('user'..i)
        i = i + 1
    end
end;
usermax();
--# setopt delimiter ''
box.schema.user.create('rich')
box.schema.user.grant('rich', 'read,write', 'universe')
session.su('rich')
uid = session.uid()
box.schema.func.create('dummy')
session.su('admin')
box.space['_user']:delete{uid}
box.schema.func.drop('dummy')
box.space['_user']:delete{uid}
box.schema.user.revoke('rich', 'read,write', 'universe')
box.space['_user']:delete{uid}
box.schema.user.drop('test')
session = nil

--------------------------------------------------------------------------------
-- #198: names like '' and 'x.y' and 5 and 'primary ' are legal
--------------------------------------------------------------------------------
-- invalid identifiers
box.schema.user.create('invalid.identifier')
box.schema.user.create('invalid identifier')
box.schema.user.create('user ')
box.schema.user.create('5')
box.schema.user.create(' ')

-- valid identifiers
box.schema.user.create('Петя_Иванов')
box.schema.user.drop('Петя_Иванов')
