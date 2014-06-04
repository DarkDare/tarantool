box.schema.user.grant('guest', 'read,write,execute', 'universe')

--------------------------------------------------------------------------------
-- Test case for #273: IPROTO_ITERATOR ignored in network protocol
--------------------------------------------------------------------------------

space = box.schema.create_space('tweedledum')
space:create_index('primary', { type = 'tree'})
for i=1,5 do space:insert{i} end

primary_port = string.gsub(box.cfg.primary_port, '^.*:', '')
conn = box.net.box.new('127.0.0.1', tonumber(primary_port))
conn:select(space.id, 3, { iterator = 'GE' })
conn:select(space.id, 3, { iterator = 'LE' })
conn:select(space.id, 3, { iterator = 'GT' })
conn:select(space.id, 3, { iterator = 'LT' })
conn:close()

space:drop()
