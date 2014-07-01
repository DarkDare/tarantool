os.execute("rm -rf sophia")
--# stop server default
--# start server default

space = box.schema.create_space('tweedledum', { id = 123, engine = 'sophia' })
space:create_index('primary', { type = 'tree', parts = {1, 'num'} })

for v=1, 10 do space:insert({v}) end

t = space.index[0]:select({}, {iterator = box.index.ALL})
t

t = space.index[0]:select({}, {iterator = box.index.GE})
t

t = space.index[0]:select(4, {iterator = box.index.GE})
t

t = space.index[0]:select({}, {iterator = box.index.LE})
t

t = space.index[0]:select(7, {iterator = box.index.LE})
t

t = {}
for v=1, 10 do table.insert(t, space:get({v})) end
t

space:drop()
box.snapshot()
os.execute("rm -rf sophia")
