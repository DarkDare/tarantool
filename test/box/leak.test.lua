space = box.schema.create_space('leak')
space:create_index('primary', { type = 'hash' })
for i = 1,10000,1 do box.begin(); space:insert{i}; space:delete{i}; box.commit() end
space:len()
for i = 1,10000,1 do space:insert{i}; space:delete{i} end
space:len()
space:drop()

