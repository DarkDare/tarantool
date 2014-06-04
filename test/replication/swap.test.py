import os
import tarantool
from lib.tarantool_server import TarantoolServer

REPEAT = 20
ID_BEGIN = 0
ID_STEP = 5

def insert_tuples(_server, begin, end, msg = "tuple"):
    for i in range(begin, end):
        _server.sql("insert into t0 values (%d, '%s %d')" % (i, msg, i))

def select_tuples(_server, begin, end):
    for i in range(begin, end):
        _server.sql("select * from t0 where k0 = %d" % i)

# master server
master = server
# replica server
replica = TarantoolServer()
replica.script = "replication/replica.lua"
replica.rpl_master = master
replica.vardir = os.path.join(server.vardir, 'replica')
replica.deploy()

master.admin("box.schema.user.grant('guest', 'read,write,execute', 'universe')")
replica.admin("while box.space['_priv']:len() < 1 do require('fiber').sleep(0.01) end")
master.admin("s = box.schema.create_space('tweedledum', {id = 0})")
master.admin("s:create_index('primary', {type = 'hash'})")

master_id = master.get_param('node')['id']
replica_id = replica.get_param('node')['id']

id = ID_BEGIN
for i in range(REPEAT):
    print "test %d iteration" % i

    # insert to master
    insert_tuples(master, id, id + ID_STEP)
    # select from replica
    replica.wait_lsn(master_id, master.get_lsn(master_id))
    select_tuples(replica, id, id + ID_STEP)
    id += ID_STEP

    # insert to master
    insert_tuples(master, id, id + ID_STEP)
    # select from replica
    replica.wait_lsn(master_id, master.get_lsn(master_id))
    select_tuples(replica, id, id + ID_STEP)
    id += ID_STEP

    print "swap servers"
    # reconfigure replica to master
    replica.rpl_master = None
    print("switch replica to master")
    replica.admin("box.cfg{replication_source=''}")
    # reconfigure master to replica
    master.rpl_master = replica
    print("switch master to replica")
    master.admin("box.cfg{replication_source='127.0.0.1:%s'}" % replica.sql.port, silent=True)

    # insert to replica
    insert_tuples(replica, id, id + ID_STEP)
    # select from master
    master.wait_lsn(replica_id, replica.get_lsn(replica_id))
    select_tuples(master, id, id + ID_STEP)
    id += ID_STEP

    # insert to replica
    insert_tuples(replica, id, id + ID_STEP)
    # select from master
    master.wait_lsn(replica_id, replica.get_lsn(replica_id))
    select_tuples(master, id, id + ID_STEP)
    id += ID_STEP

    print "rollback servers configuration"
    # reconfigure replica to master
    master.rpl_master = None
    print("switch master to master")
    master.admin("box.cfg{replication_source=''}")
    # reconfigure master to replica
    replica.rpl_master = master
    print("switch replica to replica")
    replica.admin("box.cfg{replication_source='127.0.0.1:%s'}" % master.sql.port, silent=True)


# Cleanup.
replica.stop()
replica.cleanup(True)
server.stop()
server.deploy()
