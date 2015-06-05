/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#define MH_SOURCE 1
#include "box/bsync.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "cfg.h"
#include "fio.h"
#include "coio.h"
#include "coio_buf.h"
#include "memory.h"
#include "scoped_guard.h"
#include "box/box.h"
#include "box/txn.h"
#include "box/port.h"
#include "box/schema.h"
#include "box/space.h"
#include "box/tuple.h"
#include "box/request.h"
#include "box/replication.h"
#include "msgpuck/msgpuck.h"

#include "box/bsync_hash.h"
#include "box/iproto_constants.h"

#define BSYNC_MAX_HOSTS VCLOCK_MAX
#define BSYNC_PAIR_TIMEOUT 30

#define BSYNC_TRACE {\
	assert(bsync_state.num_hosts >= bsync_state.num_connected); \
	say_debug("[%p] %s:%d current state: nconnected=%d, bstate=%d, tstate=%d,"\
		" leader=%d, bsync_fibers=%ld, txn_fibers=%ld, bsync_active=%ld," \
		" txn_active=%ld, local_sign=%ld, gc_size=%ld", \
	fiber(), __FUNCTION__, __LINE__, bsync_state.num_connected, \
	bsync_state.state, txn_state.state, (int)bsync_state.leader_id, \
	bsync_state.bsync_fibers.size, bsync_state.txn_fibers.size, \
	bsync_state.bsync_fibers.active, bsync_state.txn_fibers.active, \
	bsync_state.local_id < 16 ? bsync_index[bsync_state.local_id].sign : -1, \
	bsync_state.region_free_size); \
}

static void* bsync_thread(void*);
static void bsync_process_fiber(va_list ap);
static void bsync_start_election();
static void bsync_disconnected(uint8_t host_id, bool recovery);
static void bsync_check_consensus(uint8_t host_id);
static void bsync_process(struct bsync_txn_info *info);
static void bsync_process_wal(struct bsync_txn_info *info);
static void txn_process(struct bsync_txn_info *info);
static void bsync_update_local(struct bsync_txn_info *info);
static void bsync_process_connect(struct bsync_txn_info *info);
static void bsync_process_disconnect(struct bsync_txn_info *info);
static void bsync_start_event(struct bsync_txn_info *info);
static void bsync_process_follow(struct bsync_txn_info *info);
static void bsync_do_reject(uint8_t host_id, struct bsync_send_elem *info);

static struct recovery_state *local_state;

static struct ev_loop* txn_loop;
static struct ev_loop* bsync_loop;
static struct ev_async txn_process_event;
static struct ev_async bsync_process_event;

struct bsync_send_elem {/* for save in host queue */
	bool system;
	uint8_t code;
	void *arg;

	struct rlist list;
};

struct bsync_region {
	struct region pool;
	struct rlist list;
};

struct bsync_incoming {
	struct fiber *f;
	const struct tt_uuid *uuid;
	int remote_id;
	struct rlist list;
};

struct bsync_common {
	struct bsync_region *region;
	struct bsync_key *dup_key;
};

static struct bsync_system_status_ {
	uint8_t leader_id;
	uint8_t local_id;

	uint8_t state;
	bool join[BSYNC_MAX_HOSTS];
	bool iproto[BSYNC_MAX_HOSTS];
	bool wait_local[BSYNC_MAX_HOSTS];
	bool recovery;
	uint8_t id2index[BSYNC_MAX_HOSTS];
	pthread_mutex_t mutex[BSYNC_MAX_HOSTS];
	pthread_cond_t cond[BSYNC_MAX_HOSTS];
	struct fiber *snapshot_fiber;

	struct rlist incoming_connections;
	struct rlist wait_start;

	struct vclock vclock;
} txn_state;

struct bsync_txn_info;
typedef void (*bsync_request_f)(struct bsync_txn_info *);

struct bsync_txn_info { /* txn information about operation */
	struct xrow_header *row;
	struct fiber *owner;
	struct bsync_operation *op;
	struct bsync_common *common;
	uint8_t connection;
	int64_t sign;
	int result;
	bool repeat;
	bool proxy;
	bsync_request_f process;

	const char *__from;
	int __line;

	struct rlist list;
	STAILQ_ENTRY(bsync_txn_info) fifo;
};
STAILQ_HEAD(bsync_fifo, bsync_txn_info);

enum bsync_operation_status {
	bsync_op_status_init = 0,
	bsync_op_status_accept = 1,
	bsync_op_status_wal = 2,
	bsync_op_status_submit = 3,
	bsync_op_status_yield = 4,
	bsync_op_status_fail = 5
};

struct bsync_operation {
	uint64_t sign;
	uint8_t host_id;
	uint8_t status;
	uint8_t accepted;
	uint8_t rejected;

	struct fiber *owner;
	struct bsync_common *common;
	struct bsync_txn_info *txn_data;
	struct xrow_header *row;

	struct rlist list;
};

enum bsync_host_flags {
	bsync_host_active_write = 0x01,
	bsync_host_rollback = 0x02,
	bsync_host_reconnect_sleep = 0x04,
	bsync_host_ping_sleep = 0x08,
	bsync_host_follow_fast = 0x10
};

enum bsync_host_state {
	bsync_host_disconnected = 0,
	bsync_host_recovery = 1,
	bsync_host_follow = 2,
	bsync_host_connected = 3
};

struct bsync_host_data {
	char name[1024];
	int remote_id;
	uint8_t state;
	uint8_t flags;
	bool fiber_out_fail;
	struct fiber *fiber_out;
	struct fiber *fiber_in;
	int64_t sign;
	int64_t commit_sign;
	int64_t submit_sign;

	ssize_t send_queue_size;
	struct rlist send_queue;
	ssize_t follow_queue_size;
	struct rlist follow_queue;
	ssize_t op_queue_size;
	struct rlist op_queue;
	struct mh_bsync_t *active_ops;

	struct bsync_txn_info sysmsg;
	struct bsync_send_elem ping_msg;
};
static struct bsync_host_data bsync_index[BSYNC_MAX_HOSTS];

struct bsync_fiber_cache {
	size_t size;
	size_t active;
	struct rlist data;
};

static struct bsync_state_ {
	uint8_t local_id;
	uint8_t leader_id;
	uint8_t accept_id;
	uint8_t num_hosts;
	uint8_t num_connected;
	uint8_t state;
	uint8_t num_accepted;
	uint64_t wal_commit_sign;
	uint64_t wal_rollback_sign;

	ev_tstamp read_timeout;
	ev_tstamp write_timeout;
	ev_tstamp operation_timeout;
	ev_tstamp ping_timeout;
	ev_tstamp submit_timeout;
	ev_tstamp election_timeout;
	ssize_t max_host_queue;

	uint8_t iproxy_host;
	uint64_t iproxy_sign;
	const char** iproxy_pos;
	const char* iproxy_end;

	struct rlist proxy_queue;
	struct rlist txn_queue;
	struct rlist wal_queue;
	struct rlist execute_queue; // executing operations
	struct rlist submit_queue; // submitted operations
	struct rlist commit_queue; // submitted operations

	struct bsync_fiber_cache txn_fibers;
	struct bsync_fiber_cache bsync_fibers;

	struct rlist election_ops;

	struct bsync_fifo txn_proxy_queue;
	struct bsync_fifo txn_proxy_input;

	struct bsync_fifo bsync_proxy_queue;
	struct bsync_fifo bsync_proxy_input;

	struct slab_cache txn_slabc;
	struct mempool region_pool;
	struct slab_cache bsync_slabc;
	struct mempool system_send_pool;
	struct rlist region_free;
	size_t region_free_size;
	struct rlist region_gc;

	/* TODO : temporary hack for support system operations on proxy side */
	struct cord cord;
	pthread_mutex_t mutex;
	pthread_mutex_t active_ops_mutex;
	pthread_cond_t cond;
	struct bsync_txn_info sysmsg;

	bool bsync_rollback;
} bsync_state;

#define bsync_commit_foreach(f) { \
	struct bsync_operation *oper; \
	rlist_foreach_entry(oper, &bsync_state.submit_queue, list) { \
		if (f(oper)) \
			break; \
	} \
	rlist_foreach_entry(oper, &bsync_state.commit_queue, list) { \
		if (f(oper)) \
			break; \
	} \
}

#define BSYNC_LOCAL bsync_index[bsync_state.local_id]
#define BSYNC_LEADER bsync_index[bsync_state.leader_id]
#define BSYNC_REMOTE bsync_index[host_id]
#define BSYNC_LOCK(M) \
	tt_pthread_mutex_lock(&M); \
	auto guard = make_scoped_guard([&]() { \
		tt_pthread_mutex_unlock(&M); \
	})

static struct fiber*
bsync_fiber(struct bsync_fiber_cache *lst, void (*f)(va_list), ...)
{
	struct fiber *result = NULL;
	if (! rlist_empty(&lst->data)) {
		result = rlist_shift_entry(&lst->data, struct fiber, state);
	} else {
		BSYNC_TRACE
		result = fiber_new("bsync_proc", f);
		++lst->size;
	}
	++lst->active;
	return result;
}

static struct bsync_region *
bsync_new_region()
{BSYNC_TRACE
	BSYNC_LOCK(bsync_state.mutex);
	if (!rlist_empty(&bsync_state.region_free)) {
		return rlist_shift_entry(&bsync_state.region_free,
			struct bsync_region, list);
	} else {
		++bsync_state.region_free_size;
		struct bsync_region* region = (struct bsync_region *)
			mempool_alloc0(&bsync_state.region_pool);
		region_create(&region->pool, &bsync_state.txn_slabc);
		return region;
	}
}

static void
bsync_free_region(struct bsync_common *data)
{BSYNC_TRACE
	if (!data->region)
		return;
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_add_tail_entry(&bsync_state.region_gc, data->region, list);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	data->region = NULL;
}

static void
bsync_dump_region()
{
	struct rlist region_gc;
	rlist_create(&region_gc);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_swap(&region_gc, &bsync_state.region_gc);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	if (rlist_empty(&region_gc))
		return;
	struct bsync_region *next =
		rlist_first_entry(&region_gc, struct bsync_region, list);
	struct bsync_region *cur = NULL;
	while (!rlist_empty(&region_gc)) {
		cur = next;
		next = rlist_next_entry(cur, list);
		region_reset(&cur->pool);
		rlist_move_entry(&bsync_state.region_free, cur, list);
	}
	rlist_create(&region_gc);
}

static uint8_t
bsync_max_host()
{
	uint8_t max_host_id = 0;
	for (uint8_t i = 1; i < bsync_state.num_hosts; ++i) {
		if (bsync_index[i].commit_sign >= bsync_index[max_host_id].commit_sign &&
			bsync_index[i].state > bsync_host_disconnected)
		{
			max_host_id = i;
		}
	}
	return max_host_id;
}

enum bsync_message_type {
	bsync_mtype_leader_proposal = 0,
	bsync_mtype_leader_promise = 1,
	bsync_mtype_leader_accept = 2,
	bsync_mtype_leader_submit = 3,
	bsync_mtype_leader_reject = 4,
	bsync_mtype_ping = 5,
	bsync_mtype_iproto_switch = 6,
	bsync_mtype_bsync_switch = 7,
	bsync_mtype_close = 8,
	bsync_mtype_rollback = 9,
	bsync_mtype_sysend = 10,
	bsync_mtype_body = 11,
	bsync_mtype_submit = 12,
	bsync_mtype_reject = 13,
	bsync_mtype_proxy_request = 14,
	bsync_mtype_proxy_accept = 15,
	bsync_mtype_proxy_reject = 16,
	bsync_mtype_proxy_join = 17,
	bsync_mtype_count = 18,
	bsync_mtype_none = 19
};

enum bsync_machine_state {
	bsync_state_election = 0,
	bsync_state_initial = 1,
	bsync_state_promise = 2,
	bsync_state_accept = 3,
	bsync_state_recovery = 4,
	bsync_state_ready = 5,
	bsync_state_shutdown = 6
};

enum txn_machine_state {
	txn_state_join = 0,
	txn_state_snapshot = 1,
	txn_state_subscribe = 2,
	txn_state_recovery = 3,
	txn_state_ready = 4,
	txn_state_rollback = 5
};

enum bsync_iproto_flags {
	bsync_iproto_commit_sign = 0x01,
};

static const char* bsync_mtype_name[] = {
	"leader_proposal",
	"leader_promise",
	"leader_accept",
	"leader_submit",
	"leader_reject",
	"ping",
	"iproto_switch",
	"bsync_switch",
	"close",
	"rollback",
	"INVALID",
	"body",
	"submit",
	"reject",
	"proxy_request",
	"proxy_accept",
	"proxy_reject",
	"proxy_join",
	"INVALID",
	"INVALID"
};

#define SWITCH_TO_BSYNC(cb) {\
	info->process = cb; \
	if (info->row) \
	say_debug("switch to bsync by %p %d:%ld", info, info->row->server_id, info->row->lsn); \
	tt_pthread_mutex_lock(&bsync_state.mutex); \
	{ \
		struct bsync_txn_info *tmp; \
		STAILQ_FOREACH(tmp, &bsync_state.bsync_proxy_queue, fifo) { \
			if (tmp == info) \
				say_debug("%d assert %s:%d", 445, info->__from, info->__line); \
			assert (tmp != info); \
		} \
	} \
	info->__from = __PRETTY_FUNCTION__; \
	info->__line = __LINE__; \
	bool was_empty = STAILQ_EMPTY(&bsync_state.bsync_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.bsync_proxy_queue, info, fifo); \
	if (was_empty) ev_async_send(bsync_loop, &bsync_process_event); \
	tt_pthread_mutex_unlock(&bsync_state.mutex); }

#define SWITCH_TO_TXN(info, cb) {\
	(info)->process = cb; \
	if ((info)->row) \
		say_debug("switch to txn by %p %d:%ld", (info), (info)->row->server_id, (info)->row->lsn); \
	tt_pthread_mutex_lock(&bsync_state.mutex); \
	{ \
		struct bsync_txn_info *tmp; \
		STAILQ_FOREACH(tmp, &bsync_state.txn_proxy_queue, fifo) { \
			if (tmp == info) \
				say_debug("%d assert %s:%d", 468, (info)->__from, (info)->__line); \
			assert (tmp != info); \
		} \
	} \
	(info)->__from = __PRETTY_FUNCTION__; \
	(info)->__line = __LINE__; \
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, info, fifo); \
	if (was_empty) ev_async_send(txn_loop, &txn_process_event); \
	tt_pthread_mutex_unlock(&bsync_state.mutex); }

static bool
bsync_begin_op(struct bsync_key *key, uint32_t server_id)
{BSYNC_TRACE
	if (!local_state->remote[txn_state.leader_id].localhost)
		return true;
	/*
	 * TODO : special analyze for operations with spaces (remove/create)
	 */
	BSYNC_LOCK(bsync_state.active_ops_mutex);
	mh_int_t keys[BSYNC_MAX_HOSTS];
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == txn_state.local_id)
			continue;
		keys[host_id] = mh_bsync_find(BSYNC_REMOTE.active_ops, *key, NULL);
		if (keys[host_id] == mh_end(BSYNC_REMOTE.active_ops))
			continue;
		struct mh_bsync_node_t *node =
			mh_bsync_node(BSYNC_REMOTE.active_ops, keys[host_id]);
		if (server_id != local_state->server_id && node->val.remote_id != server_id) {
			if (node->val.remote_id == local_state->server_id) {
				say_error("conflict with operations from %s and %s"
						" in %s. space is %d",
					local_state->remote[server_id].source,
					BSYNC_LOCAL.name,
					BSYNC_REMOTE.name,
					key->space_id);
			} else {
				say_error("conflict with operations from %s and %s"
						" in %s. space is %d",
					local_state->remote[server_id].source,
					local_state->remote[node->val.remote_id].source,
					BSYNC_REMOTE.name,
					key->space_id);
			}
			return false;
		}
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == txn_state.local_id)
			continue;
		if (keys[host_id] != mh_end(BSYNC_REMOTE.active_ops)) {
			struct mh_bsync_node_t *node =
				mh_bsync_node(BSYNC_REMOTE.active_ops, keys[host_id]);
			if (server_id == 0) {
				++node->val.local_ops;
			} else {
				node->val.remote_id = server_id;
				++node->val.remote_ops;
			}
			node->key.data = key->data;
		} else {
			struct mh_bsync_node_t node;
			node.key = *key;
			if (server_id == 0) {
				node.val.local_ops = 1;
				node.val.remote_ops = 0;
			} else {
				node.val.local_ops = 0;
				node.val.remote_ops = 1;
			}
			node.val.remote_id = server_id;
			mh_bsync_put(BSYNC_REMOTE.active_ops, &node, NULL, NULL);
		}
	}
	return true;
}

static void
bsync_end_op(uint8_t host_id, struct bsync_key *key, uint32_t server_id)
{BSYNC_TRACE
	if (!local_state->remote[txn_state.leader_id].localhost)
		return;
	BSYNC_LOCK(bsync_state.active_ops_mutex);
	mh_int_t k = mh_bsync_find(BSYNC_REMOTE.active_ops, *key, NULL);
	if (k == mh_end(BSYNC_REMOTE.active_ops))
		return;

	struct mh_bsync_node_t *node = mh_bsync_node(BSYNC_REMOTE.active_ops, k);
	if (server_id != 0)
		--node->val.remote_ops;
	else
		--node->val.local_ops;
	if ((node->val.local_ops + node->val.remote_ops) == 0)
		mh_bsync_del(BSYNC_REMOTE.active_ops, k, NULL);
}

struct bsync_parse_data {
	uint32_t space_id;
	bool is_tuple;
	const char *data;
	const char *end;
};

static void
bsync_space_cb(void *d, uint8_t key, uint32_t v)
{
	if (key == IPROTO_SPACE_ID)
		((struct bsync_parse_data *) d)->space_id = v;
}

static void
bsync_tuple_cb(void *d, uint8_t key, const char *v, const char *vend)
{
	((struct bsync_parse_data *) d)->data = v;
	((struct bsync_parse_data *) d)->end = vend;
	((struct bsync_parse_data *) d)->is_tuple = (key == IPROTO_TUPLE);
}

static bool
txn_in_remote_recovery()
{
	return txn_state.leader_id != BSYNC_MAX_HOSTS &&
		((txn_state.state == txn_state_recovery && txn_state.leader_id != txn_state.local_id) ||
		(txn_state.state == txn_state_subscribe && txn_state.leader_id == txn_state.local_id));
}

static bool
txn_in_local_recovery()
{
	return txn_state.local_id == BSYNC_MAX_HOSTS ||
		txn_state.state == txn_state_snapshot;

}

#define BSYNC_MAX_KEY_PART_LEN 256
static void
bsync_parse_dup_key(struct bsync_common *data, struct key_def *key,
		    struct tuple *tuple)
{
	data->dup_key = (struct bsync_key *)
		region_alloc(&data->region->pool, sizeof(struct bsync_key));
	data->dup_key->size = 0;
	data->dup_key->data = (char *)region_alloc(&data->region->pool,
					BSYNC_MAX_KEY_PART_LEN * key->part_count);
	char *i_data = data->dup_key->data;
	for (uint32_t p = 0; p < key->part_count; ++p) {
		if (key->parts[p].type == NUM) {
			uint32_t v = tuple_field_u32(tuple, key->parts[p].fieldno);
			memcpy(i_data, &v, sizeof(uint32_t));
			data->dup_key->size += sizeof(uint32_t);
			i_data += sizeof(uint32_t);
			continue;
		}
		const char *key_part =
			tuple_field_cstr(tuple, key->parts[p].fieldno);
		size_t key_part_len = strlen(key_part);
		data->dup_key->size += key_part_len;
		memcpy(i_data, key_part, key_part_len + 1);
		i_data += data->dup_key->size;
	}
	data->dup_key->space_id = key->space_id;
}

static int
bsync_find_incoming(int id, struct tt_uuid *uuid)
{BSYNC_TRACE
	int result;
	struct bsync_incoming *inc;
	rlist_foreach_entry(inc, &txn_state.incoming_connections, list) {
		if (!tt_uuid_is_equal(uuid, inc->uuid))
			continue;
		if (id >= 0)
			inc->remote_id = id;
		say_debug("found pair in incoming connections");
		fiber_call(inc->f);
		result = inc->remote_id;
		goto finish;
	}
	if (id < 0) {
		for (int i = 0; i < local_state->remote_size; ++i) {
			if (tt_uuid_is_equal(
				&local_state->remote[i].server_uuid, uuid)
				&& local_state->remote[i].switched)
			{
				say_debug("found pair in switched connections");
				result = i;
				goto finish;
			}
		}
	}
	inc = (struct bsync_incoming *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_incoming));
	inc->f = fiber();
	inc->uuid = uuid;
	inc->remote_id = id;
	rlist_add_entry(&txn_state.incoming_connections, inc, list);
	fiber_yield_timeout(BSYNC_PAIR_TIMEOUT);
	say_debug("pair was found, or timeout exceed");
	rlist_del_entry(inc, list);
	if (inc->remote_id >= 0) {
		assert(!ev_is_active(&local_state->remote[inc->remote_id].out));
		say_info("connected with %s",
			local_state->remote[inc->remote_id].source);
	}
	BSYNC_TRACE
	result = inc->remote_id;
finish:
	if (txn_state.leader_id < BSYNC_MAX_HOSTS &&
		txn_state.local_id != txn_state.leader_id)
		return id;
	return result;
}

void
bsync_push_connection(int id)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	assert(!ev_is_active(&local_state->remote[id].out));
	if (txn_state.local_id == BSYNC_MAX_HOSTS) {BSYNC_TRACE
		txn_state.wait_local[id] = true;
		fiber_yield();
	}
	BSYNC_TRACE
	if (bsync_find_incoming(id, &local_state->remote[id].server_uuid) < 0)
		tnt_raise(ClientError, ER_NO_CONNECTION);
}

void
bsync_push_localhost(int id)
{
	if (!local_state->bsync_remote)
		return;
	txn_state.local_id = id;
	struct bsync_txn_info *info = &bsync_index[id].sysmsg;
	if (txn_state.state < txn_state_subscribe)
		info->sign = -1;
	else
		info->sign = vclock_signature(&local_state->vclock);
	SWITCH_TO_BSYNC(bsync_update_local);
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (!txn_state.wait_local[i])
			continue;
		fiber_call(local_state->remote[i].connecter);
		txn_state.wait_local[i] = false;
	}
}

void
bsync_init_in(uint8_t host_id, int fd)
{
	coio_init(&local_state->remote[host_id].in);
	local_state->remote[host_id].in.fd = fd;
	local_state->remote[host_id].writer = fiber();
}

void
bsync_switch_2_election(uint8_t host_id, struct recovery_state *state)
{BSYNC_TRACE
	struct bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	if (state)
		info->sign = vclock_signature(&state->vclock);
	else
		info->sign = -1;
	txn_state.iproto[host_id] = true;
	assert(local_state->remote[host_id].in.fd >= 0);
	assert(local_state->remote[host_id].out.fd >= 0);
	assert(!ev_is_active(&local_state->remote[host_id].in));
	assert(!ev_is_active(&local_state->remote[host_id].out));
	SWITCH_TO_BSYNC(bsync_process_connect);
	fiber_yield();
	txn_state.iproto[host_id] = false;
}

bool
bsync_process_join(int fd, struct tt_uuid *uuid)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return true;
	int host_id = 0;
	if ((host_id = bsync_find_incoming(-1, uuid)) == -1)
		return false;
	local_state->remote[host_id].switched = false;
	assert(BSYNC_REMOTE.state == bsync_host_disconnected ||
		BSYNC_REMOTE.state == bsync_host_follow);
	bsync_init_in(host_id, fd);
	txn_state.join[host_id] = true;
	bsync_switch_2_election(host_id, NULL);
	say_debug("[%p] bsync_process_join %d %s", fiber(),
		  __LINE__, local_state->remote[host_id].source);
	if (local_state->remote[txn_state.leader_id].localhost) {
		say_debug("[%p] bsync_process_join %d %s", fiber(),
			__LINE__, local_state->remote[host_id].source);
		if (txn_state.state < txn_state_subscribe) {
			say_debug("[%p] bsync_process_join %d %s", fiber(),
				__LINE__, local_state->remote[host_id].source);
			box_on_cluster_join(uuid);
			fiber_yield();
			say_debug("[%p] bsync_process_join %d %s", fiber(),
				__LINE__, local_state->remote[host_id].source);
			txn_state.join[host_id] = false;
		} else {
			say_debug("[%p] bsync_process_join %d %s", fiber(),
				__LINE__, local_state->remote[host_id].source);
			txn_state.join[host_id] = false;
			box_on_cluster_join(uuid);
		}
		say_info("start sending snapshot to %s", tt_uuid_str(uuid));
		BSYNC_TRACE
		return true;
	} else
		txn_state.join[host_id] = false;
	return false;
}

bool
bsync_process_subscribe(int fd, struct tt_uuid *uuid,
			struct recovery_state *state)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return true;
	int host_id = 0;
	if ((host_id = bsync_find_incoming(-1, uuid)) == -1)
		return false;
	bool switched = local_state->remote[host_id].switched;
	local_state->remote[host_id].switched = false;
	bsync_init_in(host_id, fd);
	txn_state.id2index[state->server_id] = host_id;
	say_info("set host_id %d to server_id %d", host_id, state->server_id);
	if (txn_state.leader_id < BSYNC_MAX_HOSTS && switched &&
		local_state->remote[txn_state.leader_id].localhost)
	{
		BSYNC_TRACE
		return true;
	}
	bsync_switch_2_election(host_id, state);
	BSYNC_TRACE
	return local_state->remote[txn_state.leader_id].localhost;
}

int
bsync_join()
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return 0;
	if (txn_state.leader_id < BSYNC_MAX_HOSTS) {
		if (txn_state.leader_id == txn_state.local_id)
		return txn_state.leader_id;
	}
	txn_state.recovery = true;
	fiber_yield();
	txn_state.recovery = false;
	if (txn_state.snapshot_fiber) {
		say_info("wait for finish snapshot generating");
		fiber_join(txn_state.snapshot_fiber);
		txn_state.snapshot_fiber = NULL;
		vclock_copy(&txn_state.vclock, &local_state->vclock);
	} else
		txn_state.state = txn_state_snapshot;
	BSYNC_TRACE
	return txn_state.leader_id;
}

int
bsync_subscribe()
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return 0;
	auto state_guard = make_scoped_guard([]() {
		txn_state.state = txn_state_recovery;
	});
	if (txn_state.leader_id < BSYNC_MAX_HOSTS) {
		return txn_state.leader_id;
	}
	BSYNC_TRACE
	txn_state.recovery = true;
	fiber_yield();
	txn_state.recovery = false;
	return txn_state.leader_id;
}

int
bsync_ready_subscribe()
{BSYNC_TRACE
	if (txn_state.leader_id == BSYNC_MAX_HOSTS)
		return -1;
	txn_state.state = txn_state_recovery;
	say_info("start to recovery from %s",
		local_state->remote[txn_state.leader_id].source);
	return txn_state.leader_id;
}

static void
bsync_txn_leader(struct bsync_txn_info *info)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	txn_state.leader_id = info->connection;
	if (txn_state.state > txn_state_snapshot &&
		local_state->remote[info->connection].localhost &&
		local_state->remote[txn_state.leader_id].reader != NULL)
	{
		fiber_call(local_state->remote[txn_state.leader_id].reader);
	}
}

static void
bsync_txn_join(struct bsync_txn_info * /*info*/)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	txn_state.state = txn_state_snapshot;
	txn_state.snapshot_fiber = fiber_new("generate_initial_snapshot",
		(fiber_func) box_generate_initial_snapshot);
	fiber_set_joinable(txn_state.snapshot_fiber, true);
	fiber_call(txn_state.snapshot_fiber);
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (txn_state.join[i])
			fiber_call(local_state->remote[i].writer);
	}
	fiber_call(txn_state.snapshot_fiber);
	fiber_call(local_state->remote[txn_state.leader_id].reader);
}

static void
txn_process_recovery(struct bsync_txn_info *info)
{
	say_debug("bsync_process_recovery for %s from %s",
		  local_state->remote[info->connection].source, info->__from);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	if (local_state->remote[txn_state.leader_id].localhost) {
		uint8_t hi = info->connection;
		local_state->remote[hi].switched = true;
		say_debug("bsync_process_recovery: host_id=%d, join=%d, iproto=%d, snapshot=%p",
			hi, txn_state.join[hi] ? 1 : 0, txn_state.iproto[hi] ? 1 : 0,
			txn_state.snapshot_fiber);
		if (txn_state.join[hi] && txn_state.snapshot_fiber != NULL)
			return;
		if (txn_state.iproto[hi])
			fiber_call(local_state->remote[hi].writer);
		if (txn_state.join[hi])
			fiber_call(local_state->remote[hi].writer);
	} else {
		if (!txn_state.recovery) {
			recovery_follow_remote(local_state);
		} else {
			say_info("start recover from %s",
				local_state->remote[txn_state.leader_id].source);
			if (txn_state.recovery) {
				assert(local_state->remote[txn_state.leader_id].reader);
				fiber_call(local_state->remote[txn_state.leader_id].reader);
			}
		}
	}
}

static void
bsync_process_close(struct bsync_txn_info *info)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	evio_close(loop(), &local_state->remote[info->connection].in);
	evio_close(loop(), &local_state->remote[info->connection].out);
	local_state->remote[info->connection].switched = false;
	local_state->remote[info->connection].connected = false;
	if (local_state->remote[txn_state.leader_id].localhost)
		return;
	if (txn_state.iproto[info->connection])
		fiber_call(local_state->remote[info->connection].writer);
	if (txn_state.join[info->connection])
		fiber_call(local_state->remote[info->connection].writer);
}

static void
bsync_reconnect_all()
{
	if (txn_state.leader_id != BSYNC_MAX_HOSTS) {
		struct bsync_txn_info *info = &bsync_index[txn_state.local_id].sysmsg;
		info->sign = vclock_signature(&local_state->vclock);
		info->repeat = false;
		SWITCH_TO_BSYNC(bsync_update_local);
	}
	txn_state.leader_id = BSYNC_MAX_HOSTS;
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (local_state->remote[i].localhost)
			continue;
		if (evio_is_active(&local_state->remote[i].in))
			evio_close(loop(), &local_state->remote[i].in);
		if (evio_is_active(&local_state->remote[i].out))
			evio_close(loop(), &local_state->remote[i].out);
		local_state->remote[i].switched = false;
		fiber_call(local_state->remote[i].connecter);
	}
}

static void
txn_process_reconnnect(struct bsync_txn_info *info)
{
	if (info->proxy) {BSYNC_TRACE
		tt_pthread_mutex_lock(&bsync_state.mutex);
		STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
		bsync_reconnect_all();
	} else {BSYNC_TRACE
		bsync_process_close(info);
		fiber_call(local_state->remote[info->connection].connecter);
	}
}

bool
bsync_follow(struct recovery_state * r)
{BSYNC_TRACE
	/* try to switch to normal mode */
	if (!local_state->bsync_remote || !local_state->finalize)
		return false;
	int host_id = txn_state.id2index[r->server_id];
	assert(host_id < BSYNC_MAX_HOSTS);
	BSYNC_LOCK(txn_state.mutex[host_id]);
	bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	info->sign = vclock_signature(&r->vclock);
	say_debug("try to follow for %s", local_state->remote[host_id].source);
	say_debug("try to follow remote is %s", vclock_to_string(&r->vclock));
	say_debug("try to follow local is %s", vclock_to_string(&txn_state.vclock));
	say_debug("try to follow commited is %s", vclock_to_string(&local_state->vclock));
	SWITCH_TO_BSYNC(bsync_process_follow);
	tt_pthread_cond_wait(&txn_state.cond[host_id],
		&txn_state.mutex[host_id]);
	return info->proxy;
}

void
bsync_recovery_stop(struct recovery_state *r)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	assert(loop() == txn_loop);
	int host_id = txn_state.leader_id;
	/* switch to normal mode */
	if (r) {
		host_id = txn_state.id2index[r->server_id];
		assert(r->commit.begin == r->commit.end);
	} else {
		r = local_state;
	}
	struct bsync_txn_info *info = &bsync_index[host_id].sysmsg;
	info->sign = vclock_signature(&r->vclock);
	say_debug("bsync_recovery_stop for %s lvclock=%s, tvclock=%s",
		  local_state->remote[host_id].source,
		  vclock_to_string(&local_state->vclock),
		  vclock_to_string(&txn_state.vclock));
	if (host_id == txn_state.local_id) {
		SWITCH_TO_BSYNC(bsync_update_local);
	} else {
		SWITCH_TO_BSYNC(bsync_process_connect);
	}
	if (txn_state.state < txn_state_ready) {
		txn_state.state = txn_state_ready;
		vclock_copy(&txn_state.vclock, &local_state->vclock);
		struct fiber *f = NULL;
		while (!rlist_empty(&txn_state.wait_start)) {
			f = rlist_shift_entry(&txn_state.wait_start,
						struct fiber, state);
			fiber_call(f);
		}
	}
}

void
bsync_recovery_fail(struct recovery_state *r)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	uint8_t host_id = txn_state.id2index[r->server_id];
	txn_state.id2index[r->server_id] = BSYNC_MAX_HOSTS;

	if (evio_is_active(&local_state->remote[host_id].in))
		evio_close(loop(), &local_state->remote[host_id].in);
	if (evio_is_active(&local_state->remote[host_id].out))
		evio_close(loop(), &local_state->remote[host_id].out);
	struct bsync_txn_info *info = &bsync_index[host_id].sysmsg;
	info->proxy = false;
	SWITCH_TO_BSYNC(bsync_process_disconnect);
	fiber_call(local_state->remote[host_id].connecter);
}

void
bsync_replica_fail()
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	struct bsync_txn_info *info = &bsync_index[txn_state.leader_id].sysmsg;
	SWITCH_TO_BSYNC(bsync_process_disconnect);
	bsync_reconnect_all();
	txn_state.leader_id = BSYNC_MAX_HOSTS;
}

static void bsync_outgoing(va_list ap);
static void bsync_accept_handler(va_list ap);

static void
bsync_fill_lsn(struct xrow_header *row)
{
	if (row->server_id == 0) {
		row->server_id = local_state->server_id;
		row->lsn = vclock_inc(&txn_state.vclock, row->server_id);
	} else {
		vclock_follow(&txn_state.vclock, row->server_id, row->lsn);
	}
}

static struct bsync_txn_info *
bsync_alloc_txn_info(struct xrow_header *row, bool proxy)
{
	struct bsync_txn_info *info = (struct bsync_txn_info *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_txn_info));
	info->common = (struct bsync_common *) region_alloc(
		&fiber()->gc, sizeof(struct bsync_common));
	if (proxy)
		info->common->region = NULL;
	else
		info->common->region = bsync_new_region();
	info->row = row;
	info->op = NULL;
	info->repeat = false;
	info->result = 0;
	info->proxy = proxy;
	info->process = bsync_process;
	return info;
}

static int
bsync_write_local(struct recovery_state *r, struct txn_stmt *stmt)
{BSYNC_TRACE
	wal_fill_lsn(local_state, stmt->row);
	vclock_follow(&txn_state.vclock, stmt->row->server_id,
			stmt->row->lsn);
	stmt->row->commit_sn = vclock_signature(&txn_state.vclock);
	say_debug("bsync_write_local receive %d:%ld", stmt->row->server_id,
		stmt->row->lsn);
	return wal_write(r, stmt->row);
}

static int
bsync_write_remote(struct txn_stmt *stmt)
{BSYNC_TRACE
	struct bsync_txn_info *info = bsync_alloc_txn_info(stmt->row, true);
	if (stmt->row->server_id == 0) {
		assert(local_state->server_id > 0);
		if (txn_state.state == txn_state_recovery)
			return -1;
		wal_fill_lsn(local_state, stmt->row);
	} else {
		assert(local_state->server_id != 0 ||
			txn_state.leader_id != txn_state.local_id);
		vclock_follow(&local_state->vclock, stmt->row->server_id,
				stmt->row->lsn);
	}
	say_debug("bsync_write_remote receive %d:%ld", stmt->row->server_id,
		stmt->row->lsn);
	if (vclock_get(&txn_state.vclock, stmt->row->server_id) < stmt->row->lsn)
		vclock_follow(&txn_state.vclock, stmt->row->server_id,
				stmt->row->lsn);
	info->sign = vclock_signature(&local_state->vclock);
	info->owner = fiber();
	SWITCH_TO_BSYNC(bsync_process_wal);
	fiber_yield();
	assert(info->result >= 0);
	say_debug("result of recovery operation is %d", info->result);
	return info->result;
}

void
bsync_commit_local(uint32_t server_id, uint64_t lsn)
{
	if (!local_state->bsync_remote)
		return;
	if (vclock_get(&local_state->vclock, server_id) == lsn)
		return;
	vclock_follow(&local_state->vclock, server_id, lsn);
	if (vclock_get(&txn_state.vclock, server_id) < lsn)
		vclock_follow(&txn_state.vclock, server_id, lsn);
	struct bsync_txn_info *info = bsync_alloc_txn_info(NULL, false);
	info->sign = vclock_signature(&local_state->vclock);
	info->owner = fiber();
	SWITCH_TO_BSYNC(bsync_process_wal);
	fiber_yield();
	assert(info->result >= 0);
}

int
bsync_write(struct recovery_state *r, struct txn_stmt *stmt) try {BSYNC_TRACE
	if (!local_state->bsync_remote)
		return wal_write(r, stmt->row);
	assert(local_state == NULL || local_state == r);
	if (txn_in_local_recovery())
		return bsync_write_local(r, stmt);
	if (txn_in_remote_recovery())
		return bsync_write_remote(stmt);
	if (txn_state.state < txn_state_ready) {
		BSYNC_TRACE
		say_info("add operation to wait queue, state: %d", txn_state.state);
		rlist_add_tail_entry(&txn_state.wait_start, fiber(), state);
		fiber_yield();
		rlist_del_entry(fiber(), state);
	}

	if (txn_state.state == txn_state_rollback) {
		if (stmt->row->server_id == 0)
			return -1;
		rlist_shift(&bsync_state.txn_queue);
		fiber_yield();
		return 0;
	}
	if (!stmt->old_tuple && !stmt->new_tuple)
		return 0;
	assert(stmt->row->server_id > 0 || (stmt->row->commit_sn == 0 &&
		stmt->row->rollback_sn == 0));
	bsync_dump_region();

	stmt->row->tm = ev_now(loop());
	stmt->row->sync = 0;
	struct bsync_txn_info *info = NULL;
	bsync_fill_lsn(stmt->row);
	say_debug("receive row %d:%ld", stmt->row->server_id, stmt->row->lsn);
	if (stmt->row->server_id == local_state->server_id) {
		info = bsync_alloc_txn_info(stmt->row, false);
		if (stmt->new_tuple) {
			bsync_parse_dup_key(info->common,
				stmt->space->index[0]->key_def,
				stmt->new_tuple);
		} else {
			bsync_parse_dup_key(info->common,
				stmt->space->index[0]->key_def,
				stmt->old_tuple);
		}
		bool begin_result = bsync_begin_op(info->common->dup_key,
						   stmt->row->server_id);
		(void) begin_result;
		assert(begin_result);
	} else { /* proxy request */
		info = rlist_shift_entry(&bsync_state.txn_queue,
			struct bsync_txn_info, list);
		say_debug("send request %d:%ld to bsync",
			info->op->row->server_id, info->op->row->lsn);
	}
	info->owner = fiber();
	auto vclock_guard = make_scoped_guard([&](){
		if (info->result < 0) {
			say_debug("row %d:%ld rejected from %s:%d",
				stmt->row->server_id, stmt->row->lsn,
				info->__from, info->__line);
			return;
		}
		say_debug("commit request %d:%ld sign=%ld",
			  stmt->row->server_id, stmt->row->lsn, info->sign);
		if (txn_state.state != txn_state_recovery)
			vclock_follow(&local_state->vclock, stmt->row->server_id,
					stmt->row->lsn);
	});
	info->sign = vclock_signature(&txn_state.vclock);
	rlist_add_tail_entry(&bsync_state.execute_queue, info, list);
	SWITCH_TO_BSYNC(bsync_process);
	fiber_yield();
	BSYNC_TRACE
	if (txn_state.state != txn_state_rollback) {
		rlist_del_entry(info, list);
		if (info->result >= 0) {
			info->repeat = false;
			return info->result;
		}
		tt_pthread_mutex_lock(&bsync_state.mutex);
		txn_state.state = txn_state_rollback;
		/* rollback in reverse order local operations */
		say_info("conflict detected, start to rollback");
		struct bsync_txn_info *s;
		STAILQ_FOREACH(s, &bsync_state.bsync_proxy_input, fifo) {
			say_debug("drop from proxy_input %p (%d:%ld)", fiber(),
				  s->row->server_id, s->row->lsn);
		}
		STAILQ_FOREACH(s, &bsync_state.bsync_proxy_queue, fifo) {
			say_debug("drop from proxy_queue %p (%d:%ld)", fiber(),
				  s->row->server_id, s->row->lsn);
		}
		rlist_foreach_entry_reverse(s, &bsync_state.execute_queue, list) {
			s->result = -1;
			say_debug("reject row %d:%ld", s->row->server_id, s->row->lsn);
			if (s->op->status == bsync_op_status_submit) {
				s->repeat = true;
				rlist_add_entry(&bsync_state.submit_queue, s->op, list);
			}
			fiber_call(s->owner);
		}
		rlist_create(&bsync_state.execute_queue);
		STAILQ_INIT(&bsync_state.bsync_proxy_input);
		STAILQ_INIT(&bsync_state.bsync_proxy_queue);
		say_info("start to reapply commited ops");
		bsync_commit_foreach([](struct bsync_operation *oper) {
			if (oper->txn_data->owner)
				fiber_call(oper->txn_data->owner);
			return false;
		});
		say_info("finish rollback execution");
		txn_state.state = txn_state_ready;
		tt_pthread_cond_signal(&bsync_state.cond);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
	}
	info->owner = NULL;
	return info->result;
}
catch (...) {
	say_crit("bsync_write found unhandled exception");
	throw;
}

static void
bsync_send_data(struct bsync_host_data *host, struct bsync_send_elem *elem)
{BSYNC_TRACE
	/* TODO : check all send ops for memory leak case */
	if (host->state == bsync_host_disconnected)
		return;
	assert(elem->code < bsync_mtype_count);
	say_debug("add message %s to send queue %s",
		bsync_mtype_name[elem->code], host->name);
	rlist_add_tail_entry(&host->send_queue, elem, list);
	++host->send_queue_size;
	if ((host->flags & bsync_host_active_write) == 0 &&
		host->fiber_out != fiber())
	{
		fiber_call(host->fiber_out);
	}
}

static struct bsync_send_elem *
bsync_alloc_send(uint8_t type)
{
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		mempool_alloc0(&bsync_state.system_send_pool);
	elem->code = type;
	elem->system = true;
	return elem;
}

static int
bsync_wal_write(struct xrow_header *row)
{BSYNC_TRACE
	if (bsync_state.state == bsync_state_shutdown)
		return -1;
	if (bsync_state.wal_commit_sign) {
		row->commit_sn = bsync_state.wal_commit_sign;
		BSYNC_LOCAL.commit_sign = bsync_state.wal_commit_sign;
		bsync_state.wal_commit_sign = 0;
		say_debug("commit %ld sign", row->commit_sn);
	}
	if (bsync_state.wal_rollback_sign) {
		row->rollback_sn = bsync_state.wal_rollback_sign;
		bsync_state.wal_rollback_sign = 0;
		say_debug("rollback %ld sign", row->rollback_sn);
	}
	return wal_write(local_state, row);
}

static void
bsync_rollback_slave(struct bsync_operation *oper, bool retry_commit)
{BSYNC_TRACE
	struct bsync_operation *op;
	oper->txn_data->process = txn_process;
	tt_pthread_mutex_lock(&bsync_state.mutex);
	bsync_commit_foreach([retry_commit](struct bsync_operation *oper) {
		if (retry_commit)
			oper->txn_data->repeat = true;
		else
			oper->status = bsync_op_status_fail;
		return false;
	});
	if (!retry_commit) {
		rlist_create(&bsync_state.submit_queue);
		rlist_create(&bsync_state.commit_queue);
	}
	rlist_foreach_entry(op, &bsync_state.proxy_queue, list) {
		op->txn_data->result = -1;
		op->status = bsync_op_status_fail;
		if (op->owner != fiber())
			fiber_call(op->owner);
	}
	rlist_foreach_entry(op, &bsync_state.wal_queue, list) {
		op->txn_data->result = -1;
		op->status = bsync_op_status_fail;
	}
	rlist_create(&bsync_state.wal_queue);
	rlist_create(&bsync_state.proxy_queue);
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue);
	assert(oper->txn_data->owner);
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, oper->txn_data, fifo);
	if (was_empty)
		ev_async_send(txn_loop, &txn_process_event);
	bsync_state.bsync_rollback = true;
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	bsync_state.bsync_rollback = false;
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	bsync_send_data(&BSYNC_LEADER, bsync_alloc_send(bsync_mtype_proxy_join));
}

static void
bsync_slave_wal(struct bsync_operation *oper, struct bsync_send_elem *elem)
{BSYNC_TRACE
	say_debug("[%p] start to apply request  %d:%ld(%ld) to WAL", fiber(),
		oper->row->server_id, oper->row->lsn, oper->sign);
	oper->status = bsync_op_status_wal;
	rlist_add_tail_entry(&bsync_state.wal_queue, oper, list);
	BSYNC_LOCAL.sign = oper->sign;
	int wal_res = bsync_wal_write(oper->txn_data->row);
	rlist_shift_entry(&bsync_state.wal_queue, struct bsync_operation, list);
	if (oper->status == bsync_op_status_fail)
		return;
	if (wal_res >= 0) {
		if (!rlist_empty(&bsync_state.submit_queue)) {
			assert(oper->sign > rlist_last_entry(
				&bsync_state.submit_queue,
				struct bsync_operation, list)->sign);
		}
		rlist_add_tail_entry(&bsync_state.submit_queue, oper, list);
		say_debug("submit request %d:%ld(%ld)",
			  oper->row->server_id, oper->row->lsn, oper->sign);
	} else {
		say_debug("reject request %d:%ld(%ld)",
			  oper->row->server_id, oper->row->lsn, oper->sign);
	}
	oper->txn_data->result = wal_res;
	elem->code = (oper->txn_data->result < 0 ?
			bsync_mtype_reject : bsync_mtype_submit);
	bsync_send_data(&BSYNC_LEADER, elem);
	if (oper->txn_data->result >= 0)
		return;
	say_debug("rollback request %d:%ld(%ld)", oper->row->server_id,
		  oper->row->lsn, oper->sign);
	bsync_rollback_slave(oper, false);
	if (bsync_state.leader_id > BSYNC_MAX_HOSTS)
		bsync_disconnected(bsync_state.leader_id, false);
}

static void
bsync_queue_slave(struct bsync_operation *oper)
{BSYNC_TRACE
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_send_elem));
	elem->code = bsync_mtype_proxy_request;
	elem->arg = oper;
	oper->txn_data->result = 0;
	say_debug("start to proceed request %d:%ld(%ld)", oper->row->server_id,
		oper->row->lsn, oper->sign);
	rlist_add_tail_entry(&bsync_state.proxy_queue, oper, list);
	bsync_send_data(&BSYNC_LEADER, elem);
	/* wait accept or reject */
	fiber_yield();
	BSYNC_TRACE
	if (oper->txn_data->result < 0) {
		say_debug("request %d:%ld rejected", oper->row->server_id,
			oper->row->lsn);
		bsync_rollback_slave(oper, true);
		return;
	}
	say_debug("request %d:%ld accepted to %ld",
		  oper->row->server_id, oper->row->lsn, oper->sign);
	oper->txn_data->sign = oper->sign;
	bsync_slave_wal(oper, elem);
	say_debug("request %d:%ld pushed to WAL, status is %d",
		  oper->row->server_id, oper->row->lsn, oper->status);
	if (oper->status == bsync_op_status_wal) {
		oper->status = bsync_op_status_yield;
		fiber_yield();
		BSYNC_TRACE
	}
	if (oper->status == bsync_op_status_fail) {
		oper->txn_data->result = -1;
	} else {
		assert (oper->status == bsync_op_status_submit);
		oper->txn_data->result = 1;
	}
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	bsync_free_region(oper->common);
}

static void
bsync_proxy_slave(struct bsync_operation *oper, struct bsync_send_elem *send)
{BSYNC_TRACE
	bsync_slave_wal(oper, send);
	if (oper->status == bsync_op_status_fail || oper->txn_data->result < 0)
		return;
	say_debug("start to apply request %d:%ld(%ld) to TXN",
		oper->row->server_id, oper->row->lsn, oper->sign);
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	fiber_yield();
	say_debug("finish apply request %d:%ld(%ld) to TXN, status is %d",
		oper->row->server_id, oper->row->lsn, oper->sign, oper->status);
	if (oper->status == bsync_op_status_fail)
		return;
	if (oper->status == bsync_op_status_wal) {
		oper->status = bsync_op_status_yield;
		fiber_yield();
		BSYNC_TRACE
	}
	say_debug("%d:%ld(%ld), status=%d", oper->row->server_id,
		  oper->row->lsn, oper->sign, oper->status);
	if (oper->status == bsync_op_status_fail) {
		oper->txn_data->result = -1;
	} else {
		assert (oper->status == bsync_op_status_submit);
		oper->txn_data->result = 1;
	}
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	fiber_yield();
	bsync_free_region(oper->common);
}

static void
bsync_wait_slow(struct bsync_operation *oper)
{BSYNC_TRACE
	if (2 * oper->accepted > bsync_state.num_hosts) {
		bsync_state.wal_commit_sign = oper->sign;
	}
	while ((oper->accepted + oper->rejected) < bsync_state.num_hosts) {
		oper->status = bsync_op_status_yield;
		fiber_yield();
		BSYNC_TRACE
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.commit_sign == BSYNC_REMOTE.submit_sign ||
			bsync_state.local_id == host_id ||
			(BSYNC_REMOTE.flags & bsync_host_active_write) != 0)
		{
			continue;
		}
		if (BSYNC_REMOTE.fiber_out)
			fiber_call(BSYNC_REMOTE.fiber_out);
	}
	say_debug("finish request %d:%ld, acc/rej: %d/%d", oper->row->server_id,
		oper->row->lsn, oper->accepted, oper->rejected);
}

static void
bsync_leader_cleanup_followed(uint8_t host_id, struct bsync_operation *oper)
{
	struct rlist queue;
	size_t queue_size = 0;
	rlist_create(&queue);
	struct bsync_send_elem *elem = NULL;
	if (oper) {
		while(!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
			elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
						 struct bsync_send_elem, list);
			if (oper->sign >= ((struct bsync_operation *)elem->arg)->sign)
				break;
			rlist_add_tail_entry(&queue, elem, list);
			++queue_size;
		}
	}
	while(!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
					 struct bsync_send_elem, list);
		bsync_do_reject(host_id, elem);
	}
	rlist_swap(&BSYNC_REMOTE.follow_queue, &queue);
	BSYNC_REMOTE.follow_queue_size = queue_size;
}

static void
bsync_check_follow(uint8_t host_id)
{
	struct bsync_send_elem *elem = NULL;
	if (BSYNC_REMOTE.follow_queue_size < bsync_state.max_host_queue)
		return;
	ssize_t remove_count = BSYNC_REMOTE.follow_queue_size -
					bsync_state.max_host_queue;
	if (remove_count > 0 && (BSYNC_REMOTE.flags & bsync_host_follow_fast) != 0)
		remove_count = BSYNC_REMOTE.follow_queue_size;
	for (ssize_t i = 0; i < remove_count; ++i) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
					struct bsync_send_elem, list);
		--BSYNC_REMOTE.follow_queue_size;
		bsync_do_reject(host_id, elem);
	}
	if (BSYNC_REMOTE.flags & bsync_host_follow_fast) {
		--bsync_state.num_connected;
		BSYNC_REMOTE.flags &= ~bsync_host_follow_fast;
		BSYNC_REMOTE.state = bsync_host_disconnected;
		bsync_leader_cleanup_followed(host_id, NULL);
	}
}

static void
bsync_check_slow(uint8_t host_id)
{BSYNC_TRACE
	if (BSYNC_REMOTE.state == bsync_host_follow) {
		bsync_check_follow(host_id);
		return;
	}
	struct bsync_send_elem *elem = NULL;
	ssize_t queue_size =
		BSYNC_REMOTE.send_queue_size + BSYNC_REMOTE.op_queue_size;
	if (queue_size <= bsync_state.max_host_queue ||
		bsync_state.max_host_queue <= 0)
	{
		return;
	}
	rlist_foreach_entry(elem, &BSYNC_REMOTE.op_queue, list) {
		bsync_do_reject(host_id, elem);
	}
	rlist_foreach_entry(elem, &BSYNC_REMOTE.send_queue, list) {
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
	}
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	if (BSYNC_REMOTE.state == bsync_host_connected) {
		say_info("host %s detected as slow, disconnecting", BSYNC_REMOTE.name);
		bsync_disconnected(host_id, false);
	}
}

static void
bsync_leader_cleanup_connected(uint8_t host_id, struct bsync_operation *oper)
{
	struct rlist queue;
	size_t queue_size = 0;
	rlist_create(&queue);
	struct bsync_send_elem *elem = NULL;
	while(!rlist_empty(&BSYNC_REMOTE.op_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
					 struct bsync_send_elem, list);
		if (oper->sign >= ((struct bsync_operation *)elem->arg)->sign)
			break;
		rlist_add_tail_entry(&queue, elem, list);
		++queue_size;
	}
	while(!rlist_empty(&BSYNC_REMOTE.op_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
					 struct bsync_send_elem, list);
		bsync_do_reject(host_id, elem);
	}
	rlist_swap(&BSYNC_REMOTE.op_queue, &queue);
	BSYNC_REMOTE.op_queue_size = queue_size;
	while(!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					 struct bsync_send_elem, list);
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
		if (elem->system)
			mempool_free(&bsync_state.system_send_pool, elem);
	}
	BSYNC_REMOTE.send_queue_size = 0;
	elem = bsync_alloc_send(bsync_mtype_rollback);
	elem->arg = (void*)oper->sign;
	bsync_send_data(&BSYNC_REMOTE, elem);
}

static void
bsync_leader_rollback(struct bsync_operation *oper)
{
	say_info("start leader rollback from %d:%ld (%ld)",
		 oper->row->server_id, oper->row->lsn, oper->sign);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	bsync_commit_foreach([oper](struct bsync_operation *op) {
		if (op->sign >= oper->sign)
			op->status = bsync_op_status_fail;
		return false;
	});
	rlist_create(&bsync_state.submit_queue);
	rlist_create(&bsync_state.commit_queue);
	struct bsync_operation *op;
	rlist_foreach_entry(op, &bsync_state.wal_queue, list) {
		if (op->sign < oper->sign)
			continue;
		op->txn_data->result = -1;
		op->status = bsync_op_status_fail;
	}
	rlist_create(&bsync_state.wal_queue);
	bsync_state.wal_rollback_sign = oper->sign;
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id ||
			BSYNC_REMOTE.state < bsync_host_follow)
			continue;
		if (BSYNC_REMOTE.state == bsync_host_connected)
			bsync_leader_cleanup_connected(host_id, oper);
		else
			bsync_leader_cleanup_followed(host_id, oper);
	}
	oper->txn_data->process = txn_process;
	uint8_t state = bsync_state.state;
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue);
	assert(oper->txn_data->owner);
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, oper->txn_data, fifo);
	if (was_empty)
		ev_async_send(txn_loop, &txn_process_event);
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	bsync_state.state = state;
	tt_pthread_mutex_unlock(&bsync_state.mutex);
}

#define BSYNC_SEND_2_BSYNC {\
	bool was_empty = STAILQ_EMPTY(&bsync_state.bsync_proxy_input); \
	STAILQ_INSERT_TAIL(&bsync_state.bsync_proxy_input, oper->txn_data, fifo); \
	if (was_empty) \
		ev_async_send(loop(), &bsync_process_event); \
}

static void
bsync_queue_leader(struct bsync_operation *oper, bool proxy)
{BSYNC_TRACE
	if (!proxy) {
		oper->row = (struct xrow_header *)
			region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
		xrow_copy(oper->txn_data->row, oper->row);
	}
	BSYNC_LOCAL.sign = oper->sign;
	oper->status = bsync_op_status_init;
	say_debug("start to proceed request %d:%ld, sign is %ld",
		oper->row->server_id, oper->row->lsn, oper->sign);
	oper->rejected = oper->accepted = 0;
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id ||
			BSYNC_REMOTE.state < bsync_host_follow)
		{
			if (host_id != bsync_state.local_id) {
				say_debug("reject operation %d:%ld for host %d",
					oper->row->server_id, oper->row->lsn, host_id);
				++oper->rejected;
				bsync_end_op(host_id, oper->common->dup_key, oper->host_id);
			}
			continue;
		}
		struct bsync_send_elem *elem = (struct bsync_send_elem *)
			region_alloc0(&fiber()->gc, sizeof(struct bsync_send_elem));
		elem->arg = oper;
		if (oper->host_id == host_id)
			elem->code = bsync_mtype_proxy_accept;
		else
			elem->code = bsync_mtype_body;
		if (BSYNC_REMOTE.state == bsync_host_connected) {
			bsync_send_data(&BSYNC_REMOTE, elem);
		} else {
			rlist_add_tail_entry(&BSYNC_REMOTE.follow_queue,
					     elem, list);
			++BSYNC_REMOTE.follow_queue_size;
		}
		bsync_check_slow(host_id);
	}
	oper->status = bsync_op_status_wal;
	rlist_add_tail_entry(&bsync_state.submit_queue, oper, list);
	oper->txn_data->result = bsync_wal_write(oper->txn_data->row);
	if (oper->txn_data->result < 0) {
		say_debug("reject operation %d:%ld for host %d",
			oper->row->server_id, oper->row->lsn, bsync_state.local_id);
		++oper->rejected;
	} else
		++oper->accepted;
	ev_tstamp start = ev_now(loop());
	oper->status = bsync_op_status_yield;
	while (2 * oper->accepted <= bsync_state.num_hosts) {
		if (ev_now(loop()) - start > bsync_state.operation_timeout ||
			2 * oper->rejected > bsync_state.num_hosts)
		{
			break;
		}
		fiber_yield_timeout(bsync_state.operation_timeout);
	}
	oper->txn_data->result = (
		2 * oper->accepted > bsync_state.num_hosts ? 0 : -1);
	rlist_del_entry(oper, list);
	say_debug("%d:%ld(%ld) result: num_accepted=%d, num_rejected=%d",
		  oper->row->server_id, oper->row->lsn, oper->sign,
		  oper->accepted, oper->rejected);
	if (oper->txn_data->result < 0) {
		if (oper->status != bsync_op_status_fail)
			bsync_leader_rollback(oper);
	} else {
		SWITCH_TO_TXN(oper->txn_data, txn_process);
		if (proxy) {
			oper->status = bsync_op_status_wal;
			fiber_yield();
		}
	}
	bsync_wait_slow(oper);
	bsync_free_region(oper->common);
	say_debug("finish request %d:%ld", oper->row->server_id,
		  oper->row->lsn);
}


static void
bsync_proxy_leader(struct bsync_operation *oper, struct bsync_send_elem *send)
{
	say_debug("start to apply request %d:%ld to TXN",
		  oper->row->server_id, oper->row->lsn);
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	fiber_yield();
	BSYNC_TRACE
	oper->sign = oper->txn_data->sign;
	if (oper->txn_data->result >= 0) {
		bsync_queue_leader(oper, true);
		return;
	}
	uint8_t host_id = oper->host_id;
	if (BSYNC_REMOTE.flags & bsync_host_rollback)
		return;
	BSYNC_REMOTE.flags |= bsync_host_rollback;
	/* drop all active operations from host */
	tt_pthread_mutex_lock(&bsync_state.mutex);
	struct bsync_txn_info *info;
	STAILQ_FOREACH(info, &bsync_state.txn_proxy_input, fifo) {
		if (info->op->host_id == oper->host_id)
			info->result = -1;
	}
	STAILQ_FOREACH(info, &bsync_state.txn_proxy_queue, fifo) {
		if (info->row->server_id == oper->host_id)
			info->result = -1;
	}
	tt_pthread_cond_signal(&bsync_state.cond);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	send->code = bsync_mtype_proxy_reject;
	bsync_send_data(&BSYNC_REMOTE, send);
}

static void
bsync_proxy_processor()
{BSYNC_TRACE
	struct bsync_operation *oper = (struct bsync_operation *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_operation));
	struct bsync_send_elem *send = (struct bsync_send_elem *)
		region_alloc0(&fiber()->gc, sizeof(bsync_send_elem));
	send->arg = oper;
	oper->txn_data = bsync_alloc_txn_info((struct xrow_header *)
		region_alloc0(&fiber()->gc, sizeof(struct xrow_header)), true);
	oper->common = oper->txn_data->common;
	oper->txn_data->op = oper;
	oper->txn_data->owner = NULL;
	xrow_header_decode(oper->txn_data->row, bsync_state.iproxy_pos,
			   bsync_state.iproxy_end);
	oper->txn_data->row->commit_sn = oper->txn_data->row->rollback_sn = 0;
	struct iovec xrow_body[XROW_BODY_IOVMAX];
	memcpy(xrow_body, oper->txn_data->row->body,
		sizeof(oper->txn_data->row->body));
	for (int i = 0; i < oper->txn_data->row->bodycnt; ++i) {
		oper->txn_data->row->body[i].iov_base = region_alloc(
			&fiber()->gc, xrow_body[i].iov_len);
		memcpy(oper->txn_data->row->body[i].iov_base,
			xrow_body[i].iov_base, xrow_body[i].iov_len);
	}
	oper->row = oper->txn_data->row;
	say_debug("try to proxy %d:%ld", oper->row->server_id, oper->row->lsn);
	oper->host_id = bsync_state.iproxy_host;
	if (bsync_state.iproxy_sign)
		oper->sign = bsync_state.iproxy_sign;
	bsync_state.iproxy_sign = 0;
	bsync_state.iproxy_host = BSYNC_MAX_HOSTS;
	bsync_state.iproxy_pos = NULL;
	bsync_state.iproxy_end = NULL;
	oper->owner = fiber();

	BSYNC_SEND_2_BSYNC
	fiber_yield();

	if (bsync_state.leader_id != bsync_state.local_id)
		bsync_proxy_slave(oper, send);
	else
		bsync_proxy_leader(oper, send);
}

static void
txn_rollback_leader(struct bsync_txn_info *info)
{BSYNC_TRACE
	info->result = -1;
	SWITCH_TO_BSYNC(bsync_process);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
}

/*
 * Command handlers block
 */

static void
bsync_body(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	bsync_state.iproxy_sign = mp_decode_uint(ipos);
	if (bsync_state.state == bsync_state_shutdown) {
		*ipos = iend;
		return;
	}
	assert(host_id == bsync_state.leader_id);
	bsync_state.iproxy_host = host_id;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
	assert(*ipos == iend);
}

static void
bsync_do_submit(uint8_t host_id, struct bsync_send_elem *info)
{BSYNC_TRACE
	struct bsync_operation *oper = (struct bsync_operation *)info->arg;
	++oper->accepted;
	bsync_end_op(host_id, oper->common->dup_key, oper->host_id);
	BSYNC_REMOTE.submit_sign = oper->sign;
	if (oper->status == bsync_op_status_yield)
		fiber_call(oper->owner);
}

static void
bsync_submit(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	*ipos = iend;
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	--BSYNC_REMOTE.op_queue_size;
	bsync_do_submit(host_id,
		rlist_shift_entry(&BSYNC_REMOTE.op_queue,
			struct bsync_send_elem, list));
	BSYNC_TRACE
}

static void
bsync_do_reject(uint8_t host_id, struct bsync_send_elem *info)
{BSYNC_TRACE
	struct bsync_operation *oper = (struct bsync_operation *)info->arg;
	say_debug("reject operation %d:%ld for host %d",
		oper->row->server_id, oper->row->lsn, host_id);
	++oper->rejected;
	bsync_end_op(host_id, oper->common->dup_key, oper->host_id);
	if (oper->status == bsync_op_status_yield && oper->owner != fiber())
		fiber_call(oper->owner);
}

static void
bsync_reject(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	*ipos = iend;
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	--BSYNC_REMOTE.op_queue_size;
	bsync_do_reject(host_id,
		rlist_shift_entry(&BSYNC_REMOTE.op_queue,
			struct bsync_send_elem, list));
}

static void
bsync_proxy_request(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	if ((BSYNC_REMOTE.flags & bsync_host_rollback) != 0 ||
		bsync_state.state != bsync_state_ready)
	{
		/*
		 * skip all proxy requests from node in conflict state or
		 * if we lost consensus
		 */
		*ipos = iend;
		return;
	}
	bsync_state.iproxy_host = host_id;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
	assert(*ipos == iend);
}

static void
bsync_proxy_accept(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	assert(!rlist_empty(&bsync_state.proxy_queue));
	assert(host_id == bsync_state.leader_id);
	struct bsync_operation *oper = rlist_shift_entry(&bsync_state.proxy_queue,
							 bsync_operation, list);
	oper->sign = mp_decode_uint(ipos);
	say_debug("accept operation %d:%ld(%ld)", oper->row->server_id,
		  oper->row->lsn, oper->sign);
	assert(*ipos == iend);

	BSYNC_SEND_2_BSYNC
}

static void
bsync_do_proxy_reject(struct bsync_operation *op)
{
	op->txn_data->result = -1;
	fiber_call(op->owner);
}

static void
bsync_proxy_reject(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) host_id; (void) ipos; (void) iend;
	assert(!rlist_empty(&bsync_state.proxy_queue));
	struct bsync_operation *op =
		rlist_shift_entry(&bsync_state.proxy_queue,
				  bsync_operation, list);
	assert(*ipos == iend);
	bsync_do_proxy_reject(op);
}

static void
bsync_proxy_join(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	*ipos = iend;
	BSYNC_REMOTE.flags &= ~bsync_host_rollback;
}

static void
bsync_commit(uint64_t commit_sign)
{BSYNC_TRACE
	struct bsync_operation *op = NULL;
	uint64_t op_sign = 0;
	do {
		assert(!rlist_empty(&bsync_state.submit_queue));
		op = rlist_shift_entry(&bsync_state.submit_queue,
					bsync_operation, list);
		op_sign = op->sign;
		say_debug("commit operation %d:%ld, %ld < %ld, status is %d",
			op->row->server_id, op->row->lsn, op_sign, commit_sign,
			op->status);
		bool yield = (op->status == bsync_op_status_yield);
		if (op->status != bsync_op_status_fail)
			op->status = bsync_op_status_submit;
		if (yield)
			fiber_call(op->owner);
	} while (op_sign < commit_sign);
	bsync_state.wal_commit_sign = commit_sign;
}

static void
bsync_rollback(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) host_id; (void) iend;
	assert(bsync_state.local_id != bsync_state.leader_id);
	uint64_t sign = mp_decode_uint(ipos);
	struct bsync_operation *oper = NULL;
	rlist_foreach_entry(oper, &bsync_state.submit_queue, list) {
		if (oper->sign != sign)
			continue;
		break;
	}
	assert(oper != NULL);
	bsync_state.wal_rollback_sign = sign;
	bsync_rollback_slave(oper, false);
}

/*
 * Fibers block
 */

static void
bsync_election_ops();

static void
txn_parse_request(struct bsync_txn_info *info, struct request *req)
{BSYNC_TRACE
	request_create(req, info->row->type);
	request_decode(req, (const char*) info->row->body[0].iov_base,
		info->row->body[0].iov_len);
	req->header = info->row;
	info->op->common->region = bsync_new_region();
}

static void
txn_queue_leader(struct bsync_txn_info *info)
{BSYNC_TRACE
	struct request req;
	txn_parse_request(info, &req);
	struct bsync_parse_data data;
	request_header_decode(info->row, bsync_space_cb, bsync_tuple_cb, &data);
	struct tuple *tuple = NULL;
	struct space *space = space_cache_find(data.space_id);
	assert(space && space->index_count > 0 &&
		space->index[0]->key_def->iid == 0);
	if (data.is_tuple) {
		tuple = tuple_new(space->format, data.data, data.end);
		space_validate_tuple(space, tuple);
	} else {
		const char *key = req.key;
		uint32_t part_count = mp_decode_array(&key);
		tuple = space->index[0]->findByKey(key, part_count);
	}
	if (!tuple) {
		txn_rollback_leader(info);
		return;
	}
	bsync_parse_dup_key(info->common, space->index[0]->key_def, tuple);
	TupleGuard guard(tuple);
	if (!bsync_begin_op(info->common->dup_key, info->op->host_id)) {
		txn_rollback_leader(info);
		return;
	}
	try {
		rlist_add_tail_entry(&bsync_state.txn_queue, info, list);
		box_process(&req, &null_port);
	} catch (LoggedError *e) {
		if (e->errcode() != ER_WAL_IO) {
			panic("Leader cant proceed verified operation");
		}
	}
}

static void
txn_process_slave(struct bsync_txn_info *info)
{BSYNC_TRACE
	struct request req;
	txn_parse_request(info, &req);
	rlist_add_tail_entry(&bsync_state.txn_queue, info, list);
	assert(!box_is_ro());
	box_process(&req, &null_port);
	if (!rlist_empty(&info->list) && !info->repeat) {
		abort(); // TODO
		tnt_raise(LoggedError, ER_BSYNC_SLAVE_INVALID,
			  "request wasnt logged to WAL");
	}
	BSYNC_TRACE
}

static void
txn_queue_slave(struct bsync_txn_info *info)
{BSYNC_TRACE
	if (info->result < 0) {
		SWITCH_TO_BSYNC(bsync_process);
		return;
	}
	try {
		txn_process_slave(info);
	} catch (Exception *e) {
		if (!info->repeat) {
			panic("Slave cant proceed submited operation");
		}
		if (txn_state.state != txn_state_rollback)
			rlist_del_entry(info, list);
		say_debug("cant proceed operation %d:%ld, reason: %s",
			info->row->server_id, info->row->lsn, e->errmsg());
		info->owner = fiber();
		fiber_yield();
		txn_process_slave(info);
	}
	BSYNC_TRACE
}

static void
txn_process_fiber(va_list /* ap */)
{
restart:
	tt_pthread_mutex_lock(&bsync_state.mutex);
	struct bsync_txn_info *info = STAILQ_FIRST(
		&bsync_state.txn_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);

	if (txn_state.leader_id == txn_state.local_id)
		txn_queue_leader(info);
	else
		txn_queue_slave(info);
	SWITCH_TO_BSYNC(bsync_process);

	assert(bsync_state.txn_fibers.active > 0);
	--bsync_state.txn_fibers.active;
	rlist_add_tail_entry(&bsync_state.txn_fibers.data, fiber(), state);
	fiber_yield();
	BSYNC_TRACE
	goto restart;
}

static void
bsync_txn_process(ev_loop * /*loop*/, ev_async */*watcher*/, int /*event*/)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_CONCAT(&bsync_state.txn_proxy_input,
		&bsync_state.txn_proxy_queue);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	while (!STAILQ_EMPTY(&bsync_state.txn_proxy_input)) {
		struct bsync_txn_info *info = STAILQ_FIRST(
			&bsync_state.txn_proxy_input);
		if (info->owner) {BSYNC_TRACE
			STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
			fiber_call(info->owner);
		} else {BSYNC_TRACE
			info->process(info);
		}
	}
}

static void
txn_process(struct bsync_txn_info */*info*/)
{
	fiber_call(bsync_fiber(&bsync_state.txn_fibers, txn_process_fiber));
}

static void
bsync_process_fiber(va_list /* ap */)
{
restart:BSYNC_TRACE
	struct bsync_txn_info *info = NULL;
	struct bsync_operation *oper = NULL;
	if (bsync_state.state == bsync_state_shutdown)
		goto exit;
	if (bsync_state.iproxy_end) {
		assert(bsync_state.state == bsync_state_ready ||
			bsync_state.state == bsync_state_recovery);
		bsync_proxy_processor();
		goto exit;
	}
	tt_pthread_mutex_lock(&bsync_state.mutex);
	info = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	oper = (struct bsync_operation *) region_alloc(&fiber()->gc,
		sizeof(struct bsync_operation));
	oper->common = (struct bsync_common *) region_alloc(&fiber()->gc,
		sizeof(struct bsync_common));
	oper->txn_data = info;
	oper->txn_data->op = oper;
	oper->sign = oper->txn_data->sign;
	oper->common->dup_key = oper->txn_data->common->dup_key;
	oper->common->region = oper->txn_data->common->region;
	oper->owner = fiber();
	oper->host_id = bsync_state.local_id;
	if (bsync_state.state != bsync_state_ready) {
		say_debug("push request %d:%ld to election ops",
			  info->row->server_id, info->row->lsn);
		rlist_add_tail_entry(&bsync_state.election_ops, oper, list);
		fiber_yield_timeout(bsync_state.operation_timeout);
		if (bsync_state.state != bsync_state_ready) {
			BSYNC_TRACE
			info->result = -1;
			info->op = NULL;
			SWITCH_TO_TXN(oper->txn_data, txn_process);
			goto exit;
		}
	}
	if (bsync_state.leader_id == bsync_state.local_id) {
		bsync_queue_leader(oper, false);
	} else {
		oper->row = oper->txn_data->row;
		bsync_queue_slave(oper);
	}
	oper->owner = NULL;
exit:
	--bsync_state.bsync_fibers.active;
	BSYNC_TRACE
	fiber_gc();
	BSYNC_TRACE
	rlist_add_tail_entry(&bsync_state.bsync_fibers.data, fiber(), state);
	fiber_yield();
	goto restart;
}

static void
bsync_shutdown_fiber(va_list /* ap */)
{BSYNC_TRACE
	if (bsync_state.wal_commit_sign > 0 ||
		bsync_state.wal_rollback_sign > 0)
	{
		struct xrow_header *row = (struct xrow_header *)
			region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
		row->type = IPROTO_WAL_FLAG;
		bsync_wal_write(row);
		fiber_gc();
	}
	BSYNC_TRACE
	for (int i = 0; i < bsync_state.num_hosts; ++i) {
		if (!bsync_index[i].fiber_out)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_close));
	}
	BSYNC_TRACE
	bsync_state.state = bsync_state_shutdown;
	for (int i = 0; i < bsync_state.num_hosts; ++i) {
		if (!bsync_index[i].fiber_out)
			continue;
		if ((bsync_index[i].flags & bsync_host_ping_sleep) != 0)
			fiber_call(bsync_index[i].fiber_out);
		bsync_index[i].state = bsync_host_disconnected;
	}
	BSYNC_TRACE
	recovery_delete(local_state);
	ev_break(bsync_loop, 1);
	BSYNC_TRACE
}

static void
bsync_shutdown(struct bsync_txn_info * info)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	fiber_call(fiber_new("bsync_shutdown", bsync_shutdown_fiber));
}

static void
bsync_repair_dump(struct bsync_operation *oper, struct bsync_txn_info * info)
{
	struct bsync_operation *found = oper;
	bsync_state.wal_commit_sign = oper->sign;
	info->result = 1;
	while (!rlist_empty(&bsync_state.submit_queue)) {
		oper = rlist_shift_entry(&bsync_state.submit_queue,
					 struct bsync_operation, list);
		bool yield = (oper->status == bsync_op_status_yield);
		assert (oper->status != bsync_op_status_fail);
		oper->status = bsync_op_status_submit;
		if (yield)
			fiber_call(oper->owner);
		if (oper == found)
			break;
	}
}

static bool
bsync_repair_from_commit(struct bsync_txn_info * info)
{
	if (rlist_empty(&bsync_state.submit_queue))
		return false;
	struct bsync_operation *oper = NULL;
	struct bsync_operation *found = NULL;
	rlist_foreach_entry(oper, &bsync_state.submit_queue, list) {
		if (oper->sign < info->sign)
			continue;
		found = oper;
		break;
	}
	oper = rlist_last_entry(&bsync_state.submit_queue,
				struct bsync_operation, list);
	if (found == NULL && oper->sign < info->sign) {
		bsync_repair_dump(oper, info);
		return false;
	}
	if (!info->proxy) {
		bsync_repair_dump(found, info);
		return true;
	}
	oper = found;
	// check operation
	if (oper == NULL || oper->row->lsn != info->row->lsn ||
		oper->row->server_id != info->row->server_id ||
		oper->row->bodycnt != info->row->bodycnt ||
		oper->row->body[0].iov_len != info->row->body[0].iov_len ||
		memcmp(oper->row->body[0].iov_base, info->row->body[0].iov_base,
			oper->row->body[0].iov_len))
	{
		abort();
		say_warn("rollback operations from commit queue");
		bsync_state.wal_rollback_sign =
			rlist_last_entry(&bsync_state.submit_queue,
					 struct bsync_operation, list)->sign;
		while (!rlist_empty(&bsync_state.submit_queue)) {
			oper = rlist_shift_entry(&bsync_state.submit_queue,
						 struct bsync_operation, list);
			oper->txn_data->result = -1;
			oper->status = bsync_op_status_fail;
			if (oper->status == bsync_op_status_yield)
				fiber_call(oper->owner);
		}
		return false;
	}
	bsync_repair_dump(oper, info);
	return true;
}

static void
bsync_process_wal_fiber(va_list ap)
{BSYNC_TRACE
	struct bsync_txn_info * info = va_arg(ap, struct bsync_txn_info *);
	if (!bsync_repair_from_commit(info)) {
		assert(info->proxy);
		info->result = bsync_wal_write(info->row);
	}
	SWITCH_TO_TXN(info, txn_process);
	fiber_gc();
	return;
}

static void
bsync_process_wal(struct bsync_txn_info * info)
{
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	fiber_start(fiber_new("bsync_wal", bsync_process_wal_fiber), info);
}

static void
bsync_process(struct bsync_txn_info *info)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	struct bsync_txn_info *tmp = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	assert(tmp == info && info);
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
}

static bool
bsync_has_consensus()
{
	return 2 * bsync_state.num_connected > bsync_state.num_hosts
		&& bsync_state.local_id < BSYNC_MAX_HOSTS;
}

static void
bsync_stop_io(uint8_t host_id, bool close)
{BSYNC_TRACE
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		return;
	assert(bsync_state.num_connected > 0);
	assert(bsync_state.local_id == BSYNC_MAX_HOSTS ||
		bsync_state.num_connected > 1 ||
		bsync_state.state == bsync_state_shutdown ||
		BSYNC_REMOTE.state == bsync_host_follow);
	if (BSYNC_REMOTE.state != bsync_host_follow) {
		--bsync_state.num_connected;
		BSYNC_REMOTE.state = bsync_host_disconnected;
	}
	if (BSYNC_REMOTE.fiber_out && BSYNC_REMOTE.fiber_out != fiber() &&
		(BSYNC_REMOTE.flags & bsync_host_ping_sleep) != 0)
	{
		fiber_call(BSYNC_REMOTE.fiber_out);
	}
	if (BSYNC_REMOTE.fiber_in && BSYNC_REMOTE.fiber_in != fiber()) {
		BSYNC_REMOTE.fiber_in = NULL;
	}
	while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		struct bsync_send_elem *elem =
			rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					  struct bsync_send_elem, list);
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
		else if (elem->code == bsync_mtype_proxy_request) {
			bsync_do_proxy_reject((struct bsync_operation*)elem->arg);
		}
		if (elem->system)
			mempool_free(&bsync_state.system_send_pool, elem);
	}
	if (close) {
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, bsync_process_close);
	}
}

static void
bsync_stop_io_fiber(va_list ap)
{BSYNC_TRACE
	uint8_t host_id = va_arg(ap, int);
	bool close = va_arg(ap, int);
	bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_close));
	bsync_stop_io(host_id, close);
}

static void
bsync_start_event(struct bsync_txn_info * info)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	ev_async_start(local_state->writer->txn_loop,
		&local_state->writer->write_event);
}

static void
bsync_cleanup_follow(uint8_t host_id, int64_t sign)
{
	say_debug("bsync_cleanup_follow for %s (%ld)", BSYNC_REMOTE.name, sign);
	while (BSYNC_REMOTE.follow_queue_size > 0) {
		struct bsync_send_elem *elem =
			rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
				struct bsync_send_elem, list);
		if ((int64_t)((struct bsync_operation *)elem->arg)->sign <= sign) {
			--BSYNC_REMOTE.follow_queue_size;
			bsync_do_submit(host_id, elem);
		} else {
			rlist_add_entry(&BSYNC_REMOTE.follow_queue, elem, list);
			break;
		}
	}
}

static void
bsync_update_local(struct bsync_txn_info *info)
{
	assert(loop() == bsync_loop);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	bsync_state.local_id = info->connection;
	say_info("bsync_update_local %d to signature %ld",
		 bsync_state.local_id, info->sign);
	if (BSYNC_LOCAL.sign >= 0)
		BSYNC_LOCAL.commit_sign = BSYNC_LOCAL.submit_sign = info->sign;
	else
		BSYNC_LOCAL.sign = BSYNC_LOCAL.commit_sign =
			BSYNC_LOCAL.submit_sign = info->sign;
	if (BSYNC_LOCAL.state == bsync_host_connected)
		return;
	++bsync_state.num_connected;
	BSYNC_LOCAL.state = bsync_host_connected;
	bsync_start_election();
}

static bool
bsync_check_follow(uint8_t host_id, int64_t sign)
{
	bsync_cleanup_follow(host_id, sign);
	uint64_t follow_sign = BSYNC_LOCAL.sign;
	if (BSYNC_REMOTE.follow_queue_size > 0) {BSYNC_TRACE
		void *arg = rlist_first_entry(&BSYNC_REMOTE.follow_queue,
			struct bsync_send_elem, list)->arg;
		follow_sign = ((struct bsync_operation *)arg)->sign;
	}
	say_debug("follow_sign=%ld, remote_sign=%ld", follow_sign, sign);
	return sign < (int64_t)(follow_sign - 1);
}

static void
bsync_process_connect(struct bsync_txn_info *info)
{
	assert(loop() == bsync_loop);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	uint8_t host_id = info->connection;
	say_debug("bsync_process_connect with %s, signature is %ld, local is %ld",
		  BSYNC_REMOTE.name, info->sign, BSYNC_LOCAL.commit_sign);
	BSYNC_REMOTE.sign = BSYNC_REMOTE.commit_sign =
		BSYNC_REMOTE.submit_sign = info->sign;
	bool do_switch = true;
	BSYNC_TRACE
	if (bsync_state.leader_id != bsync_state.local_id &&
		bsync_state.leader_id == host_id &&
		bsync_state.state == bsync_state_recovery &&
		bsync_state.leader_id < BSYNC_MAX_HOSTS)
	{
		do_switch = false;
	}
	else if (bsync_state.leader_id == bsync_state.local_id &&
		BSYNC_REMOTE.state == bsync_host_follow)
	{BSYNC_TRACE
		do_switch = bsync_check_follow(host_id, info->sign);
	}
	if (!do_switch) {
		uint8_t num_connected = bsync_state.num_connected;
		if (2 * num_connected > bsync_state.num_hosts &&
			bsync_state.state < bsync_state_ready)
		{
			bsync_state.state = bsync_state_ready;
			if (!rlist_empty(&bsync_state.submit_queue)) {
				uint64_t commit_sign =
					rlist_last_entry(&bsync_state.submit_queue,
						struct bsync_operation, list)->sign;
				bsync_commit(commit_sign);
			}
		}
	}
	auto guard = make_scoped_guard([&]() {
		bsync_start_election();
		bsync_election_ops();
	});
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		++bsync_state.num_connected;
	if (do_switch) {
		if (BSYNC_REMOTE.state != bsync_host_follow)
			BSYNC_REMOTE.state = bsync_host_recovery;
	}
	assert(host_id != bsync_state.local_id && host_id < BSYNC_MAX_HOSTS);
	bool has_leader = bsync_state.leader_id < BSYNC_MAX_HOSTS;
	bool local_leader = has_leader &&
		bsync_state.leader_id == bsync_state.local_id;
	assert(rlist_empty(&BSYNC_REMOTE.send_queue));
	say_debug("push to bsync loop out_fd=%d, in_fd=%d, switch=%d",
		local_state->remote[host_id].out.fd,
		local_state->remote[host_id].in.fd,
		do_switch ? 1 : 0);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_outgoing),
			local_state->remote[host_id].out.fd, host_id);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_accept_handler),
			local_state->remote[host_id].in.fd, host_id);
	if (has_leader && !local_leader && host_id != bsync_state.leader_id) {
		fiber_start(fiber_new("bsync_stop_io", bsync_stop_io_fiber),
			host_id, 1);
	}
	if (!local_leader)
		return;
	/* TODO : support hot recovery from send queue */
	bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_leader_submit));
	if (do_switch) {
		bsync_send_data(&BSYNC_REMOTE,
				bsync_alloc_send(bsync_mtype_iproto_switch));
	} else {
		bsync_cleanup_follow(host_id, info->sign);
		while (!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
						struct bsync_send_elem, list);
			bsync_send_data(&BSYNC_REMOTE, elem);
		}
		BSYNC_REMOTE.follow_queue_size = 0;
		BSYNC_REMOTE.state = bsync_host_connected;
	}
}

static void
bsync_process_disconnect(struct bsync_txn_info *info)
{
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	int host_id = info->connection;
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		return;
	if (BSYNC_REMOTE.state == bsync_host_follow) {
		while (!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
						struct bsync_send_elem, list);
			bsync_do_reject(host_id, elem);
		}
		BSYNC_REMOTE.follow_queue_size = 0;
	}
	assert(bsync_state.num_connected > 0);
	assert(bsync_state.local_id == BSYNC_MAX_HOSTS ||
		bsync_state.num_connected > 1);
	--bsync_state.num_connected;
	if ((bsync_state.local_id == bsync_state.leader_id &&
		2 * bsync_state.num_connected <= bsync_state.num_hosts) ||
		host_id == bsync_state.leader_id)
	{
		bsync_state.leader_id = BSYNC_MAX_HOSTS;
		bsync_state.accept_id = BSYNC_MAX_HOSTS;
		bsync_state.state = bsync_state_initial;
		BSYNC_REMOTE.flags = 0;
	}
	BSYNC_REMOTE.state = bsync_host_disconnected;
}

static void
bsync_process_follow(struct bsync_txn_info *info)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	info->__from = __PRETTY_FUNCTION__;
	info->__line = __LINE__;
	int host_id = info->connection;
	uint64_t cleanup_sign = 0;
	tt_pthread_mutex_lock(&txn_state.mutex[host_id]);
	assert(rlist_empty(&BSYNC_REMOTE.send_queue));
	uint64_t last_sign = info->sign;
	uint64_t sign_bonus = bsync_state.max_host_queue -
			      BSYNC_REMOTE.follow_queue_size;
	if (BSYNC_REMOTE.follow_queue_size > 0) {
		assert(BSYNC_REMOTE.state == bsync_host_follow);
		void *arg = rlist_first_entry(&BSYNC_REMOTE.follow_queue,
					      struct bsync_send_elem, list)->arg;
		if (((struct bsync_operation *)arg)->sign <= info->sign) {
			arg = rlist_last_entry(&BSYNC_REMOTE.follow_queue,
					     struct bsync_send_elem, list)->arg;
			last_sign = ((struct bsync_operation *)arg)->sign;
		}
	}
	if (BSYNC_LOCAL.sign <= (last_sign + sign_bonus / 2)) {
		info->proxy = (BSYNC_REMOTE.state == bsync_host_follow ||
				bsync_state.state < bsync_state_ready);
		BSYNC_REMOTE.state = bsync_host_follow;
	}
	say_debug("follow result for %s is %ld < %ld (%ld + %ld),"
		"queue size is %ld, state=%d, proxy=%d",
		BSYNC_REMOTE.name, info->sign, BSYNC_LOCAL.sign, last_sign,
		sign_bonus, BSYNC_REMOTE.follow_queue_size, BSYNC_REMOTE.state,
		info->proxy ? 1 : 0);
	cleanup_sign = info->sign;
	tt_pthread_cond_signal(&txn_state.cond[host_id]);
	tt_pthread_mutex_unlock(&txn_state.mutex[host_id]);
	bsync_cleanup_follow(host_id, cleanup_sign);
}

static void
bsync_process_loop(struct ev_loop */*loop*/, ev_async */*watcher*/, int /*event*/)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_CONCAT(&bsync_state.bsync_proxy_input,
		      &bsync_state.bsync_proxy_queue);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	struct bsync_txn_info *info = NULL;
	while (!STAILQ_EMPTY(&bsync_state.bsync_proxy_input)) {
		BSYNC_TRACE
		info = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
		if (info->op) {
			BSYNC_TRACE
			STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
			info->__from = __PRETTY_FUNCTION__;
			info->__line = __LINE__;
			if (bsync_state.state != bsync_state_ready) {
				continue;
			}
			fiber_call(info->op->owner);
		} else {
			info->process(info);
			BSYNC_TRACE
		}
	}
}

/*
 * send leader ID to TXN
 * if we dont have snapshot start to generate and send it
 * if we have snapshot check for SLAVE for ready state
 * if SLAVE is ready switch it to ready state
 * else switch it to recovery state, send iproto_switch and switch to TXN
 */

static void
bsync_set_leader(uint8_t host_id)
{BSYNC_TRACE
	say_info("new leader are %s", BSYNC_REMOTE.name);
	bsync_state.leader_id = host_id;
	bsync_state.sysmsg.connection = host_id;
	SWITCH_TO_TXN(&bsync_state.sysmsg, bsync_txn_leader);
	if (BSYNC_LOCAL.sign == -1) {
		bsync_state.state = bsync_state_recovery;
		if (bsync_state.leader_id == bsync_state.local_id) {
			SWITCH_TO_TXN(&BSYNC_LOCAL.sysmsg, bsync_txn_join);
		}
		for (int i = 0; i < bsync_state.num_hosts; ++i) {
			if (bsync_index[i].state == bsync_host_disconnected ||
				i == host_id || i == bsync_state.local_id)
			{
				continue;
			}
			if (bsync_state.leader_id != bsync_state.local_id) {
				bsync_send_data(&bsync_index[i],
					bsync_alloc_send(bsync_mtype_close));
			} else {
				bsync_send_data(&bsync_index[i],
					bsync_alloc_send(bsync_mtype_iproto_switch));
			}
		}
		return;
	}
	if (host_id != bsync_state.local_id) {
		for (int i = 0; i < bsync_state.num_hosts; ++i) {
			if (bsync_index[i].state == bsync_host_disconnected ||
				i == host_id || i == bsync_state.local_id)
			{
				continue;
			}
			bsync_send_data(&bsync_index[i],
				bsync_alloc_send(bsync_mtype_close));
		}
		if (BSYNC_REMOTE.commit_sign > BSYNC_LOCAL.commit_sign ||
			BSYNC_LOCAL.sign == -1)
		{
			bsync_state.state = bsync_state_recovery;
		} else {
			bsync_state.state = bsync_state_ready;
			bsync_election_ops();
		}
		return;
	}
	/* start recovery */
	uint8_t num_up = 1;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		if (bsync_index[i].sign == BSYNC_LOCAL.sign) {
			++num_up;
			continue;
		}
		/* stop bsync io fibers and switch host to recovery state */
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_iproto_switch));
	}
	say_info("%s num_up = %d", __PRETTY_FUNCTION__, num_up);
	if (2 * num_up > bsync_state.num_hosts) {
		bsync_state.state = bsync_state_ready;
		bsync_election_ops();
	} else {
		bsync_state.state = bsync_state_recovery;
	}
}

/*
 * Leader election block
 */

static void
bsync_start_election()
{BSYNC_TRACE
	if (bsync_state.leader_id < BSYNC_MAX_HOSTS || !bsync_has_consensus()
		|| bsync_state.state == bsync_state_recovery)
		return;
	if (bsync_state.state == bsync_state_election) {
		if (bsync_state.num_connected == bsync_state.num_hosts)
			bsync_state.state = bsync_state_initial;
		else
			return;
	}
	assert(bsync_state.state < bsync_state_recovery);
	say_info("consensus connected");
	uint8_t max_host_id = bsync_max_host();
	say_info("next leader should be %s", bsync_index[max_host_id].name);
	if (max_host_id != bsync_state.local_id) {
		bsync_send_data(&bsync_index[max_host_id],
			bsync_alloc_send(bsync_mtype_leader_proposal));
		return;
	}
	bsync_state.num_accepted = 1;
	if (bsync_state.state == bsync_state_promise)
		return;
	else
		bsync_state.state = bsync_state_promise;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == max_host_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_leader_promise));
	}
}

static void
bsync_check_consensus(uint8_t host_id)
{BSYNC_TRACE
	BSYNC_REMOTE.sysmsg.proxy = false;
	if ((bsync_state.leader_id == BSYNC_MAX_HOSTS ||
		host_id != bsync_state.leader_id) &&
	    (bsync_state.state >= bsync_state_recovery ||
		2 * bsync_state.num_connected > bsync_state.num_hosts) &&
	    (bsync_state.local_id != bsync_state.leader_id ||
		2 * bsync_state.num_connected > bsync_state.num_hosts))
	{
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_process_reconnnect);
		return;
	}
	BSYNC_TRACE
	bsync_state.leader_id = BSYNC_MAX_HOSTS;
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	bsync_state.state = bsync_state_initial;
	BSYNC_REMOTE.sysmsg.proxy = true;
	struct bsync_send_elem *elem = NULL;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id)
			continue;
		while (!rlist_empty(&bsync_index[i].follow_queue)) {
			elem = rlist_shift_entry(&bsync_index[i].follow_queue,
						 struct bsync_send_elem, list);
			bsync_do_reject(i, elem);
		}
		while (!rlist_empty(&bsync_index[i].op_queue)) {
			elem = rlist_shift_entry(&bsync_index[i].op_queue,
						 struct bsync_send_elem, list);
			bsync_do_reject(i, elem);
		}
		while (!rlist_empty(&bsync_index[i].send_queue)) {
			elem =rlist_shift_entry(&bsync_index[i].send_queue,
						struct bsync_send_elem, list);
			switch (elem->code) {
			case bsync_mtype_body:
			case bsync_mtype_proxy_accept:
				bsync_do_reject(i, elem);
				break;
			}
			if (elem->system)
				mempool_free(&bsync_state.system_send_pool, elem);
		}
		bsync_stop_io(i, false);
		bsync_index[i].flags &= ~bsync_host_follow_fast;
		bsync_index[i].op_queue_size = 0;
		bsync_index[i].send_queue_size = 0;
		bsync_index[i].follow_queue_size = 0;
	}
	SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_process_reconnnect);
}

static void
bsync_disconnected(uint8_t host_id, bool recovery)
{BSYNC_TRACE
	if (BSYNC_REMOTE.state == bsync_host_disconnected ||
		BSYNC_REMOTE.state == bsync_host_follow ||
		bsync_state.state == bsync_state_shutdown)
		return;
	bsync_stop_io(host_id, false);
	if (host_id == bsync_state.accept_id) {
		bsync_state.state = bsync_state_initial;
		bsync_state.accept_id = BSYNC_MAX_HOSTS;
	}
	{
		BSYNC_LOCK(bsync_state.active_ops_mutex);
		mh_bsync_clear(BSYNC_REMOTE.active_ops);
	}
	/* TODO : clean up wait queue using fiber_call() */
	say_warn("disconnecting host %s", BSYNC_REMOTE.name);
	rlist_create(&BSYNC_REMOTE.follow_queue);
	BSYNC_REMOTE.follow_queue_size = 0;
	struct bsync_send_elem *elem;
	bool do_reject = (bsync_state.local_id != bsync_state.leader_id ||
			  !recovery);
	if (do_reject) {
		rlist_foreach_entry(elem, &BSYNC_REMOTE.op_queue, list) {
			bsync_do_reject(host_id, elem);
		}
	} else {
		rlist_swap(&BSYNC_REMOTE.op_queue, &BSYNC_REMOTE.follow_queue);
		BSYNC_REMOTE.follow_queue_size = BSYNC_REMOTE.op_queue_size;
	}
	while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					 struct bsync_send_elem, list);
		switch (elem->code) {
		case bsync_mtype_body:
		case bsync_mtype_proxy_accept:
			if (do_reject)
				bsync_do_reject(host_id, elem);
			else {
				assert(!elem->system);
				rlist_add_tail_entry(&BSYNC_REMOTE.follow_queue,
							elem, list);
				++BSYNC_REMOTE.follow_queue_size;
			}
			break;
		case bsync_mtype_rollback:
			/* TODO */
			break;
		}
		if (elem->system) {
			mempool_free(&bsync_state.system_send_pool, elem);
		}
	}
	if (!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
		BSYNC_REMOTE.state = bsync_host_follow;
		++bsync_state.num_connected;
		BSYNC_REMOTE.flags |= bsync_host_follow_fast;
	}
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	if (host_id == bsync_state.leader_id) {
		while (!rlist_empty(&bsync_state.wal_queue)) {
			struct bsync_operation *oper = rlist_shift_entry(
				&bsync_state.wal_queue,
				struct bsync_operation, list);
			oper->status = bsync_op_status_fail;
			if (bsync_state.wal_rollback_sign < oper->sign)
				bsync_state.wal_rollback_sign = oper->sign;
		}
		if (!rlist_empty(&bsync_state.proxy_queue)) {
			struct bsync_operation *oper = rlist_shift_entry(
				&bsync_state.proxy_queue,
				struct bsync_operation, list);
			oper->txn_data->result = -1;
			fiber_call(oper->owner);
			rlist_create(&bsync_state.proxy_queue);
		}
		if (bsync_state.wal_rollback_sign != 0) {
			struct xrow_header *row = (struct xrow_header *)
				region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
			row->type = IPROTO_WAL_FLAG;
			say_debug("rollback all ops from sign %ld",
				  bsync_state.wal_rollback_sign);
			BSYNC_LOCK(bsync_state.mutex);
			bsync_wal_write(row);
		}
	} else if (bsync_state.local_id == bsync_state.leader_id) {
		while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
			elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
						struct bsync_send_elem, list);
			switch (elem->code) {
			case bsync_mtype_body:
			case bsync_mtype_proxy_accept:
				bsync_do_reject(host_id, elem);
				break;
			case bsync_mtype_rollback:
				/* TODO */
				break;
			}
			if (elem->system) {
				mempool_free(&bsync_state.system_send_pool, elem);
			}
		}
	}
	bsync_check_consensus(host_id);
	bsync_start_election();
}

static void
bsync_leader_proposal(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	say_info("receive leader proposal from %s", BSYNC_REMOTE.name);
	if (bsync_state.state != bsync_state_promise) {
		if (bsync_state.state == bsync_state_election)
			bsync_state.state = bsync_state_initial;
		bsync_start_election();
	} else if (bsync_state.state >= bsync_state_recovery &&
		bsync_state.state < bsync_state_shutdown &&
		bsync_state.leader_id == bsync_state.local_id)
	{
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_submit));
	}
}

static void
bsync_leader_promise(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	uint8_t max_host_id = bsync_max_host();
	if (bsync_state.state >= bsync_state_recovery
		&& bsync_state.state < bsync_state_shutdown
		&& bsync_state.leader_id == bsync_state.local_id)
	{
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_submit));
	} else if (host_id == max_host_id &&
			((bsync_state.state == bsync_state_accept &&
			  bsync_state.accept_id == host_id) ||
			bsync_state.state < bsync_state_accept))
	{
		say_info("accept leader promise from %s", BSYNC_REMOTE.name);
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_accept));
		bsync_state.state = bsync_state_accept;
		bsync_state.accept_id = host_id;
	} else {
		say_warn("reject leader promise from %s(%ld),"
			"next leader should be %s(%ld), state=%d",
			BSYNC_REMOTE.name, BSYNC_REMOTE.commit_sign,
			bsync_index[max_host_id].name,
			bsync_index[max_host_id].commit_sign, bsync_state.state);
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_reject));
	}
}

static void
bsync_election_ops()
{BSYNC_TRACE
	while (!rlist_empty(&bsync_state.election_ops)
		&& bsync_state.state == bsync_state_ready) {
		struct bsync_operation *oper = rlist_shift_entry(
			&bsync_state.election_ops, struct bsync_operation,
			list);
		fiber_call(oper->owner);
	}
}

static void
bsync_leader_accept(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	if (bsync_state.state != bsync_state_promise)
		return;
	if (2 * ++bsync_state.num_accepted <= bsync_state.num_hosts)
		return;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_leader_submit));
	}
	bsync_set_leader(bsync_state.local_id);
}

static void
bsync_leader_submit(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	if (host_id != bsync_state.leader_id)
		bsync_set_leader(host_id);
}

static void
bsync_leader_reject(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	bsync_state.state = bsync_state_initial;
	bsync_start_election();
}

static void
bsync_ping(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.sign = mp_decode_uint(ipos);
}

static void
bsync_iproto_switch(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) ipos; (void) iend;
	if (bsync_state.leader_id != bsync_state.local_id) {
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_iproto_switch));
		if (BSYNC_REMOTE.fiber_out)
			fiber_join(BSYNC_REMOTE.fiber_out);
	}
	bsync_stop_io(host_id, false);
	if (BSYNC_REMOTE.fiber_out_fail) {BSYNC_TRACE
		bsync_check_consensus(host_id);
	} else {BSYNC_TRACE
		if (BSYNC_REMOTE.state != bsync_host_follow) {
			BSYNC_REMOTE.state = bsync_host_recovery;
			++bsync_state.num_connected;
		}
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_process_recovery);
	}
}

static void
bsync_close(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) ipos; (void) iend;
	if (BSYNC_REMOTE.state != bsync_host_disconnected && BSYNC_REMOTE.fiber_out)
		bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_close));
	bsync_stop_io(host_id, false);
}

/*
 * Network block
 */

typedef void
(*bsync_handler_t)(uint8_t host_id, const char** ipos, const char* iend);

static bsync_handler_t bsync_handlers[] = {
	bsync_leader_proposal,
	bsync_leader_promise,
	bsync_leader_accept,
	bsync_leader_submit,
	bsync_leader_reject,
	bsync_ping,
	bsync_iproto_switch,
	NULL,
	bsync_close,
	bsync_rollback,
	NULL,
	bsync_body,
	bsync_submit,
	bsync_reject,
	bsync_proxy_request,
	bsync_proxy_accept,
	bsync_proxy_reject,
	bsync_proxy_join
};

static int
encode_sys_request(uint8_t host_id, struct bsync_send_elem *elem, struct iovec *iov);

static void
bsync_decode_extended_header(uint8_t /* host_id */, const char **pos)
{
	uint32_t len = mp_decode_uint(pos);
	const char *end = *pos + len;
	if (!len)
		return;
	uint32_t flags = mp_decode_uint(pos);
	assert(flags);
	if (flags & bsync_iproto_commit_sign)
		bsync_commit(mp_decode_uint(pos));
	/* ignore all unknown data from extended header */
	*pos = end;
}

static void
bsync_breadn(struct ev_io *coio, struct ibuf *in, size_t sz)
{
	coio_breadn_timeout(coio, in, sz, bsync_state.read_timeout);
	if (errno == ETIMEDOUT) {
		tnt_raise(SocketError, coio->fd, "timeout");
	}
}

static uint32_t
bsync_read_package(struct ev_io *coio, struct ibuf *in, uint8_t host_id, bool tm)
{
	/* Read fixed header */
	if (ibuf_size(in) < 1) {
		if (tm) {BSYNC_TRACE
			bsync_breadn(coio, in, 1);
		} else
			coio_breadn(coio, in, 1);
	}
	/* Read length */
	if (mp_typeof(*in->pos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	if (tm) {BSYNC_TRACE}
	ssize_t to_read = mp_check_uint(in->pos, in->end);
	if (to_read > 0)
		bsync_breadn(coio, in, to_read);
	if (tm) {BSYNC_TRACE}
	uint32_t len = mp_decode_uint((const char **) &in->pos);
	if (len == 0)
		return 0;
	/* Read header and body */
	to_read = len - ibuf_size(in);
	if (to_read > 0)
		bsync_breadn(coio, in, to_read);
	if (tm) {BSYNC_TRACE}
	const char *pos = (const char *)in->pos;
	bsync_decode_extended_header(host_id, (const char **) &in->pos);
	return len - (in->pos - pos);
}

static void
bsync_incoming(struct ev_io *coio, struct iobuf *iobuf, uint8_t host_id)
{BSYNC_TRACE
	struct ibuf *in = &iobuf->in;
	while (BSYNC_REMOTE.state > bsync_host_disconnected &&
		bsync_state.state != bsync_state_shutdown)
	{
		/* cleanup buffer */
		iobuf_reset(iobuf);
		fiber_gc();
		uint32_t len = bsync_read_package(coio, in, host_id, false);
		/* proceed message */
		const char* iend = (const char *)in->pos + len;
		const char **ipos = (const char **)&in->pos;
		if (BSYNC_REMOTE.fiber_in != fiber())
			return;
		if (BSYNC_REMOTE.state > bsync_host_disconnected) {
			uint32_t type = mp_decode_uint(ipos);
			assert(type < bsync_mtype_count);
			say_debug("receive message from %s, type %s, length %d",
				BSYNC_REMOTE.name, bsync_mtype_name[type], len);
			assert(type < sizeof(bsync_handlers));
			if (type == bsync_mtype_close &&
				(bsync_state.local_id == bsync_state.leader_id ||
				host_id == bsync_state.leader_id))
			{
				BSYNC_TRACE
				bsync_disconnected(host_id, true);
			}
			(*bsync_handlers[type])(host_id, ipos, iend);
			if (type == bsync_mtype_iproto_switch)
				break;
		} else {
			*ipos = iend;
			say_warn("receive message from disconnected host %s",
				 BSYNC_REMOTE.name);
		}
	}
}

static void
bsync_accept_handler(va_list ap)
{BSYNC_TRACE
	struct ev_io coio;
	coio_init(&coio);
	coio.fd = va_arg(ap, int);
	int host_id = va_arg(ap, int);
	struct iobuf *iobuf = iobuf_new(BSYNC_REMOTE.name);
	auto iobuf_guard = make_scoped_guard([&]() {
		iobuf_delete(iobuf);
	});
	BSYNC_REMOTE.fiber_in = fiber();
	fiber_set_joinable(fiber(), true);
	struct bsync_send_elem *elem = bsync_alloc_send(bsync_mtype_bsync_switch);
	auto elem_guard = make_scoped_guard([&](){
		mempool_free(&bsync_state.system_send_pool, elem);
	});
	struct iovec iov;
	encode_sys_request(host_id, elem, &iov);
	try {
		ssize_t total_sent = coio_writev_timeout(&coio, &iov, 1, -1,
							bsync_state.write_timeout);
		if (errno == ETIMEDOUT) {
			tnt_raise(SocketError, coio.fd, "timeout");
		}
		fiber_gc();
		say_info("sent to %s bsync_switch, num bytes is %ld",
			 BSYNC_REMOTE.name, total_sent);
		bsync_incoming(&coio, iobuf, host_id);
	} catch (Exception *e) {
		if (fiber() == BSYNC_REMOTE.fiber_in) {
			e->log();
			BSYNC_TRACE
			bsync_disconnected(host_id, true);
		}
	}
	BSYNC_REMOTE.fiber_in = NULL;
	say_debug("stop incoming fiber from %s", BSYNC_REMOTE.name);
}

static int
bsync_extended_header_size(uint8_t host_id, bool sys)
{
	if (BSYNC_REMOTE.submit_sign > BSYNC_REMOTE.commit_sign && !sys)
		return mp_sizeof_uint(bsync_iproto_commit_sign) +
			mp_sizeof_uint(BSYNC_REMOTE.submit_sign);
	else
		return 0;
}

static char *
bsync_extended_header_encode(uint8_t host_id, char *pos, bool sys)
{
	pos = mp_encode_uint(pos, bsync_extended_header_size(host_id, sys));
	if (BSYNC_REMOTE.submit_sign > BSYNC_REMOTE.commit_sign && !sys) {
		pos = mp_encode_uint(pos, bsync_iproto_commit_sign);
		pos = mp_encode_uint(pos, BSYNC_REMOTE.submit_sign);
		BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign;
	}
	return pos;
}

static uint64_t
bsync_mp_real_size(uint64_t size)
{
	return mp_sizeof_uint(size) + size;
}

static void
bsync_writev(struct ev_io *coio, struct iovec *iov, int iovcnt, uint8_t host_id)
{
	BSYNC_REMOTE.flags |= bsync_host_active_write;
	auto guard = make_scoped_guard([&]() {
		BSYNC_REMOTE.flags &= ~bsync_host_active_write;
	});
	coio_writev_timeout(coio, iov, iovcnt, -1, bsync_state.write_timeout);
	if (errno == ETIMEDOUT) {
		tnt_raise(SocketError, coio->fd, "timeout");
	}
}

static int
encode_sys_request(uint8_t host_id, struct bsync_send_elem *elem, struct iovec *iov)
{
	if (elem->code == bsync_mtype_none) {
		elem->code = bsync_mtype_ping;
	}
	ssize_t size = mp_sizeof_uint(elem->code)
		+ bsync_mp_real_size(bsync_extended_header_size(host_id,
					elem->code != bsync_mtype_ping));
	switch (elem->code) {
	case bsync_mtype_leader_reject:
	case bsync_mtype_ping:
	case bsync_mtype_leader_proposal:
	case bsync_mtype_leader_accept:
	case bsync_mtype_leader_promise:
	case bsync_mtype_leader_submit:
		size += mp_sizeof_uint(BSYNC_LOCAL.commit_sign);
		break;
	case bsync_mtype_rollback:
		size += mp_sizeof_uint((uint64_t)elem->arg);
		/* no break */
	default:
		break;
	}
	iov->iov_len = mp_sizeof_uint(size) + size;
	iov->iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	char *pos = (char *) iov[0].iov_base;
	pos = mp_encode_uint(pos, size);
	pos = bsync_extended_header_encode(host_id, pos,
					elem->code != bsync_mtype_ping);
	pos = mp_encode_uint(pos, elem->code);

	switch (elem->code) {
	case bsync_mtype_leader_reject:
	case bsync_mtype_ping:
	case bsync_mtype_leader_proposal:
	case bsync_mtype_leader_accept:
	case bsync_mtype_leader_promise:
	case bsync_mtype_leader_submit:
		pos = mp_encode_uint(pos, BSYNC_LOCAL.commit_sign);
		break;
	case bsync_mtype_rollback:
		size += mp_sizeof_uint((uint64_t)elem->arg);
		/* no break */
	default:
		break;
	}
	return 1;
}

static int
encode_request(uint8_t host_id, struct bsync_send_elem *elem, struct iovec *iov)
{
	int iovcnt = 1;
	ssize_t bsize =
		bsync_mp_real_size(bsync_extended_header_size(host_id, false))
		+ mp_sizeof_uint(elem->code);
	struct bsync_operation *oper = (struct bsync_operation *)elem->arg;
	if (elem->code != bsync_mtype_proxy_request
		&& elem->code != bsync_mtype_proxy_reject
		&& elem->code != bsync_mtype_proxy_join)
	{
		bsize += mp_sizeof_uint(oper->sign);
	}
	ssize_t fsize = bsize;
	if (elem->code == bsync_mtype_body || elem->code == bsync_mtype_proxy_request) {
		iovcnt += xrow_header_encode(oper->row, iov + 1);
		for (int i = 1; i < iovcnt; ++i) {
			bsize += iov[i].iov_len;
		}
	}
	iov[0].iov_len = mp_sizeof_uint(bsize) + fsize;
	iov[0].iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	char* pos = (char*) iov[0].iov_base;
	pos = mp_encode_uint(pos, bsize);
	pos = bsync_extended_header_encode(host_id, pos, false);
	pos = mp_encode_uint(pos, elem->code);
	if (elem->code != bsync_mtype_proxy_request
		&& elem->code != bsync_mtype_proxy_reject
		&& elem->code != bsync_mtype_proxy_join)
	{
		pos = mp_encode_uint(pos, oper->sign);
	}
	return iovcnt;
}

static bool
bsync_is_out_valid(uint8_t host_id)
{
	return BSYNC_REMOTE.state > bsync_host_disconnected &&
		bsync_state.state != bsync_state_shutdown &&
		fiber() == BSYNC_REMOTE.fiber_out;
}

#define BUFV_IOVMAX 200*XROW_IOVMAX
static void
bsync_send(struct ev_io *coio, uint8_t host_id)
{BSYNC_TRACE
	while (!rlist_empty(&BSYNC_REMOTE.send_queue) &&
		bsync_is_out_valid(host_id))
	{
		struct iovec iov[BUFV_IOVMAX];
		int iovcnt = 0;
		do {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.send_queue,
						  struct bsync_send_elem, list);
			--BSYNC_REMOTE.send_queue_size;
			assert(elem->code < bsync_mtype_count);
			if (elem->code == bsync_mtype_body ||
				elem->code == bsync_mtype_proxy_accept)
			{
				++BSYNC_REMOTE.op_queue_size;
				rlist_add_tail_entry(&BSYNC_REMOTE.op_queue,
						elem, list);
				say_debug("send to %s message with type %s, lsn=%d:%ld",
					BSYNC_REMOTE.name,
					bsync_mtype_name[elem->code],
					((bsync_operation *)elem->arg)->row->server_id,
					((bsync_operation *)elem->arg)->row->lsn);
			} else if (elem->code == bsync_mtype_leader_reject ||
				   elem->code == bsync_mtype_leader_promise) {
				say_debug("send to %s message with type %s. "
					  "local sign=%ld, remote_sign=%ld",
					  BSYNC_REMOTE.name,
					  bsync_mtype_name[elem->code],
					  BSYNC_LOCAL.commit_sign,
					  BSYNC_REMOTE.commit_sign);
			} else {
				say_debug("send to %s message with type %s",
					BSYNC_REMOTE.name,
					bsync_mtype_name[elem->code]);
			}
			if (elem->code < bsync_mtype_sysend)
				iovcnt += encode_sys_request(host_id, elem,
							     iov + iovcnt);
			else
				iovcnt += encode_request(host_id, elem,
							 iov + iovcnt);
			bool stop = (elem->code == bsync_mtype_iproto_switch);
			if (elem->system)
				mempool_free(&bsync_state.system_send_pool, elem);
			if (stop) {
				while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
					elem = rlist_shift_entry(
						&BSYNC_REMOTE.send_queue,
						struct bsync_send_elem, list);
					assert(elem->system);
					mempool_free(&bsync_state.system_send_pool, elem);
				}
				BSYNC_REMOTE.fiber_out = NULL;
				break;
			}
		} while (!rlist_empty(&BSYNC_REMOTE.send_queue) &&
			 (iovcnt + XROW_IOVMAX) < BUFV_IOVMAX);
		bsync_writev(coio, iov, iovcnt, host_id);
	}
}

static void
bsync_outgoing(va_list ap)
{BSYNC_TRACE
	struct ev_io coio;
	coio_init(&coio);
	coio.fd = va_arg(ap, int);
	int host_id = va_arg(ap, int);
	BSYNC_REMOTE.fiber_out = fiber();
	fiber_set_joinable(fiber(), true);
	ev_tstamp prev = ev_now(loop());
	BSYNC_REMOTE.fiber_out_fail = false;
	say_debug("start outgoing to %s, fd=%d", BSYNC_REMOTE.name, coio.fd);
	try {
		{
			BSYNC_TRACE
			const char *source = recovery->remote[host_id].source;
			struct iobuf *iobuf = iobuf_new(source);
			BSYNC_REMOTE.flags |= bsync_host_active_write;
			BSYNC_TRACE
			auto iobuf_guard = make_scoped_guard([&]() {
				BSYNC_REMOTE.flags &= ~bsync_host_active_write;
				iobuf_delete(iobuf);
				fiber_gc();
			});
			BSYNC_TRACE
			bsync_read_package(&coio, &iobuf->in, host_id, true);
			uint32_t type = mp_decode_uint((const char **)&iobuf->in.pos);
			if (type != bsync_mtype_bsync_switch) {
				tnt_raise(SocketError, coio.fd, "incorrect handshake");
			}
			say_info("receive from %s bsync_switch", BSYNC_REMOTE.name);
		}
		bool force = false;
		while (bsync_is_out_valid(host_id))
		{
			if (bsync_extended_header_size(host_id, false) > 0 &&
				rlist_empty(&BSYNC_REMOTE.send_queue) &&
				bsync_state.state != bsync_state_shutdown &&
				!force)
			{
				fiber_yield_timeout(bsync_state.submit_timeout);
			}
			force = false;
			ev_tstamp now = ev_now(loop());
			while (bsync_state.state == bsync_state_ready &&
				(now - prev) < 0.001)
			{
				fiber_yield_timeout(0.001 + prev - now);
				now = ev_now(loop());
			}
			bool was_empty = rlist_empty(&BSYNC_REMOTE.send_queue);
			bsync_send(&coio, host_id);
			prev = ev_now(loop());
			fiber_gc();
			if (was_empty && bsync_is_out_valid(host_id) &&
				rlist_empty(&BSYNC_REMOTE.send_queue))
			{
				if (bsync_extended_header_size(host_id, false) != 0) {
					bsync_send_data(&BSYNC_REMOTE, &BSYNC_REMOTE.ping_msg);
					force = true;
					continue;
				}
				if (BSYNC_REMOTE.state == bsync_host_connected) {
					BSYNC_TRACE
					BSYNC_REMOTE.flags |= bsync_host_ping_sleep;
					fiber_yield_timeout(bsync_state.ping_timeout);
					BSYNC_REMOTE.flags &= ~bsync_host_ping_sleep;
					if (BSYNC_REMOTE.send_queue_size != 0)
						continue;
					bsync_send_data(&BSYNC_REMOTE, &BSYNC_REMOTE.ping_msg);
					force = true;
				} else {
					BSYNC_TRACE
					BSYNC_REMOTE.flags |= bsync_host_ping_sleep;
					fiber_yield();
					BSYNC_REMOTE.flags &= ~bsync_host_ping_sleep;
				}
			}
		}
	} catch (Exception *e) {
		if (bsync_is_out_valid(host_id)) {
			e->log();
			BSYNC_REMOTE.fiber_out_fail = true;
			BSYNC_TRACE
			bsync_disconnected(host_id, true);
		}
	}
	if (fiber() == BSYNC_REMOTE.fiber_out)
		BSYNC_REMOTE.fiber_out = NULL;
}

/*
 * System block:
 * 1. initialize local variables;
 * 2. read cfg;
 * 3. start/stop cord and event loop
 */

static void
bsync_cfg_push_host(uint8_t host_id, const char *source)
{
	strncpy(BSYNC_REMOTE.name, source, 1024);
	BSYNC_REMOTE.fiber_in = NULL;
	BSYNC_REMOTE.fiber_out = NULL;
	BSYNC_REMOTE.sign = -1;
	BSYNC_REMOTE.submit_sign = -1;
	BSYNC_REMOTE.commit_sign = -1;
	BSYNC_REMOTE.flags = 0;
	BSYNC_REMOTE.state = bsync_host_disconnected;
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
	rlist_create(&BSYNC_REMOTE.follow_queue);
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	BSYNC_REMOTE.follow_queue_size = 0;
	BSYNC_REMOTE.active_ops = mh_bsync_new();
	if (BSYNC_REMOTE.active_ops == NULL)
	panic("out of memory");
	memset(&BSYNC_REMOTE.sysmsg, 0, sizeof(struct bsync_txn_info));
	BSYNC_REMOTE.sysmsg.connection = host_id;
	BSYNC_REMOTE.ping_msg.code = bsync_mtype_ping;
	BSYNC_REMOTE.ping_msg.arg = NULL;
	BSYNC_REMOTE.ping_msg.system = false;

	pthread_mutexattr_t errorcheck;
	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&txn_state.mutex[host_id], &errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);
	(void) tt_pthread_cond_init(&txn_state.cond[host_id], NULL);
}

static void
bsync_cfg_read()
{
	bsync_state.num_connected = 0;
	bsync_state.state = bsync_state_election;
	bsync_state.local_id = BSYNC_MAX_HOSTS;
	bsync_state.leader_id = BSYNC_MAX_HOSTS;
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	txn_state.leader_id = BSYNC_MAX_HOSTS;
	txn_state.local_id = BSYNC_MAX_HOSTS;

	bsync_state.read_timeout = cfg_getd("replication.read_timeout");
	bsync_state.write_timeout = cfg_getd("replication.write_timeout");
	bsync_state.operation_timeout = cfg_getd(
		"replication.operation_timeout");
	bsync_state.ping_timeout = cfg_getd("replication.ping_timeout");
	bsync_state.election_timeout = cfg_getd("replication.election_timeout");
	bsync_state.submit_timeout = cfg_getd("replication.submit_timeout");
	bsync_state.num_hosts = cfg_getarr_size("replication.source");
	bsync_state.max_host_queue = cfg_geti("replication.max_host_queue");
}

void
bsync_init(struct recovery_state *r)
{
	local_state = r;
	if (!r->bsync_remote) {
		return;
	}
	bsync_state.iproxy_end = NULL;
	bsync_state.iproxy_pos = NULL;
	bsync_state.wal_commit_sign = 0;
	bsync_state.wal_rollback_sign = 0;
	memset(&bsync_state.sysmsg, 0, sizeof(struct bsync_txn_info));
	/* I. Initialize the state. */
	pthread_mutexattr_t errorcheck;

	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck,
		PTHREAD_MUTEX_ERRORCHECK);
#endif
	tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_RECURSIVE);
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&bsync_state.mutex, &errorcheck);
	(void) tt_pthread_mutex_init(&bsync_state.active_ops_mutex,
		&errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);

	(void) tt_pthread_cond_init(&bsync_state.cond, NULL);

	STAILQ_INIT(&bsync_state.txn_proxy_input);
	STAILQ_INIT(&bsync_state.txn_proxy_queue);
	STAILQ_INIT(&bsync_state.bsync_proxy_input);
	STAILQ_INIT(&bsync_state.bsync_proxy_queue);
	bsync_state.txn_fibers.size = 0;
	bsync_state.txn_fibers.active = 0;
	bsync_state.bsync_fibers.size = 0;
	bsync_state.bsync_fibers.active = 0;
	bsync_state.bsync_rollback = false;
	rlist_create(&bsync_state.txn_fibers.data);
	rlist_create(&bsync_state.bsync_fibers.data);
	rlist_create(&bsync_state.proxy_queue);
	rlist_create(&bsync_state.submit_queue);
	rlist_create(&bsync_state.commit_queue);
	rlist_create(&bsync_state.txn_queue);
	rlist_create(&bsync_state.wal_queue);
	rlist_create(&bsync_state.execute_queue);
	rlist_create(&bsync_state.election_ops);
	rlist_create(&bsync_state.region_free);
	bsync_state.region_free_size = 0;
	rlist_create(&bsync_state.region_gc);
	rlist_create(&txn_state.incoming_connections);
	rlist_create(&txn_state.wait_start);
	if (recovery_has_data(r))
		txn_state.state = txn_state_subscribe;
	else
		txn_state.state = txn_state_join;

	slab_cache_create(&bsync_state.txn_slabc, &runtime);
	mempool_create(&bsync_state.region_pool, &bsync_state.txn_slabc,
		sizeof(struct bsync_region));

	txn_state.recovery = false;
	txn_state.snapshot_fiber = NULL;
	memset(txn_state.iproto, 0, sizeof(txn_state.iproto));
	memset(txn_state.join, 0, sizeof(txn_state.join));
	memset(txn_state.wait_local, 0, sizeof(txn_state.wait_local));
	memset(txn_state.id2index, BSYNC_MAX_HOSTS, sizeof(txn_state.id2index));

	ev_async_init(&txn_process_event, bsync_txn_process);
	ev_async_init(&bsync_process_event, bsync_process_loop);

	txn_loop = loop();

	ev_async_start(loop(), &txn_process_event);

	/* II. Start the thread. */
	bsync_cfg_read();
	for (int i = 0; i < r->remote_size; ++i) {
		bsync_cfg_push_host(i, r->remote[i].source);
	}
	tt_pthread_mutex_lock(&bsync_state.mutex);
	if (!cord_start(&bsync_state.cord, "bsync", bsync_thread, NULL)) {
		tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
	} else {
		local_state = NULL;
	}
}

void
bsync_start(struct recovery_state *r)
{BSYNC_TRACE
	if (!r->bsync_remote) {
		ev_async_start(local_state->writer->txn_loop,
			&local_state->writer->write_event);
		return;
	}
	assert(local_state == r);
	local_state->writer->txn_loop = bsync_loop;
	txn_state.state = txn_state_subscribe;
	struct bsync_txn_info *info = &bsync_state.sysmsg;
	SWITCH_TO_BSYNC(bsync_start_event);

	for (int i = 0; i < local_state->remote_size; ++i) {
		if (!local_state->remote[i].connected ||
			local_state->remote[i].localhost ||
			local_state->remote[i].writer == NULL)
			continue;
		fiber_call(local_state->remote[i].writer);
	}
}

static void
bsync_election(va_list /* ap */)
{
	fiber_yield_timeout(bsync_state.election_timeout);
	if (bsync_state.state != bsync_state_election)
		return;
	bsync_state.state = bsync_state_initial;
	bsync_start_election();
}

static void*
bsync_thread(void*)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	iobuf_init();
	slab_cache_create(&bsync_state.bsync_slabc, &runtime);
	mempool_create(&bsync_state.system_send_pool, &bsync_state.bsync_slabc,
		sizeof(struct bsync_send_elem));
	if (bsync_state.num_hosts == 1) {
		bsync_state.leader_id = bsync_state.local_id = 0;
		bsync_index[0].state = bsync_host_connected;
		bsync_state.state = bsync_state_ready;
	}
	bsync_loop = loop();
	ev_async_start(loop(), &bsync_process_event);
	tt_pthread_cond_signal(&bsync_state.cond);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	fiber_call(fiber_new("bsync_election", bsync_election));
	try {
		ev_run(loop(), 0);
	} catch (...) {
		say_crit("bsync_thread found unhandled exception");
		throw;
	}
	ev_async_stop(bsync_loop, &bsync_process_event);
	say_info("bsync stopped");
	slab_cache_destroy(&bsync_state.bsync_slabc);
	return NULL;
}

void
bsync_writer_stop(struct recovery_state *r)
{BSYNC_TRACE
	assert(local_state == r);
	if (!local_state->bsync_remote) {
		recovery_delete(r);
		return;
	}
	if (cord_is_main()) {
		tt_pthread_mutex_lock(&bsync_state.mutex);
		if (bsync_state.bsync_rollback) {
			tt_pthread_cond_signal(&bsync_state.cond);
		}
		tt_pthread_mutex_unlock(&bsync_state.mutex);
		struct bsync_txn_info *info = &bsync_state.sysmsg;
		SWITCH_TO_BSYNC(bsync_shutdown);
		if (&bsync_state.cord != cord() && cord_join(&bsync_state.cord)) {
			panic_syserror("BSYNC writer: thread join failed");
		}
	}
	ev_async_stop(txn_loop, &txn_process_event);
	bsync_state.txn_fibers.size = bsync_state.txn_fibers.active = 0;
	bsync_state.bsync_fibers.size = bsync_state.bsync_fibers.active = 0;
	rlist_create(&bsync_state.txn_fibers.data);
	rlist_create(&bsync_state.bsync_fibers.data);
	rlist_create(&bsync_state.proxy_queue);
	rlist_create(&bsync_state.submit_queue);
	rlist_create(&bsync_state.txn_queue);
	rlist_create(&bsync_state.execute_queue);
	rlist_create(&bsync_state.election_ops);
	rlist_create(&bsync_state.region_free);
	rlist_create(&bsync_state.region_gc);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		rlist_create(&BSYNC_REMOTE.op_queue);
		BSYNC_REMOTE.op_queue_size = 0;
		rlist_create(&BSYNC_REMOTE.send_queue);
		BSYNC_REMOTE.send_queue_size = 0;
		rlist_create(&BSYNC_REMOTE.follow_queue);
		mh_bsync_delete(BSYNC_REMOTE.active_ops);
	}
	tt_pthread_mutex_destroy(&bsync_state.mutex);
	tt_pthread_mutex_destroy(&bsync_state.active_ops_mutex);
	slab_cache_destroy(&bsync_state.txn_slabc);
}
