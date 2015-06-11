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
#include "replication.h"
#include "sophia_engine.h"
#include "cfg.h"
#include "xrow.h"
#include "tuple.h"
#include "scoped_guard.h"
#include "txn.h"
#include "index.h"
#include "sophia_index.h"
#include "recovery.h"
#include "space.h"
#include "schema.h"
#include "request.h"
#include "iproto_constants.h"
#include "replication.h"
#include "salad/rlist.h"
#include <sophia.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

void sophia_raise(void *env)
{
	void *c = sp_ctl(env);
	void *o = sp_get(c, "sophia.error");
	char *error = (char *)sp_get(o, "value", NULL);
	auto scoped_guard =
		make_scoped_guard([=] { sp_destroy(o); });
	tnt_raise(ClientError, ER_SOPHIA, error);
}

void sophia_info(void (*callback)(const char*, const char*, void*), void *arg)
{
	SophiaEngine *engine = (SophiaEngine*)engine_find("sophia");
	void *env = engine->env;
	void *c = sp_ctl(env);
	void *o, *cur = sp_cursor(c);
	if (cur == NULL)
		sophia_raise(env);
	while ((o = sp_get(cur))) {
		const char *k = (const char *)sp_get(o, "key", NULL);
		const char *v = (const char *)sp_get(o, "value", NULL);
		callback(k, v, arg);
	}
	sp_destroy(cur);
}

void
sophia_replace(struct txn * /* txn */, struct space *space,
	       struct tuple *old_tuple, struct tuple *new_tuple,
               enum dup_replace_mode mode)
{
	Index *index = index_find(space, 0);
	(void) index->replace(old_tuple, new_tuple, mode);
}

struct SophiaSpace: public Handler {
	SophiaSpace(Engine*);
};

SophiaSpace::SophiaSpace(Engine *e)
	:Handler(e)
{
	replace = sophia_replace;
}

SophiaEngine::SophiaEngine()
	:Engine("sophia")
	 ,m_prev_checkpoint_lsn(-1)
	 ,m_checkpoint_lsn(-1)
	 ,recovery_complete(0)
{
	flags = 0;
	env = NULL;
}

static inline int
sophia_poll(SophiaEngine *e)
{
	void *req = sp_poll(e->env);
	if (req == NULL)
		return 0;
	struct fiber *fiber = (struct fiber *)sp_get(req, "arg");
	fiber_call(fiber);
	sp_destroy(req);
	return 1;
}

static inline int
sophia_queue(SophiaEngine *e)
{
	void *env = e->env;
	void *c = sp_ctl(env);
	void *o = sp_get(c, "scheduler.reqs");
	if (o == NULL)
		sophia_raise(env);
	char *p = (char *)sp_get(o, "value", NULL);
	uint32_t queue = atoi(p);
	sp_destroy(o);
	return queue;
}

static inline void
sophia_on_event(void *arg)
{
	SophiaEngine *engine = (SophiaEngine *)arg;
	ev_async_send(engine->cord->loop, &engine->watcher);
}

static void
sophia_idle_cb(ev_loop *loop, struct ev_idle *w, int /* events */)
{
	SophiaEngine *engine = (SophiaEngine *)w->data;
	sophia_poll(engine);
	if (sophia_queue(engine) == 0)
		ev_idle_stop(loop, w);
}

static void
sophia_async_schedule(ev_loop *loop, struct ev_async *w, int /* events */)
{
	SophiaEngine *engine = (SophiaEngine *)w->data;
	while (sophia_poll(engine));
	ev_idle_start(loop, &engine->idle);
}

void
SophiaEngine::init()
{
	cord = cord();
	ev_idle_init(&idle, sophia_idle_cb);
	ev_async_init(&watcher, sophia_async_schedule);
	ev_async_start(cord->loop, &watcher);
	watcher.data = this;
	idle.data = this;

	env = sp_env();
	if (env == NULL)
		panic("failed to create sophia environment");
	void *c = sp_ctl(env);
	sp_set(c, "sophia.path", cfg_gets("sophia_dir"));
	sp_set(c, "sophia.path_create", "0");
	sp_set(c, "scheduler.threads", cfg_gets("sophia.threads"));
	char callback[64];
	snprintf(callback, sizeof(callback), "pointer: %p", (void*)sophia_on_event);
	char arg[64];
	snprintf(arg, sizeof(arg), "pointer: %p", this);
	sp_set(c, "scheduler.on_event", callback, arg);
	sp_set(c, "memory.limit", cfg_gets("sophia.memory_limit"));
	sp_set(c, "compaction.node_size", cfg_gets("sophia.node_size"));
	sp_set(c, "compaction.page_size", cfg_gets("sophia.page_size"));
	sp_set(c, "compaction.0.async", "1");
	sp_set(c, "log.enable", "0");
	sp_set(c, "log.two_phase_recover", "1");
	sp_set(c, "log.commit_lsn", "1");
	int rc = sp_open(env);
	if (rc == -1)
		sophia_raise(env);
}

void
SophiaEngine::endRecovery()
{
	if (recovery_complete)
		return;
	/* complete two-phase recovery */
	int rc = sp_open(env);
	if (rc == -1)
		sophia_raise(env);
	recovery_complete = 1;
}

Handler *
SophiaEngine::open()
{
	return new SophiaSpace(this);
}

static inline void
sophia_send_row(Relay *relay, uint32_t space_id, char *tuple,
                uint32_t tuple_size)
{
	struct recovery_state *r = relay->r;
	struct request_replace_body body;
	body.m_body = 0x82; /* map of two elements. */
	body.k_space_id = IPROTO_SPACE_ID;
	body.m_space_id = 0xce; /* uint32 */
	body.v_space_id = mp_bswap_u32(space_id);
	body.k_tuple = IPROTO_TUPLE;
	struct xrow_header row;
	row.type = IPROTO_INSERT;
	row.server_id = 0;
	row.lsn = vclock_inc(&r->vclock, row.server_id);
	row.bodycnt = 2;
	row.body[0].iov_base = &body;
	row.body[0].iov_len = sizeof(body);
	row.body[1].iov_base = tuple;
	row.body[1].iov_len = tuple_size;
	relay_send(relay, &row);
}

static inline struct key_def*
sophia_join_key_def(void *db)
{
	/* prepare space schema */
	void *dbctl = sp_ctl(db);
	void *oid = sp_get(dbctl, "name");
	char *name = (char*)sp_get(oid, "value", NULL);
	char *pe = NULL;
	uint32_t space_id = strtoul(name, &pe, 10);
	sp_destroy(oid);
	void *opart = sp_get(dbctl, "partcount");
	char *partcountsz = (char*)sp_get(opart, "value", NULL);
	uint32_t partcount = strtoul(partcountsz, &pe, 10);
	sp_destroy(opart);
	struct key_def *key_def;
	key_def = key_def_new(space_id, 0, "sophia_join", TREE, true, partcount);
	int i = 0;
	while (i < partcount) {
		char part[32];
		snprintf(part, sizeof(part), "%d", i);
		void *opart = sp_get(dbctl, part);
		char *parttype = (char*)sp_get(oid, "value", NULL);
		key_def->parts[i].fieldno = i;
		if (strcmp(parttype, "string") == 0)
			key_def->parts[i].type = STRING;
		else
		if (strcmp(parttype, "u64") == 0)
			key_def->parts[i].type = NUM;
		sp_destroy(opart);
		i++;
	}
	return key_def;
}

/**
 * Relay all data that should be present in the snapshot
 * to the replica.
 */
void
SophiaEngine::join(Relay *relay)
{
	struct vclock *res = vclockset_last(&relay->r->snap_dir.index);
	if (res == NULL)
		tnt_raise(ClientError, ER_MISSING_SNAPSHOT);
	int64_t signt = vclock_sum(res);

	/* get snapshot object */
	char id[128];
	snprintf(id, sizeof(id), "snapshot.%" PRIu64, signt);
	void *c = sp_ctl(env);
	void *snapshot = sp_get(c, id);
	assert(snapshot != NULL);

	/* iterate through a list of databases which took a
	 * part in the snapshot */
	void *db_cursor = sp_ctl(snapshot, "db_list");
	if (db_cursor == NULL)
		sophia_raise(env);

	while (sp_get(db_cursor)) {
		void *db = sp_object(db_cursor);
		/* prepare space schema */
		struct key_def *key_def;
		try {
			key_def = sophia_join_key_def(db);
		} catch (...) {
			sp_destroy(db_cursor);
			throw;
		}
		/* send database */
		void *o = sp_object(db);
		void *cursor = sp_cursor(snapshot, o);
		if (cursor == NULL) {
			sp_destroy(db_cursor);
			key_def_delete(key_def);
			sophia_raise(env);
		}
		while ((o = sp_get(cursor))) {
			uint32_t tuple_size;
			char *tuple = (char *)sophia_tuple(o, key_def, NULL, &tuple_size);
			try {
				sophia_send_row(relay, key_def->space_id, tuple, tuple_size);
			} catch (...) {
				key_def_delete(key_def);
				free(tuple);
				sp_destroy(cursor);
				sp_destroy(db_cursor);
				throw;
			}
			free(tuple);
		}
		sp_destroy(cursor);
		key_def_delete(key_def);
	}
	sp_destroy(db_cursor);
}

Index*
SophiaEngine::createIndex(struct key_def *key_def)
{
	switch (key_def->type) {
	case TREE: return new SophiaIndex(key_def);
	default:
		assert(false);
		return NULL;
	}
}

void
SophiaEngine::dropIndex(Index *index)
{
	SophiaIndex *i = (SophiaIndex*)index;
	/* schedule asynchronous drop */
	int rc = sp_drop(i->db);
	if (rc == -1)
		sophia_raise(env);
	/* unref db object */
	rc = sp_destroy(i->db);
	if (rc == -1)
		sophia_raise(env);
	/* maybe start asynchronous database
	 * shutdown: last snapshot might hold a
	 * db pointer. */
	rc = sp_destroy(i->db);
	if (rc == -1)
		sophia_raise(env);
	i->db  = NULL;
	i->env = NULL;
}

void
SophiaEngine::keydefCheck(struct space *space, struct key_def *key_def)
{
	switch (key_def->type) {
	case TREE: {
		if (! key_def->is_unique) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "Sophia TREE index must be unique");
		}
		if (key_def->iid != 0) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "Sophia TREE secondary indexes are not supported");
		}
		int i = 0;
		while (i < key_def->part_count) {
			struct key_part *part = &key_def->parts[i];
			if (part->type != NUM && part->type != STRING) {
				tnt_raise(ClientError, ER_MODIFY_INDEX,
				          key_def->name,
				          space_name(space),
				          "Sophia TREE index field type must be STR or NUM");
			}
			if (part->fieldno != i) {
				tnt_raise(ClientError, ER_MODIFY_INDEX,
				          key_def->name,
				          space_name(space),
				          "Sophia TREE key-parts must follow first and cannot be sparse");
			}
			i++;
		}
		break;
	}
	default:
		tnt_raise(ClientError, ER_INDEX_TYPE,
			  key_def->name,
			  space_name(space));
		break;
	}
}

void
SophiaEngine::beginStatement(struct txn *txn)
{
	assert(txn->engine_tx == NULL || txn->n_stmts != 1);
	if (txn->n_stmts == 1) {
		txn->engine_tx = sp_begin(env);
		if (txn->engine_tx == NULL)
			sophia_raise(env);
	}
}

void
SophiaEngine::prepare(struct txn *txn)
{
	int rc = sp_prepare(txn->engine_tx);
	switch (rc) {
	case 1: /* rollback */
		txn->engine_tx = NULL;
	case 2: /* lock */
		tnt_raise(ClientError, ER_TRANSACTION_CONFLICT);
		break;
	case -1:
		sophia_raise(env);
		break;
	}
}

void
SophiaEngine::commit(struct txn *txn)
{
	if (txn->engine_tx == NULL)
		return;
	/* commit transaction using transaction
	 * commit signature */
	assert(txn->signature >= 0);
	int rc = sp_commit(txn->engine_tx, txn->signature);
	if (rc == -1) {
		panic("sophia commit failed: txn->signature = %"
		      PRIu64, txn->signature);
	}
	txn->engine_tx = NULL;
}

void
SophiaEngine::rollbackStatement(struct txn_stmt *)
{
	panic("not implemented");
}

void
SophiaEngine::rollback(struct txn *txn)
{
	if (txn->engine_tx) {
		sp_destroy(txn->engine_tx);
		txn->engine_tx = NULL;
	}
}

void
SophiaEngine::beginJoin()
{
	/* put engine to recovery-complete state to
	 * correctly support join */
	endRecovery();
}

static inline void
sophia_snapshot(void *env, int64_t lsn)
{
	/* start asynchronous checkpoint */
	void *c = sp_ctl(env);
	int rc = sp_set(c, "scheduler.checkpoint");
	if (rc == -1)
		sophia_raise(env);
	char snapshot[128];
	snprintf(snapshot, sizeof(snapshot), "snapshot.%" PRIu64, lsn);
	/* ensure snapshot is not already exists */
	void *o = sp_get(c, snapshot);
	if (o) {
		return;
	}
	snprintf(snapshot, sizeof(snapshot), "%" PRIu64, lsn);
	rc = sp_set(c, "snapshot", snapshot);
	if (rc == -1)
		sophia_raise(env);
}

static inline void
sophia_reference_checkpoint(void *env, int64_t lsn)
{
	/* recovered snapshot lsn is >= then last
	 * engine lsn */
	char checkpoint_id[32];
	snprintf(checkpoint_id, sizeof(checkpoint_id), "%" PRIu64, lsn);
	void *c = sp_ctl(env);
	int rc = sp_set(c, "snapshot", checkpoint_id);
	if (rc == -1)
		sophia_raise(env);
	char snapshot[128];
	snprintf(snapshot, sizeof(snapshot), "snapshot.%" PRIu64 ".lsn", lsn);
	rc = sp_set(c, snapshot, checkpoint_id);
	if (rc == -1)
		sophia_raise(env);
}

static inline int
sophia_snapshot_ready(void *env, int64_t lsn)
{
	/* get sophia lsn associated with snapshot */
	char snapshot[128];
	snprintf(snapshot, sizeof(snapshot), "snapshot.%" PRIu64 ".lsn", lsn);
	void *c = sp_ctl(env);
	void *o = sp_get(c, snapshot);
	if (o == NULL) {
		if (sp_error(env))
			sophia_raise(env);
		panic("sophia snapshot %" PRIu64 " does not exist", lsn);
	}
	char *pe;
	char *p = (char *)sp_get(o, "value", NULL);
	int64_t snapshot_start_lsn = strtoull(p, &pe, 10);
	sp_destroy(o);

	/* compare with a latest completed checkpoint lsn */
	o = sp_get(c, "scheduler.checkpoint_lsn_last");
	if (o == NULL)
		sophia_raise(env);
	p = (char *)sp_get(o, "value", NULL);
	int64_t last_lsn = strtoull(p, &pe, 10);
	sp_destroy(o);
	return last_lsn >= snapshot_start_lsn;
}

static inline void
sophia_delete_checkpoint(void *env, int64_t lsn)
{
	char snapshot[128];
	snprintf(snapshot, sizeof(snapshot), "snapshot.%" PRIu64, lsn);
	void *c = sp_ctl(env);
	void *s = sp_get(c, snapshot);
	if (s == NULL) {
		if (sp_error(env))
			sophia_raise(env);
		panic("sophia snapshot %" PRIu64 " does not exist", lsn);
	}
	int rc = sp_destroy(s);
	if (rc == -1)
		sophia_raise(env);
}

void
SophiaEngine::recoverToCheckpoint(int64_t checkpoint_id)
{
	/*
	 * Create a reference to the "current" snapshot,
	 * to ensure correct reference counting when a new
	 * snapshot is created.
	 * Sophia doesn't store snapshot references persistently,
	 * so, after recovery, we remember the reference to the
	 * "previous" snapshot, so that when the next snapshot is
	 * taken, this reference is garbage collected. This
	 * will also prevent this snapshot from accidental
	 * garbage collection before a new snapshot is created,
	 * and thus ensure correct crash recovery in case of crash
	 * in the period between startup and creation of the first
	 * snapshot.
	 */
	sophia_reference_checkpoint(env, checkpoint_id);
	m_prev_checkpoint_lsn = checkpoint_id;
}

int
SophiaEngine::beginCheckpoint(int64_t lsn)
{
	assert(m_checkpoint_lsn == -1);
	if (lsn != m_prev_checkpoint_lsn) {
		sophia_snapshot(env, lsn);
		m_checkpoint_lsn = lsn;
		return 0;
	}
	errno = EEXIST;
	return -1;
}

int
SophiaEngine::waitCheckpoint()
{
	assert(m_checkpoint_lsn != -1);
	while (! sophia_snapshot_ready(env, m_checkpoint_lsn))
		fiber_yield_timeout(.020);
	return 0;
}

void
SophiaEngine::commitCheckpoint()
{
	if (m_prev_checkpoint_lsn >= 0)
		sophia_delete_checkpoint(env, m_prev_checkpoint_lsn);
	m_prev_checkpoint_lsn = m_checkpoint_lsn;
	m_checkpoint_lsn = -1;
}

void
SophiaEngine::abortCheckpoint()
{
	if (m_checkpoint_lsn >= 0) {
		sophia_delete_checkpoint(env, m_checkpoint_lsn);
		m_checkpoint_lsn = -1;
	}
}

int sophia_schedule(void)
{
	SophiaEngine *engine = (SophiaEngine *)engine_find("sophia");
	assert(engine->env != NULL);
	void *c = sp_ctl(engine->env);
	return sp_set(c, "scheduler.run");
}
