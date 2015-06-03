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
#include "sophia_index.h"
#include "say.h"
#include "tuple.h"
#include "scoped_guard.h"
#include "errinj.h"
#include "schema.h"
#include "space.h"
#include "txn.h"
#include "cfg.h"
#include "sophia_engine.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <sophia.h>
#include <stdio.h>
#include <inttypes.h>

enum sophia_op {
	SOPHIA_SET,
	SOPHIA_DELETE
};

static uint64_t num_parts[16];

static inline void*
sophia_key(void *env, void *db, struct key_def *key_def,
           const char *key,
           const char **keyend, int async)
{
	assert(key_def->part_count < 16);
	void *o = sp_object(db);
	if (o == NULL)
		sophia_raise(env);
	if (async) {
		sp_set(o, "async", 1, fiber());
	}
	if (key == NULL)
		return o;
	int i = 0;
	while (i < key_def->part_count) {
		char partname[32];
		int len = snprintf(partname, sizeof(partname), "key");
		if (i > 0)
			 snprintf(partname + len, sizeof(partname) - len, "_%d", i);
		const char *part;
		uint32_t partsize;
		if (key_def->parts[i].type == STRING) {
			part = mp_decode_str(&key, &partsize);
		} else {
			num_parts[i] = mp_decode_uint(&key);
			part = (char *)&num_parts[i];
			partsize = sizeof(uint64_t);
		}
		if (sp_set(o, partname, part, partsize) == -1)
			sophia_raise(env);
		i++;
	}
	if (keyend)
		*keyend = key;
	return o;
}

void *sophia_tuple(void *o, struct key_def *key_def, struct tuple_format *format, uint32_t *bsize)
{
	int valuesize = 0;
	char *value = (char*)sp_get(o, "value", &valuesize);
	char *valueend = value + valuesize;

	assert(key_def->part_count < 16);
	struct {
		const char *part;
		int size;
	} parts[16];

	/* prepare keys */
	int size = 0;
	int i = 0;
	while (i < key_def->part_count) {
		char partname[32];
		int len = snprintf(partname, sizeof(partname), "key");
		if (i > 0)
			 snprintf(partname + len, sizeof(partname) - len, "_%d", i);
		parts[i].part = (const char*)sp_get(o, partname, &parts[i].size);
		assert(parts[i].part != NULL);
		if (key_def->parts[i].type == STRING) {
			size += mp_sizeof_str(parts[i].size);
		} else {
			size += mp_sizeof_uint(*(uint64_t*)parts[i].part);
		}
		i++;
	}
	int count = key_def->part_count;
	char *p = value;
	while (p < valueend) {
		count++;
		mp_next((const char**)&p);
	}
	size += mp_sizeof_array(count);
	size += valuesize;
	if (bsize)
		*bsize = size;

	/* build tuple */
	struct tuple *tuple;
	char *raw;
	if (format) {
		tuple = tuple_alloc(format, size);
		p = tuple->data;
	} else {
		raw = (char*)malloc(size);
		if (raw == NULL)
			tnt_raise(ClientError, ER_MEMORY_ISSUE, size, "tuple");
		p = raw;
	}
	p = mp_encode_array(p, count);
	for (i = 0; i < key_def->part_count; i++) {
		if (key_def->parts[i].type == STRING)
			p = mp_encode_str(p, parts[i].part, parts[i].size);
		else
			p = mp_encode_uint(p, *(uint64_t*)parts[i].part);
	}
	memcpy(p, value, valuesize);
	if (format) {
		try {
			tuple_init_field_map(format, tuple, (uint32_t*)tuple);
		} catch (...) {
			tuple_delete(tuple);
			throw;
		}
		return tuple;
	}
	return raw;
}

static inline void
sophia_write(void *env, void *db, void *tx, enum sophia_op op,
             struct key_def *key_def,
             struct tuple *tuple)
{
	const char *key = tuple_field(tuple, key_def->parts[0].fieldno);
	const char *value;
	size_t valuesize;
	void *o = sophia_key(env, db, key_def, key, &value, 0);
	valuesize = tuple->bsize - (value - tuple->data);
	if (valuesize > 0)
		sp_set(o, "value", value, valuesize);
	int rc;
	switch (op) {
	case SOPHIA_DELETE:
		rc = sp_delete(tx, o);
		break;
	case SOPHIA_SET:
		rc = sp_set(tx, o);
		break;
	}
	if (rc == -1)
		sophia_raise(env);
}

static struct tuple*
sophia_read(void *env, void *db, void *tx, const char *key,
            struct key_def *key_def,
            struct tuple_format *format)
{
	const char *keyend;
	void *o = sophia_key(env, db, key_def, key, &keyend, 1);
	void *request = sp_get((tx) ? tx: db, o);
	if (request == NULL)
		return NULL;
	fiber_yield();
	void *result = sp_get(request, "result");
	if (result == NULL)
		return NULL;
	return (struct tuple *)sophia_tuple(result, key_def, format, NULL);
}

static inline void*
sophia_configure(struct space *space, struct key_def *key_def)
{
	SophiaEngine *engine =
		(SophiaEngine*)space->handler->engine;
	void *env = engine->env;
	void *c = sp_ctl(env);
	char name[128];
	snprintf(name, sizeof(name), "%" PRIu32, key_def->space_id);
	sp_set(c, "db", name);
	snprintf(name, sizeof(name), "db.%" PRIu32 ".format",
	         key_def->space_id);
	sp_set(c, name, "kv");
	int i = 0;
	while (i < key_def->part_count)
	{
		char *type;
		if (key_def->parts[i].type == NUM)
			type = (char *)"u64";
		else
			type = (char *)"string";
		char part[32];
		if (i == 0) {
			snprintf(part, sizeof(part), "key");
		} else {
			snprintf(name, sizeof(name), "db.%" PRIu32 ".index",
			         key_def->space_id);
			snprintf(part, sizeof(part), "key_%d", i);
			sp_set(c, name, part);
		}
		snprintf(name, sizeof(name), "db.%" PRIu32 ".index.%s",
		         key_def->space_id, part);
		sp_set(c, name, type);
		i++;
	}
	snprintf(name, sizeof(name), "db.%" PRIu32 ".compression", key_def->space_id);
	sp_set(c, name, cfg_gets("sophia.compression"));
	snprintf(name, sizeof(name), "db.%" PRIu32 ".compression_key", key_def->space_id);
	sp_set(c, name, cfg_gets("sophia.compression_key"));
	snprintf(name, sizeof(name), "db.%" PRIu32, key_def->space_id);
	void *db = sp_get(c, name);
	if (db == NULL)
		sophia_raise(env);
	return db;
}

SophiaIndex::SophiaIndex(struct key_def *key_def_arg __attribute__((unused)))
	: Index(key_def_arg)
{
	struct space *space = space_cache_find(key_def->space_id);
	SophiaEngine *engine =
		(SophiaEngine*)space->handler->engine;
	env = engine->env;
	db = sophia_configure(space, key_def);
	if (db == NULL)
		sophia_raise(env);
	/* start two-phase recovery for a space:
	 * a. created after snapshot recovery
	 * b. created during log recovery
	*/
	int rc = sp_open(db);
	if (rc == -1)
		sophia_raise(env);
	tuple_format_ref(space->format, 1);
}

SophiaIndex::~SophiaIndex()
{
	if (m_position != NULL) {
		m_position->free(m_position);
		m_position = NULL;
	}
	if (db) {
		int rc = sp_destroy(db);
		if (rc == 0)
			return;
		void *c = sp_ctl(env);
		void *o = sp_get(c, "sophia.error");
		char *error = (char *)sp_get(o, "value", NULL);
		say_info("sophia space %d close error: %s",
		         key_def->space_id, error);
		sp_destroy(o);
	}
}

size_t
SophiaIndex::size() const
{
	void *c = sp_ctl(env);
	char name[128];
	snprintf(name, sizeof(name), "db.%" PRIu32 ".index.count",
	         key_def->space_id);
	void *o = sp_get(c, name);
	if (o == NULL)
		sophia_raise(env);
	uint64_t count = atoi((const char *)sp_get(o, "value", NULL));
	sp_destroy(o);
	return count;
}

size_t
SophiaIndex::bsize() const
{
	void *c = sp_ctl(env);
	char name[128];
	snprintf(name, sizeof(name), "db.%" PRIu32 ".index.memory_used",
	         key_def->space_id);
	void *o = sp_get(c, name);
	if (o == NULL)
		sophia_raise(env);
	uint64_t used = atoi((const char *)sp_get(o, "value", NULL));
	sp_destroy(o);
	return used;
}

struct tuple *
SophiaIndex::findByKey(const char *key, uint32_t part_count) const
{
	assert(key_def->is_unique && part_count == key_def->part_count);
	(void) part_count;
	struct space *space = space_cache_find(key_def->space_id);
	void *tx = in_txn() ? in_txn()->engine_tx : NULL;
	return sophia_read(env, db, tx, key, key_def, space->format);
}

struct tuple *
SophiaIndex::replace(struct tuple *old_tuple, struct tuple *new_tuple,
                     enum dup_replace_mode mode)
{
	struct space *space = space_cache_find(key_def->space_id);
	struct txn *txn = in_txn();
	assert(txn != NULL && txn->engine_tx != NULL);
	void *tx = txn->engine_tx;

	/* This method does not return old tuple for replace,
	 * insert or update.
	 *
	 * Delete does return old tuple to be properly
	 * scheduled for wal write.
	 */

	/* Switch from INSERT to REPLACE during recovery.
	 *
	 * Database might hold newer key version than currenly
	 * recovered log record.
	 */
	if (mode == DUP_INSERT) {
		SophiaEngine *engine = (SophiaEngine*)space->handler->engine;
		if (! engine->recovery_complete)
			mode = DUP_REPLACE_OR_INSERT;
	}

	/* delete */
	if (old_tuple && new_tuple == NULL) {
		sophia_write(env, db, tx, SOPHIA_DELETE, key_def, old_tuple);
		return old_tuple;
	}

	/* update */
	if (old_tuple && new_tuple) {
		/* assume no primary key update is supported */
		sophia_write(env, db, tx, SOPHIA_SET, key_def, new_tuple);
		return NULL;
	}

	/* insert or replace */
	switch (mode) {
	case DUP_INSERT: {
		const char *key = tuple_field(new_tuple, key_def->parts[0].fieldno);
		struct tuple *dup_tuple =
			sophia_read(env, db, tx, key, key_def, space->format);
		if (dup_tuple) {
			int error = tuple_compare(dup_tuple, new_tuple, key_def) == 0;
			tuple_delete(dup_tuple);
			if (error) {
				struct space *sp =
					space_cache_find(key_def->space_id);
				tnt_raise(ClientError, ER_TUPLE_FOUND,
					  index_name(this), space_name(sp));
			}
		}
	}
	case DUP_REPLACE_OR_INSERT:
		sophia_write(env, db, tx, SOPHIA_SET, key_def, new_tuple);
		break;
	case DUP_REPLACE:
	default:
		assert(0);
		break;
	}
	return NULL;
}

struct sophia_iterator {
	struct iterator base;
	const char *key;
	const char *keyend;
	uint32_t part_count;
	struct space *space;
	struct key_def *key_def;
	void *env;
	void *db;
	void *cursor;
	void *tx;
};

void
sophia_iterator_free(struct iterator *ptr)
{
	assert(ptr->free == sophia_iterator_free);
	struct sophia_iterator *it = (struct sophia_iterator *) ptr;
	if (it->cursor)
		sp_destroy(it->cursor);
	free(ptr);
}

void
sophia_iterator_close(struct iterator *ptr)
{
	assert(ptr->free == sophia_iterator_free);
	struct sophia_iterator *it = (struct sophia_iterator *) ptr;
	if (it->cursor) {
		sp_destroy(it->cursor);
		it->cursor = NULL;
	}
}

struct tuple *
sophia_iterator_next(struct iterator *ptr)
{
	assert(ptr->next == sophia_iterator_next);
	struct sophia_iterator *it = (struct sophia_iterator *) ptr;
	assert(it->cursor != NULL);
	void *request = sp_get(it->cursor);
	if (request == NULL)
		return NULL;
	fiber_yield();
	void *result = sp_get(request, "result");
	if (result == NULL)
		return NULL;
	return (struct tuple *)sophia_tuple(result, it->key_def, it->space->format, NULL);
}

struct tuple *
sophia_iterator_last(struct iterator *ptr __attribute__((unused)))
{
	return NULL;
}

struct tuple *
sophia_iterator_eq(struct iterator *ptr)
{
	ptr->next = sophia_iterator_last;
	struct sophia_iterator *it = (struct sophia_iterator *) ptr;
	assert(it->cursor == NULL);
	return sophia_read(it->env, it->db, it->tx, it->key,
	                   it->key_def,
	                   it->space->format);
}

struct iterator *
SophiaIndex::allocIterator() const
{
	struct sophia_iterator *it =
		(struct sophia_iterator *) calloc(1, sizeof(*it));
	if (it == NULL) {
		tnt_raise(ClientError, ER_MEMORY_ISSUE,
		          sizeof(struct sophia_iterator), "SophiaIndex",
		          "iterator");
	}
	it->base.next  = sophia_iterator_next;
	it->base.close = sophia_iterator_close;
	it->base.free  = sophia_iterator_free;
	it->cursor = NULL;
	return (struct iterator *) it;
}

void
SophiaIndex::initIterator(struct iterator *ptr,
                          enum iterator_type type,
                          const char *key, uint32_t part_count) const
{
	struct sophia_iterator *it = (struct sophia_iterator *) ptr;
	assert(it->cursor == NULL);
	if (part_count == 0) {
		key = NULL;
	}
	it->key = key;
	it->key_def = key_def;
	it->part_count = part_count;
	it->env = env;
	it->db = db;
	it->space = space_cache_find(key_def->space_id);
	it->tx = NULL;
	const char *compare;
	switch (type) {
	case ITER_EQ:
		it->base.next = sophia_iterator_eq;
		it->tx = in_txn() ? in_txn()->engine_tx : NULL;
		return;
	case ITER_ALL:
	case ITER_GE: compare = ">=";
		break;
	case ITER_GT: compare = ">";
		break;
	case ITER_LE: compare = "<=";
		break;
	case ITER_LT: compare = "<";
		break;
	default:
		tnt_raise(ClientError, ER_UNSUPPORTED,
		          "SophiaIndex", "requested iterator type");
	}
	it->base.next = sophia_iterator_next;
	void *o = sophia_key(env, db, key_def, key, &it->keyend, 1);
	sp_set(o, "order", compare);
	it->cursor = sp_cursor(db, o);
	if (it->cursor == NULL)
		sophia_raise(env);
	fiber_yield();
}
