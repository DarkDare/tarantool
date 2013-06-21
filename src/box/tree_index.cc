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
#include "tree_index.h"
#include "tuple.h"
#include "space.h"
#include "exception.h"
#include "errinj.h"
#include <pickle.h>

/* {{{ Utilities. *************************************************/

/**
 * Unsigned 32-bit int comparison.
 */
static inline int
u32_cmp(u32 a, u32 b)
{
	return a < b ? -1 : (a > b);
}

/**
 * Unsigned 64-bit int comparison.
 */
static inline int
u64_cmp(u64 a, u64 b)
{
	return a < b ? -1 : (a > b);
}

/**
 * Tuple address comparison.
 */
static inline int
ta_cmp(struct tuple *tuple_a, struct tuple *tuple_b)
{
	if (!tuple_a)
		return 0;
	if (!tuple_b)
		return 0;
	return tuple_a < tuple_b ? -1 : (tuple_a > tuple_b);
}

/* }}} */

/* {{{ Tree internal data types. **********************************/

/**
 * Tree types
 *
 * There are four specialized kinds of tree indexes optimized for different
 * combinations of index fields.
 *
 * In the most general case tuples consist of variable length fields and the
 * index uses a sparsely distributed subset of these fields. So to determine
 * the field location in the tuple it is required to scan all the preceding
 * fields. To avoid such scans on each access to a tree node we use the SPARSE
 * tree index structure. It pre-computes on the per-tuple basis the required
 * field offsets (or immediate field values for NUMs) and stores them in the
 * corresponding tree node.
 *
 * In case the index fields form a dense sequence it is possible to find
 * each successive field location based on the previous field location. So it
 * is only required to find the offset of the first index field in the tuple
 * and store it in the tree node. In this case we use the DENSE tree index
 * structure.
 *
 * In case the index consists of only one small field it is cheaper to
 * store the field value immediately in the node rather than store the field
 * offset. In this case we use the NUM32 tree index structure.
 *
 * In case the first field offset in a dense sequence is constant there is no
 * need to store any extra data in the node. For instance, the first index
 * field may be the first field in the tuple so the offset is always zero or
 * all the preceding fields may be fixed-size NUMs so the offset is a non-zero
 * constant. In this case we use the FIXED tree structure.
 *
 * Note that there may be fields with unknown types. In particular, if a field
 * is not used by any index then it doesn't have to be typed. So in many cases
 * we cannot actually determine if the fields preceding to the index are fixed
 * size or not. Therefore we may miss the opportunity to use this optimization
 * in such cases.
 */
enum tree_type { TREE_SPARSE, TREE_DENSE, TREE_NUM32, TREE_FIXED };

/**
 * Representation of a STR field within a sparse tree index.

 * Depending on the STR length we keep either the offset of the field within
 * the tuple or a copy of the field. Specifically, if the STR length is less
 * than or equal to 7 then the length is stored in the "length" field while
 * the copy of the STR data in the "data" field. Otherwise the STR offset in
 * the tuple is stored in the "offset" field. The "length" field in this case
 * is set to 0xFF. The actual length has to be read from the tuple.
 */
struct sparse_str
{
	union
	{
		char data[7];
		u32 offset;
	};
	u8 length;
} __attribute__((packed));

#define BIG_LENGTH 0xff

/**
 * Reprsentation of a tuple field within a sparse tree index.
 *
 * For all NUMs and short STRs it keeps a copy of the field, for long STRs
 * it keeps the offset of the field in the tuple.
 */
union sparse_part {
	u32 num32;
	u64 num64;
	struct sparse_str str;
};

#define _SIZEOF_SPARSE_PARTS(part_count) \
	(sizeof(union sparse_part) * (part_count))

#define SIZEOF_SPARSE_PARTS(def) _SIZEOF_SPARSE_PARTS((def)->part_count)

/**
 * Tree nodes for different tree types
 */

struct sparse_node {
	struct tuple *tuple;
	union sparse_part parts[];
} __attribute__((packed));

struct dense_node {
	struct tuple *tuple;
	u32 offset;
} __attribute__((packed));

struct num32_node {
	struct tuple *tuple;
	u32 value;
} __attribute__((packed));

struct fixed_node {
	struct tuple *tuple;
};

/**
 * Representation of data for key search. The data corresponds to some
 * struct key_def. The part_count field from struct key_data may be less
 * than or equal to the part_count field from the struct key_def. Thus
 * the search data may be partially specified.
 *
 * For simplicity sake the key search data uses sparse_part internally
 * regardless of the target kind of tree because there is little benefit
 * of having the most compact representation of transient search data.
 */
struct key_data
{
	const char *data;
	u32 part_count;
	union sparse_part parts[];
};

/* }}} */

/* {{{ Tree auxiliary functions. **********************************/

/**
 * Find if the field has fixed offset.
 */
static int
find_fixed_offset(struct space *space, u32 fieldno, u32 skip)
{
	u32 i = skip;
	u32 offset = 0;

	while (i < fieldno) {
		/* if the field is unknown give up on it */
		if (i >= space_max_fieldno(space) || space_field_type(space, i) == UNKNOWN) {
			return -1;
		}

		/* On a fixed length field account for the appropiate
		   varint length code and for the actual data length */
		if (space_field_type(space, i) == NUM) {
			offset += 1 + 4;
		} else if (space_field_type(space, i) == NUM64) {
			offset += 1 + 8;
		}
		/* On a variable length field give up */
		else {
			return -1;
		}

		++i;
	}

	return offset;
}

/**
 * Find the first index field.
 */
static u32
find_first_field(struct key_def *key_def)
{
	for (u32 field = 0; field < key_def->max_fieldno; ++field) {
		u32 part = key_def->cmp_order[field];
		if (part != BOX_FIELD_MAX) {
			return field;
		}
	}
	panic("index field not found");
}

/**
 * Find the appropriate tree type for a given key.
 */
static enum tree_type
find_tree_type(struct space *space, struct key_def *key_def)
{
	int dense = 1;
	int fixed = 1;

	/* Scan for the first tuple field used by the index */
	u32 field = find_first_field(key_def);
	if (find_fixed_offset(space, field, 0) < 0) {
		fixed = 0;
	}

	/* Check that there are no gaps after the first field */
	for (; field < key_def->max_fieldno; ++field) {
		u32 part = key_def->cmp_order[field];
		if (part == BOX_FIELD_MAX) {
			dense = 0;
			break;
		}
	}

	/* Return the appropriate type */
	if (!dense) {
		return TREE_SPARSE;
	} else if (fixed) {
		return TREE_FIXED;
	} else if (key_def->part_count == 1 && key_def->parts[0].type == NUM) {
		return TREE_NUM32;
	} else {
		return TREE_DENSE;
	}
}

/**
 * Check if key parts make a linear sequence of fields.
 */
static bool
key_is_linear(struct key_def *key_def)
{
	if (key_def->part_count > 1) {
		u32 prev = key_def->parts[0].fieldno;
		for (u32 i = 1; i < key_def->part_count; ++i) {
			u32 next = key_def->parts[i].fieldno;
			if (next != (prev + 1)) {
				return false;
			}
			prev = next;
		}
	}
	return true;
}

/**
 * Find field offsets/values for a sparse node.
 */
static void
fold_with_sparse_parts(struct key_def *key_def, struct tuple *tuple, union sparse_part* parts)
{
	assert(tuple->field_count >= key_def->max_fieldno);

	const char *part_data = tuple->data;

	memset(parts, 0, sizeof(parts[0]) * key_def->part_count);

	for (u32 field = 0; field < key_def->max_fieldno; ++field) {
		assert(field < tuple->field_count);

		const char *data = part_data;
		u32 len = load_varint32(&data);

		u32 part = key_def->cmp_order[field];
		if (part != BOX_FIELD_MAX) {
			if (key_def->parts[part].type == NUM) {
				if (len != sizeof parts[part].num32) {
					tnt_raise(IllegalParams, "key is not u32");
				}
				memcpy(&parts[part].num32, data, len);
			} else if (key_def->parts[part].type == NUM64) {
				if (len != sizeof parts[part].num64) {
					tnt_raise(IllegalParams, "key is not u64");
				}
				memcpy(&parts[part].num64, data, len);
			} else if (len <= sizeof(parts[part].str.data)) {
				parts[part].str.length = len;
				memcpy(parts[part].str.data, data, len);
			} else {
				parts[part].str.length = BIG_LENGTH;
				parts[part].str.offset = (u32) (part_data - tuple->data);
			}
		}

		part_data = data + len;
	}
}

/**
 * Find field offsets/values for a key.
 */
static void
fold_with_key_parts(struct key_def *key_def, struct key_data *key_data)
{
	const char *part_data = key_data->data;
	union sparse_part* parts = key_data->parts;

	memset(parts, 0, sizeof(parts[0]) * key_def->part_count);

	u32 part_count = MIN(key_def->part_count, key_data->part_count);
	for (u32 part = 0; part < part_count; ++part) {
		const char *data = part_data;
		u32 len = load_varint32(&data);

		if (key_def->parts[part].type == NUM) {
			if (len != sizeof parts[part].num32)
				tnt_raise(IllegalParams, "key is not u32");
			memcpy(&parts[part].num32, data, len);
		} else if (key_def->parts[part].type == NUM64) {
			if (len != sizeof parts[part].num64)
				tnt_raise(IllegalParams, "key is not u64");
			memcpy(&parts[part].num64, data, len);
		} else if (len <= sizeof(parts[part].str.data)) {
			parts[part].str.length = len;
			memcpy(parts[part].str.data, data, len);
		} else {
			parts[part].str.length = BIG_LENGTH;
			parts[part].str.offset = (u32) (part_data - key_data->data);
		}

		part_data = data + len;
	}
}

/**
 * Find the offset for a dense node.
 */
static u32
fold_with_dense_offset(struct key_def *key_def, struct tuple *tuple)
{
	const char *tuple_data = tuple->data;

	for (u32 field = 0; field < key_def->max_fieldno; ++field) {
		assert(field < tuple->field_count);

		const char *data = tuple_data;
		u32 len = load_varint32(&data);

		u32 part = key_def->cmp_order[field];
		if (part != BOX_FIELD_MAX) {
			return (u32) (tuple_data - tuple->data);
		}

		tuple_data = data + len;
	}

	panic("index field not found");
}

/**
 * Find the value for a num32 node.
 */
static u32
fold_with_num32_value(struct key_def *key_def, struct tuple *tuple)
{
	const char *tuple_data = tuple->data;

	for (u32 field = 0; field < key_def->max_fieldno; ++field) {
		assert(field < tuple->field_count);

		const char *data = tuple_data;
		u32 len = load_varint32(&data);

		u32 part = key_def->cmp_order[field];
		if (part != BOX_FIELD_MAX) {
			u32 value;
			assert(len == sizeof value);
			memcpy(&value, data, sizeof value);
			return value;
		}

		tuple_data = data + len;
	}

	panic("index field not found");
}

/**
 * Compare a part for two keys.
 */
static int
sparse_part_compare(enum field_data_type type,
		    const char *data_a, union sparse_part part_a,
		    const char *data_b, union sparse_part part_b)
{
	if (type == NUM) {
		return u32_cmp(part_a.num32, part_b.num32);
	} else if (type == NUM64) {
		return u64_cmp(part_a.num64, part_b.num64);
	} else {
		int cmp;
		const char *ad, *bd;
		u32 al = part_a.str.length;
		u32 bl = part_b.str.length;
		if (al == BIG_LENGTH) {
			ad = data_a + part_a.str.offset;
			al = load_varint32(&ad);
		} else {
			assert(al <= sizeof(part_a.str.data));
			ad = part_a.str.data;
		}
		if (bl == BIG_LENGTH) {
			bd = data_b + part_b.str.offset;
			bl = load_varint32(&bd);
		} else {
			assert(bl <= sizeof(part_b.str.data));
			bd = part_b.str.data;
		}

		cmp = memcmp(ad, bd, MIN(al, bl));
		if (cmp == 0) {
			cmp = (int) al - (int) bl;
		}

		return cmp;
	}
}

/**
 * Compare a key for two sparse nodes.
 */
static int
sparse_node_compare(struct key_def *key_def,
		    struct tuple *tuple_a,
		    const union sparse_part* parts_a,
		    struct tuple *tuple_b,
		    const union sparse_part* parts_b)
{
	for (u32 part = 0; part < key_def->part_count; ++part) {
		int r = sparse_part_compare(key_def->parts[part].type,
					    tuple_a->data, parts_a[part],
					    tuple_b->data, parts_b[part]);
		if (r) {
			return r;
		}
	}
	return 0;
}

/**
 * Compare a key for a key search data and a sparse node.
 */
static int
sparse_key_node_compare(struct key_def *key_def,
			const struct key_data *key_data,
			struct tuple *tuple,
			const union sparse_part* parts)
{
	u32 part_count = MIN(key_def->part_count, key_data->part_count);
	for (u32 part = 0; part < part_count; ++part) {
		int r = sparse_part_compare(key_def->parts[part].type,
					    key_data->data,
					    key_data->parts[part],
					    tuple->data, parts[part]);
		if (r) {
			return r;
		}
	}
	return 0;
}

/**
 * Compare a part for two dense keys.
 */
static int
dense_part_compare(enum field_data_type type,
		   const char *ad, u32 al,
		   const char *bd, u32 bl)
{
	if (type == NUM) {
		u32 an, bn;
		assert(al == sizeof an && bl == sizeof bn);
		memcpy(&an, ad, sizeof an);
		memcpy(&bn, bd, sizeof bn);
		return u32_cmp(an, bn);
	} else if (type == NUM64) {
		u64 an, bn;
		assert(al == sizeof an && bl == sizeof bn);
		memcpy(&an, ad, sizeof an);
		memcpy(&bn, bd, sizeof bn);
		return u64_cmp(an, bn);
	} else {
		int cmp = memcmp(ad, bd, MIN(al, bl));
		if (cmp == 0) {
			cmp = (int) al - (int) bl;
		}
		return cmp;
	}
}

/**
 * Compare a key for two dense nodes.
 */
static int
dense_node_compare(struct key_def *key_def, u32 first_field,
		   struct tuple *tuple_a, u32 offset_a,
		   struct tuple *tuple_b, u32 offset_b)
{
	u32 part_count = key_def->part_count;
	assert(first_field + part_count <= tuple_a->field_count);
	assert(first_field + part_count <= tuple_b->field_count);

	/* Allocate space for offsets. */
	u32 *off_a = (u32 *) alloca(2 * part_count * sizeof(u32));
	u32 *off_b = off_a + part_count;

	/* Find field offsets. */
	off_a[0] = offset_a;
	off_b[0] = offset_b;
	if (part_count > 1) {
		const char *ad = tuple_a->data + offset_a;
		const char *bd = tuple_b->data + offset_b;
		for (u32 i = 1; i < part_count; ++i) {
			u32 al = load_varint32(&ad);
			u32 bl = load_varint32(&bd);
			ad += al;
			bd += bl;
			off_a[i] = ad - tuple_a->data;
			off_b[i] = bd - tuple_b->data;
		}
	}

	/* Compare key parts. */
	for (u32 part = 0; part < part_count; ++part) {
		u32 field = key_def->parts[part].fieldno;
		const char *ad = tuple_a->data + off_a[field - first_field];
		const char *bd = tuple_b->data + off_b[field - first_field];
		u32 al = load_varint32(&ad);
		u32 bl = load_varint32(&bd);
		int r = dense_part_compare(key_def->parts[part].type,
					   ad, al, bd, bl);
		if (r) {
			return r;
		}
	}
	return 0;
}

/**
 * Compare a part for two dense keys with parts in linear order.
 */
static int
linear_node_compare(struct key_def *key_def,
		    u32 first_field  __attribute__((unused)),
		    struct tuple *tuple_a, u32 offset_a,
		    struct tuple *tuple_b, u32 offset_b)
{
	u32 part_count = key_def->part_count;
	assert(first_field + part_count <= tuple_a->field_count);
	assert(first_field + part_count <= tuple_b->field_count);

	/* Compare key parts. */
	const char *ad = tuple_a->data + offset_a;
	const char *bd = tuple_b->data + offset_b;
	for (u32 part = 0; part < part_count; ++part) {
		u32 al = load_varint32(&ad);
		u32 bl = load_varint32(&bd);
		int r = dense_part_compare(key_def->parts[part].type,
					   ad, al, bd, bl);
		if (r) {
			return r;
		}
		ad += al;
		bd += bl;
	}
	return 0;
}

/**
 * Compare a part for a key search data and a dense key.
 */
static int
dense_key_part_compare(enum field_data_type type,
		       const char *data_a, union sparse_part part_a,
		       const char *bd, u32 bl)
{
	if (type == NUM) {
		u32 an, bn;
		an = part_a.num32;
		assert(bl == sizeof bn);
		memcpy(&bn, bd, sizeof bn);
		return u32_cmp(an, bn);
	} else if (type == NUM64) {
		u64 an, bn;
		an = part_a.num64;
		assert(bl == sizeof bn);
		memcpy(&bn, bd, sizeof bn);
		return u64_cmp(an, bn);
	} else {
		int cmp;
		const char *ad;
		u32 al = part_a.str.length;
		if (al == BIG_LENGTH) {
			ad = data_a + part_a.str.offset;
			al = load_varint32(&ad);
		} else {
			assert(al <= sizeof(part_a.str.data));
			ad = part_a.str.data;
		}

		cmp = memcmp(ad, bd, MIN(al, bl));
		if (cmp == 0) {
			cmp = (int) al - (int) bl;
		}

		return cmp;
	}
}

/**
 * Compare a key for a key search data and a dense node.
 */
static int
dense_key_node_compare(struct key_def *key_def,
		       const struct key_data *key_data,
		       u32 first_field, struct tuple *tuple, u32 offset)
{
	u32 part_count = key_def->part_count;
	assert(first_field + part_count <= tuple->field_count);

	/* Allocate space for offsets. */
	u32 *off = (u32 *) alloca(part_count * sizeof(u32));

	/* Find field offsets. */
	off[0] = offset;
	if (part_count > 1) {
		const char *data = tuple->data + offset;
		for (u32 i = 1; i < part_count; ++i) {
			u32 len = load_varint32(&data);
			data += len;
			off[i] = data - tuple->data;
		}
	}

	/* Compare key parts. */
	if (part_count > key_data->part_count)
		part_count = key_data->part_count;
	for (u32 part = 0; part < part_count; ++part) {
		u32 field = key_def->parts[part].fieldno;
		const char *bd = tuple->data + off[field - first_field];
		u32 bl = load_varint32(&bd);
		int r = dense_key_part_compare(key_def->parts[part].type,
					       key_data->data,
					       key_data->parts[part],
					       bd, bl);
		if (r) {
			return r;
		}
	}
	return 0;
}

/**
 * Compare a key for a key search data and a dense node with parts in
 * linear order.
 */
static int
linear_key_node_compare(struct key_def *key_def,
			const struct key_data *key_data,
			u32 first_field __attribute__((unused)),
			struct tuple *tuple, u32 offset)
{
	u32 part_count = key_def->part_count;
	assert(first_field + part_count <= tuple->field_count);

	/* Compare key parts. */
	if (part_count > key_data->part_count)
		part_count = key_data->part_count;
	const char *bd = tuple->data + offset;
	for (u32 part = 0; part < part_count; ++part) {
		u32 bl = load_varint32(&bd);
		int r = dense_key_part_compare(key_def->parts[part].type,
					       key_data->data,
					       key_data->parts[part],
					       bd, bl);
		if (r) {
			return r;
		}
		bd += bl;
	}
	return 0;
}

/* }}} */

/* {{{ TreeIndex Iterators ****************************************/

struct tree_iterator {
	struct iterator base;
	const TreeIndex *index;
	struct sptree_index_iterator *iter;
	struct key_data key_data;
};

static void
tree_iterator_free(struct iterator *iterator);

static inline struct tree_iterator *
tree_iterator(struct iterator *it)
{
	assert(it->free == tree_iterator_free);
	return (struct tree_iterator *) it;
}

static void
tree_iterator_free(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	if (it->iter)
		sptree_index_iterator_free(it->iter);
	free(it);
}

static struct tuple *
tree_iterator_ge(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	void *node = sptree_index_iterator_next(it->iter);
	return it->index->unfold(node);
}

static struct tuple *
tree_iterator_le(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	void *node = sptree_index_iterator_reverse_next(it->iter);
	return it->index->unfold(node);
}

static struct tuple *
tree_iterator_eq(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	void *node = sptree_index_iterator_next(it->iter);
	if (node && it->index->tree.compare(&it->key_data, node,
					    (void *) it->index) == 0)
		return it->index->unfold(node);

	return NULL;
}

static struct tuple *
tree_iterator_req(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	void *node = sptree_index_iterator_reverse_next(it->iter);
	if (node != NULL
	    && it->index->tree.compare(&it->key_data, node,
				       (void *) it->index) == 0) {
		return it->index->unfold(node);
	}

	return NULL;
}

static struct tuple *
tree_iterator_lt(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	void *node ;
	while ((node = sptree_index_iterator_reverse_next(it->iter)) != NULL) {
		if (it->index->tree.compare(&it->key_data, node,
					    (void *) it->index) != 0) {
			it->base.next = tree_iterator_le;
			return it->index->unfold(node);
		}
	}

	return NULL;
}

static struct tuple *
tree_iterator_gt(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	void *node;
	while ((node = sptree_index_iterator_next(it->iter)) != NULL) {
		if (it->index->tree.compare(&it->key_data, node,
					    (void *) it->index) != 0) {
			it->base.next = tree_iterator_ge;
			return it->index->unfold(node);
		}
	}

	return NULL;
}

/* }}} */

/* {{{ TreeIndex -- base tree index class *************************/

class SparseTreeIndex: public TreeIndex {
public:
	SparseTreeIndex(struct key_def *key_def, struct space *space);

// protected:
	size_t
	node_size() const;

	tree_cmp_t
	node_cmp() const;

	tree_cmp_t
	dup_node_cmp() const;

	tree_cmp_t
	key_node_cmp() const;

	void
	fold(void *node, struct tuple *tuple) const;

	struct tuple *
	unfold(const void *node) const;
};


class DenseTreeIndex: public TreeIndex {
public:
	DenseTreeIndex(struct key_def *key_def, struct space *space);

// protected:
	size_t
	node_size() const;

	tree_cmp_t
	node_cmp() const;

	tree_cmp_t
	dup_node_cmp() const;

	tree_cmp_t
	key_node_cmp() const;

	void
	fold(void *node, struct tuple *tuple) const;

	struct tuple *
	unfold(const void *node) const;

//private:
	u32 first_field;
};

class Num32TreeIndex: public TreeIndex {
public:
	Num32TreeIndex(struct key_def *key_def, struct space *space);

// protected:
	size_t
	node_size() const;

	tree_cmp_t
	node_cmp() const;

	tree_cmp_t
	dup_node_cmp() const;

	tree_cmp_t
	key_node_cmp() const;

	void
	fold(void *node, struct tuple *tuple) const;

	struct tuple *
	unfold(const void *node) const;
};


class FixedTreeIndex: public TreeIndex {
public:
	FixedTreeIndex(struct key_def *key_def, struct space *space);

protected:
	size_t
	node_size() const;

	tree_cmp_t
	node_cmp() const;

	tree_cmp_t
	dup_node_cmp() const;

	tree_cmp_t
	key_node_cmp() const;

	void
	fold(void *node, struct tuple *tuple) const;

	struct tuple *
	unfold(const void *node) const;
//private:
public:
	u32 first_field;
	u32 first_offset;
};


TreeIndex *
TreeIndex::factory(struct key_def *key_def, struct space *space)
{
	enum tree_type type = find_tree_type(space, key_def);
	switch (type) {
	case TREE_SPARSE:
		return new SparseTreeIndex(key_def, space);
	case TREE_DENSE:
		return new DenseTreeIndex(key_def, space);
	case TREE_NUM32:
		return new Num32TreeIndex(key_def, space);
	case TREE_FIXED:
		return new FixedTreeIndex(key_def, space);
	default:
		assert(false);
		return 0;
	}
}

TreeIndex::TreeIndex(struct key_def *key_def, struct space *space)
	: Index(key_def, space)
{
	memset(&tree, 0, sizeof tree);
}

TreeIndex::~TreeIndex()
{
	sptree_index_destroy(&tree);
}

size_t
TreeIndex::size() const
{
	return tree.size;
}

struct tuple *
TreeIndex::min() const
{
	void *node = sptree_index_first(&tree);
	return unfold(node);
}

struct tuple *
TreeIndex::max() const
{
	void *node = sptree_index_last(&tree);
	return unfold(node);
}

struct tuple *
TreeIndex::random(u32 rnd) const
{
	void *node = sptree_index_random(&tree, rnd);
	return unfold(node);
}

struct tuple *
TreeIndex::findByKey(const char *key, u32 part_count) const
{
	assert(key_def->is_unique);
	if (part_count != key_def->part_count) {
		tnt_raise(ClientError, ER_EXACT_MATCH,
			key_def->part_count, part_count);
	}

	struct key_data *key_data = (struct key_data *)
			alloca(sizeof(struct key_data) +
			 _SIZEOF_SPARSE_PARTS(part_count));

	key_data->data = key;
	key_data->part_count = part_count;
	fold_with_key_parts(key_def, key_data);

	void *node = sptree_index_find(&tree, key_data);
	return unfold(node);
}

struct tuple *
TreeIndex::findByTuple(struct tuple *tuple) const
{
	assert(key_def->is_unique);
	if (tuple->field_count < key_def->max_fieldno)
		tnt_raise(IllegalParams, "tuple must have all indexed fields");

	struct key_data *key_data = (struct key_data *)
			alloca(sizeof(struct key_data) +
			       _SIZEOF_SPARSE_PARTS(tuple->field_count));

	key_data->data = tuple->data;
	key_data->part_count = tuple->field_count;
	fold_with_sparse_parts(key_def, tuple, key_data->parts);

	void *node = sptree_index_find(&tree, key_data);
	return unfold(node);
}

struct tuple *
TreeIndex::replace(struct tuple *old_tuple, struct tuple *new_tuple,
		   enum dup_replace_mode mode)
{
	size_t node_size = this->node_size();
	void *new_node = alloca(node_size);
	void *old_node = alloca(node_size);
	uint32_t errcode;

	if (new_tuple) {
		void *dup_node = old_node;
		fold(new_node, new_tuple);

		/* Try to optimistically replace the new_tuple. */
		sptree_index_replace(&tree, new_node, &dup_node);

		struct tuple *dup_tuple = unfold(dup_node);
		errcode = replace_check_dup(old_tuple, dup_tuple, mode);

		if (errcode) {
			sptree_index_delete(&tree, new_node);
			if (dup_node)
				sptree_index_replace(&tree, dup_node, NULL);
			tnt_raise(ClientError, errcode, index_n(this));
		}
		if (dup_tuple)
			return dup_tuple;
	}
	if (old_tuple) {
		fold(old_node, old_tuple);
		sptree_index_delete(&tree, old_node);
	}
	return old_tuple;
}

struct iterator *
TreeIndex::allocIterator() const
{
	assert(key_def->part_count);
	struct tree_iterator *it = (struct tree_iterator *)
			malloc(sizeof(struct tree_iterator) +
			       SIZEOF_SPARSE_PARTS(key_def));

	if (it) {
		memset(it, 0, sizeof(struct tree_iterator));
		it->index = this;
		it->base.free = tree_iterator_free;
	}
	return (struct iterator *) it;
}

void
TreeIndex::initIterator(struct iterator *iterator, enum iterator_type type,
			const char *key, u32 part_count) const
{
	assert ( (part_count >= 1 && key != NULL) ||
		 (part_count == 0 && key == NULL));
	struct tree_iterator *it = tree_iterator(iterator);

	if (part_count == 0) {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = iterator_type_is_reverse(type) ? ITER_LE : ITER_GE;
		key = NULL;
	}
	it->key_data.data = key;
	it->key_data.part_count = part_count;

	fold_with_key_parts(key_def, &it->key_data);

	if (iterator_type_is_reverse(type))
		sptree_index_iterator_reverse_init_set(&tree, &it->iter,
						       &it->key_data);
	else
		sptree_index_iterator_init_set(&tree, &it->iter,
					       &it->key_data);

	switch (type) {
	case ITER_EQ:
		it->base.next = tree_iterator_eq;
		break;
	case ITER_REQ:
		it->base.next = tree_iterator_req;
		break;
	case ITER_ALL:
	case ITER_GE:
		it->base.next = tree_iterator_ge;
		break;
	case ITER_GT:
		it->base.next = tree_iterator_gt;
		break;
	case ITER_LE:
		it->base.next = tree_iterator_le;
		break;
	case ITER_LT:
		it->base.next = tree_iterator_lt;
		break;
	default:
		tnt_raise(ClientError, ER_UNSUPPORTED,
			  "Tree index", "requested iterator type");
	}
}

void
TreeIndex::beginBuild()
{
	assert(index_is_primary(this));

	tree.size = 0;
	tree.max_size = 64;

	size_t node_size = this->node_size();
	size_t sz = tree.max_size * node_size;
	tree.members = malloc(sz);
	if (tree.members == NULL) {
		panic("malloc(): failed to allocate %" PRI_SZ " bytes", sz);
	}
}

void
TreeIndex::buildNext(struct tuple *tuple)
{
	size_t node_size = this->node_size();

	if (tree.size == tree.max_size) {
		tree.max_size *= 2;

		size_t sz = tree.max_size * node_size;
		tree.members = realloc(tree.members, sz);
		if (tree.members == NULL) {
			panic("malloc(): failed to allocate %" PRI_SZ " bytes", sz);
		}
	}

	void *node = ((char *) tree.members + tree.size * node_size);
	fold(node, tuple);
	tree.size++;
}

void
TreeIndex::endBuild()
{
	assert(index_is_primary(this));

	u32 n_tuples = tree.size;
	u32 estimated_tuples = tree.max_size;
	void *nodes = tree.members;

	sptree_index_init(&tree,
			  node_size(), nodes, n_tuples, estimated_tuples,
			  key_node_cmp(), node_cmp(),
			  this);
}

void
TreeIndex::build(Index *pk)
{
	u32 n_tuples = pk->size();
	u32 estimated_tuples = n_tuples * 1.2;
	size_t node_size = this->node_size();

	void *nodes = NULL;
	if (n_tuples) {
		/*
		 * Allocate a little extra to avoid
		 * unnecessary realloc() when more data is
		 * inserted.
		*/
		size_t sz = estimated_tuples * node_size;
		nodes = malloc(sz);
		if (nodes == NULL) {
			panic("malloc(): failed to allocate %" PRI_SZ " bytes", sz);
		}
	}

	struct iterator *it = pk->position();
	pk->initIterator(it, ITER_ALL, NULL, 0);

	struct tuple *tuple;

	for (u32 i = 0; (tuple = it->next(it)) != NULL; ++i) {
		void *node = ((char *) nodes + i * node_size);
		fold(node, tuple);
	}

	if (n_tuples) {
		say_info("Sorting %" PRIu32 " keys in index %" PRIu32 "...", n_tuples,
			 index_n(this));
	}

	/* If n_tuples == 0 then estimated_tuples = 0, elem == NULL, tree is empty */
	sptree_index_init(&tree,
			  node_size, nodes, n_tuples, estimated_tuples,
			  key_node_cmp(),
			  key_def->is_unique ? node_cmp() : dup_node_cmp(),
			  this);
}

/* }}} */

/* {{{ SparseTreeIndex ********************************************/

static int
sparse_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	SparseTreeIndex *index = (SparseTreeIndex *) arg;
	const struct sparse_node *node_xa = (const struct sparse_node *) node_a;
	const struct sparse_node *node_xb = (const struct sparse_node *) node_b;
	return sparse_node_compare(index->key_def,
				   node_xa->tuple, node_xa->parts,
				   node_xb->tuple, node_xb->parts);
}

static int
sparse_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	int r = sparse_node_cmp(node_a, node_b, arg);
	if (r == 0) {
		const struct sparse_node *node_xa =
				(const struct sparse_node *) node_a;
		const struct sparse_node *node_xb =
				(const struct sparse_node *) node_b;
		r = ta_cmp(node_xa->tuple, node_xb->tuple);
	}
	return r;
}

static int
sparse_key_node_cmp(const void *key, const void *node, void *arg)
{
	SparseTreeIndex *index = (SparseTreeIndex *) arg;
	const struct key_data *key_data = (const struct key_data *) key;
	const struct sparse_node *node_x = (const struct sparse_node *) node;
	return sparse_key_node_compare(index->key_def, key_data,
				       node_x->tuple, node_x->parts);
}

SparseTreeIndex::SparseTreeIndex(struct key_def *key_def, struct space *space)
	: TreeIndex(key_def, space)
{
	/* Nothing */
}

size_t
SparseTreeIndex::node_size() const
{
	return sizeof(struct sparse_node) + SIZEOF_SPARSE_PARTS(key_def);
}


tree_cmp_t
SparseTreeIndex::node_cmp() const
{
	return sparse_node_cmp;
}

tree_cmp_t
SparseTreeIndex::dup_node_cmp() const
{
	return sparse_dup_node_cmp;
}

tree_cmp_t
SparseTreeIndex::key_node_cmp() const
{
	return sparse_key_node_cmp;
}

void
SparseTreeIndex::fold(void *node, struct tuple *tuple) const
{
	struct sparse_node *node_x = (struct sparse_node *) node;
	node_x->tuple = tuple;
	fold_with_sparse_parts(key_def, tuple, node_x->parts);
}

struct tuple *
SparseTreeIndex::unfold(const void *node) const
{
	const struct sparse_node *node_x = (const struct sparse_node *) node;
	return node_x ? node_x->tuple : NULL;
}

/* }}} */

/* {{{ DenseTreeIndex *********************************************/

static int
dense_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	DenseTreeIndex *index = (DenseTreeIndex *) arg;
	const struct dense_node *node_xa = (const struct dense_node *) node_a;
	const struct dense_node *node_xb = (const struct dense_node *) node_b;
	return dense_node_compare(index->key_def, index->first_field,
				  node_xa->tuple, node_xa->offset,
				  node_xb->tuple, node_xb->offset);
}

static int
dense_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	int r = dense_node_cmp(node_a, node_b, arg);
	if (r == 0) {
		const struct dense_node *node_xa =
				(const struct dense_node *) node_a;
		const struct dense_node *node_xb =
				(const struct dense_node *) node_b;
		r = ta_cmp(node_xa->tuple, node_xb->tuple);
	}
	return r;
}

static int
dense_key_node_cmp(const void *key, const void * node, void *arg)
{
	DenseTreeIndex *index = (DenseTreeIndex *) arg;
	const struct key_data *key_data = (const struct key_data *) key;
	const struct dense_node *node_x = (const struct dense_node *) node;
	return dense_key_node_compare(index->key_def, key_data,
				      index->first_field,
				      node_x->tuple, node_x->offset);
}

static int
linear_dense_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	DenseTreeIndex *index = (DenseTreeIndex *) arg;
	const struct dense_node *node_xa = (const struct dense_node *) node_a;
	const struct dense_node *node_xb = (const struct dense_node *) node_b;
	return linear_node_compare(index->key_def, index->first_field,
				   node_xa->tuple, node_xa->offset,
				   node_xb->tuple, node_xb->offset);
}

static int
linear_dense_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	int r = linear_dense_node_cmp(node_a, node_b, arg);
	if (r == 0) {
		const struct dense_node *node_xa =
				(const struct dense_node *) node_a;
		const struct dense_node *node_xb =
				(const struct dense_node *) node_b;
		r = ta_cmp(node_xa->tuple, node_xb->tuple);
	}
	return r;
}

static int
linear_dense_key_node_cmp(const void *key, const void * node, void *arg)
{
	DenseTreeIndex *index = (DenseTreeIndex *) arg;
	const struct key_data *key_data = (const struct key_data *) key;
	const struct dense_node *node_x = (const struct dense_node *) node;
	return linear_key_node_compare(index->key_def, key_data,
				       index->first_field,
				       node_x->tuple, node_x->offset);
}


DenseTreeIndex::DenseTreeIndex(struct key_def *key_def, struct space *space)
	: TreeIndex(key_def, space)
{
	first_field = find_first_field(key_def);
}

size_t
DenseTreeIndex::node_size() const
{
	return sizeof(struct dense_node);
}

tree_cmp_t
DenseTreeIndex::node_cmp() const
{
	return key_is_linear(key_def)
		? linear_dense_node_cmp
		: dense_node_cmp;
}

tree_cmp_t
DenseTreeIndex::dup_node_cmp() const
{
	return key_is_linear(key_def)
		? linear_dense_dup_node_cmp
		: dense_dup_node_cmp;
}

tree_cmp_t
DenseTreeIndex::key_node_cmp() const
{
	return key_is_linear(key_def)
		? linear_dense_key_node_cmp
		: dense_key_node_cmp;
}

void
DenseTreeIndex::fold(void *node, struct tuple *tuple) const
{
	struct dense_node *node_x = (struct dense_node *) node;
	node_x->tuple = tuple;
	node_x->offset = fold_with_dense_offset(key_def, tuple);
}

struct tuple *
DenseTreeIndex::unfold(const void *node) const
{
	const struct dense_node *node_x = (const struct dense_node *) node;
	return node_x ? node_x->tuple : NULL;
}

/* }}} */

/* {{{ Num32TreeIndex *********************************************/

static int
num32_node_cmp(const void * node_a, const void * node_b, void *arg)
{
	(void) arg;
	const struct num32_node *node_xa = (const struct num32_node *) node_a;
	const struct num32_node *node_xb = (const struct num32_node *) node_b;
	return u32_cmp(node_xa->value, node_xb->value);
}

static int
num32_dup_node_cmp(const void * node_a, const void * node_b, void *arg)
{
	int r = num32_node_cmp(node_a, node_b, arg);
	if (r == 0) {
		const struct num32_node *node_xa =
				(const struct num32_node *) node_a;
		const struct num32_node *node_xb =
				(const struct num32_node *) node_b;
		r = ta_cmp(node_xa->tuple, node_xb->tuple);
	}
	return r;
}

static int
num32_key_node_cmp(const void * key, const void * node, void *arg)
{
	(void) arg;
	const struct key_data *key_data = (const struct key_data *) key;
	const struct num32_node *node_x = (const struct num32_node *) node;
	if (key_data->part_count)
		return u32_cmp(key_data->parts[0].num32, node_x->value);
	return 0;
}

Num32TreeIndex::Num32TreeIndex(struct key_def *key_def, struct space *space)
	: TreeIndex(key_def, space)
{
	/* Nothing */
}

size_t
Num32TreeIndex::node_size() const
{
	return sizeof(struct num32_node);
}

tree_cmp_t
Num32TreeIndex::node_cmp() const
{
	return num32_node_cmp;
}

tree_cmp_t
Num32TreeIndex::dup_node_cmp() const
{
	return num32_dup_node_cmp;
}

tree_cmp_t
Num32TreeIndex::key_node_cmp() const
{
	return num32_key_node_cmp;
}

void
Num32TreeIndex::fold(void *node, struct tuple *tuple) const
{
	struct num32_node *node_x = (struct num32_node *) node;
	node_x->tuple = tuple;
	node_x->value = fold_with_num32_value(key_def, tuple);
}

struct tuple *
Num32TreeIndex::unfold(const void *node) const
{
	const struct num32_node *node_x = (const struct num32_node *) node;
	return node_x ? node_x->tuple : NULL;
}

/* }}} */

/* {{{ FixedTreeIndex *********************************************/

static int
fixed_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	FixedTreeIndex *index = (FixedTreeIndex *) arg;
	const struct fixed_node *node_xa = (const struct fixed_node *) node_a;
	const struct fixed_node *node_xb = (const struct fixed_node *) node_b;
	return dense_node_compare(index->key_def, index->first_field,
				  node_xa->tuple, index->first_offset,
				  node_xb->tuple, index->first_offset);
}

static int
fixed_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	int r = fixed_node_cmp(node_a, node_b, arg);
	if (r == 0) {
		const struct fixed_node *node_xa =
				(const struct fixed_node *) node_a;
		const struct fixed_node *node_xb =
				(const struct fixed_node *) node_b;
		r = ta_cmp(node_xa->tuple, node_xb->tuple);
	}
	return r;
}

static int
fixed_key_node_cmp(const void *key, const void * node, void *arg)
{
	FixedTreeIndex *index = (FixedTreeIndex *) arg;
	const struct key_data *key_data = (const struct key_data *) key;
	const struct fixed_node *node_x = (const struct fixed_node *) node;
	return dense_key_node_compare(index->key_def, key_data,
				      index->first_field,
				      node_x->tuple, index->first_offset);
}

static int
linear_fixed_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	FixedTreeIndex *index = (FixedTreeIndex *) arg;
	const struct fixed_node *node_xa = (const struct fixed_node *) node_a;
	const struct fixed_node *node_xb = (const struct fixed_node *) node_b;
	return linear_node_compare(index->key_def, index->first_field,
				   node_xa->tuple, index->first_offset,
				   node_xb->tuple, index->first_offset);
}

static int
linear_fixed_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	int r = linear_fixed_node_cmp(node_a, node_b, arg);
	if (r == 0) {
		const struct fixed_node *node_xa =
				(const struct fixed_node *) node_a;
		const struct fixed_node *node_xb =
				(const struct fixed_node *) node_b;
		r = ta_cmp(node_xa->tuple, node_xb->tuple);
	}
	return r;
}

static int
linear_fixed_key_node_cmp(const void *key, const void * node, void *arg)
{
	const FixedTreeIndex *index = (const FixedTreeIndex *) arg;
	const struct key_data *key_data = (const struct key_data *) key;
	const struct fixed_node *node_x = (const struct fixed_node *) node;
	return linear_key_node_compare(index->key_def, key_data,
					 index->first_field,
					 node_x->tuple, index->first_offset);
}


FixedTreeIndex::FixedTreeIndex(struct key_def *key_def, struct space *space)
	: TreeIndex(key_def, space)
{
	first_field = find_first_field(key_def);
	first_offset = find_fixed_offset(space, first_field, 0);
}


size_t
FixedTreeIndex::node_size() const
{
	return sizeof(struct fixed_node);
}

tree_cmp_t
FixedTreeIndex::node_cmp() const
{
	return key_is_linear(key_def)
		? linear_fixed_node_cmp
		: fixed_node_cmp;
}

tree_cmp_t
FixedTreeIndex::dup_node_cmp() const
{
	return key_is_linear(key_def)
		? linear_fixed_dup_node_cmp
		: fixed_dup_node_cmp;
}

tree_cmp_t
FixedTreeIndex::key_node_cmp() const
{
	return key_is_linear(key_def)
		? linear_fixed_key_node_cmp
		: fixed_key_node_cmp;
}

void
FixedTreeIndex::fold(void *node, struct tuple *tuple) const
{
	struct fixed_node *node_x = (struct fixed_node *) node;
	node_x->tuple = tuple;
}

struct tuple *
FixedTreeIndex::unfold(const void *node) const
{
	const struct fixed_node *node_x = (const struct fixed_node *) node;
	return node_x ? node_x->tuple : NULL;
}

/* }}} */
