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
#include "iproto_constants.h"
#include "msgpuck/msgpuck.h"
#include "exception.h"
#include "fiber.h"
#include "crc32.h"

const unsigned char iproto_key_type[IPROTO_KEY_MAX] =
{
	/* {{{ header */
		/* 0x00 */	MP_UINT,   /* IPROTO_REQUEST_TYPE */
		/* 0x01 */	MP_UINT,   /* IPROTO_SYNC */
		/* 0x02 */	MP_UINT,   /* IPROTO_NODE_ID */
		/* 0x03 */	MP_UINT,   /* IPROTO_LSN */
		/* 0x04 */	MP_DOUBLE, /* IPROTO_TIMESTAMP */
	/* }}} */

	/* {{{ unused */
		/* 0x05 */	MP_UINT,
		/* 0x06 */	MP_UINT,
		/* 0x07 */	MP_UINT,
		/* 0x08 */	MP_UINT,
		/* 0x09 */	MP_UINT,
		/* 0x0a */	MP_UINT,
		/* 0x0b */	MP_UINT,
		/* 0x0c */	MP_UINT,
		/* 0x0d */	MP_UINT,
		/* 0x0e */	MP_UINT,
		/* 0x0f */	MP_UINT,
	/* }}} */

	/* {{{ body -- integer keys */
		/* 0x10 */	MP_UINT, /* IPROTO_SPACE_ID */
		/* 0x11 */	MP_UINT, /* IPROTO_INDEX_ID */
		/* 0x12 */	MP_UINT, /* IPROTO_LIMIT */
		/* 0x13 */	MP_UINT, /* IPROTO_OFFSET */
		/* 0x14 */	MP_UINT, /* IPROTO_ITERATOR */
	/* }}} */

	/* {{{ unused */
		/* 0x15 */	MP_UINT,
		/* 0x16 */	MP_UINT,
		/* 0x17 */	MP_UINT,
		/* 0x18 */	MP_UINT,
		/* 0x19 */	MP_UINT,
		/* 0x1a */	MP_UINT,
		/* 0x1b */	MP_UINT,
		/* 0x1c */	MP_UINT,
		/* 0x1d */	MP_UINT,
		/* 0x1e */	MP_UINT,
		/* 0x1f */	MP_UINT,
	/* }}} */

	/* {{{ body -- all keys */
	/* 0x20 */	MP_ARRAY, /* IPROTO_KEY */
	/* 0x21 */	MP_ARRAY, /* IPROTO_TUPLE */
	/* 0x22 */	MP_STR, /* IPROTO_FUNCTION_NAME */
	/* 0x23 */	MP_STR, /* IPROTO_USER_NAME */
	/* 0x24 */	MP_STR, /* IPROTO_NODE_UUID */
	/* 0x25 */	MP_STR, /* IPROTO_CLUSTER_UUID */
	/* 0x26 */	MP_MAP, /* IPROTO_LSNMAP */
	/* }}} */
};

const char *iproto_request_type_strs[] =
{
	NULL,
	"SELECT",
	"INSERT",
	"REPLACE",
	"UPDATE",
	"DELETE",
	"CALL",
	"AUTH"
};

#define bit(c) (1ULL<<IPROTO_##c)
const uint64_t iproto_body_key_map[IPROTO_DML_REQUEST_MAX + 1] = {
	0,                                                     /* unused */
	bit(SPACE_ID) | bit(LIMIT) | bit(KEY),                 /* SELECT */
	bit(SPACE_ID) | bit(TUPLE),                            /* INSERT */
	bit(SPACE_ID) | bit(TUPLE),                            /* REPLACE */
	bit(SPACE_ID) | bit(KEY) | bit(TUPLE),                 /* UPDATE */
	bit(SPACE_ID) | bit(KEY),                              /* DELETE */
	bit(FUNCTION_NAME) | bit(TUPLE),                       /* CALL */
	bit(USER_NAME) | bit(TUPLE)                            /* AUTH */
};
#undef bit

const char *iproto_key_strs[IPROTO_KEY_MAX] = {
	"type",             /* 0x00 */
	"sync",             /* 0x01 */
	"node_id",          /* 0x02 */
	"lsn",              /* 0x03 */
	"timestamp",        /* 0x04 */
	"",                 /* 0x05 */
	"",                 /* 0x06 */
	"",                 /* 0x07 */
	"",                 /* 0x08 */
	"",                 /* 0x09 */
	"",                 /* 0x0a */
	"",                 /* 0x0b */
	"",                 /* 0x0c */
	"",                 /* 0x0d */
	"",                 /* 0x0e */
	"",                 /* 0x0f */
	"space_id",         /* 0x10 */
	"index_id",         /* 0x11 */
	"limit",            /* 0x12 */
	"offset",           /* 0x13 */
	"iterator",         /* 0x14 */
	"",                 /* 0x15 */
	"",                 /* 0x16 */
	"",                 /* 0x17 */
	"",                 /* 0x18 */
	"",                 /* 0x19 */
	"",                 /* 0x1a */
	"",                 /* 0x1b */
	"",                 /* 0x1c */
	"",                 /* 0x1d */
	"",                 /* 0x1e */
	"",                 /* 0x1f */
	"key",              /* 0x20 */
	"tuple",            /* 0x21 */
	"function name",    /* 0x22 */
	"user name",        /* 0x23 */
	"node uuid"         /* 0x24 */
	"cluster uuid"      /* 0x25 */
	"lsn map"           /* 0x26 */
};

void
iproto_header_decode(struct iproto_header *header, const char **pos,
		     const char *end)
{
	memset(header, 0, sizeof(struct iproto_header));
	const char *pos2 = *pos;
	if (mp_check(&pos2, end) != 0) {
error:
		tnt_raise(ClientError, ER_INVALID_MSGPACK, "packet header");
	}

	if (mp_typeof(**pos) != MP_MAP)
		goto error;

	uint32_t size = mp_decode_map(pos);
	for (uint32_t i = 0; i < size; i++) {
		if (mp_typeof(**pos) != MP_UINT)
			goto error;
		unsigned char key = mp_decode_uint(pos);
		if (iproto_key_type[key] != mp_typeof(**pos))
			goto error;
		switch (key) {
		case IPROTO_REQUEST_TYPE:
			header->type = mp_decode_uint(pos);
			break;
		case IPROTO_SYNC:
			header->sync = mp_decode_uint(pos);
			break;
		case IPROTO_NODE_ID:
			header->node_id = mp_decode_uint(pos);
			break;
		case IPROTO_LSN:
			header->lsn = mp_decode_uint(pos);
			break;
		case IPROTO_TIMESTAMP:
			header->tm = mp_decode_double(pos);
			break;
		default:
			/* unknown header */
			mp_next(pos);
		}
	}
	assert(*pos <= end);
	if (*pos < end) {
		header->bodycnt = 1;
		header->body[0].iov_base = (void *) *pos;
		header->body[0].iov_len = (end - *pos);
		*pos = end;
	}
}

int
iproto_header_encode(const struct iproto_header *header, struct iovec *out)
{
	enum { HEADER_LEN_MAX = 40 };

	/* allocate memory for sign + header */
	char *data = (char *) region_alloc(&fiber()->gc, HEADER_LEN_MAX);

	/* Header */
	char *d = data + 1; /* Skip 1 byte for MP_MAP */
	int map_size = 0;
	if (true) {
		d = mp_encode_uint(d, IPROTO_REQUEST_TYPE);
		d = mp_encode_uint(d, header->type);
		map_size++;
	}

	if (header->sync) {
		d = mp_encode_uint(d, IPROTO_SYNC);
		d = mp_encode_uint(d, header->sync);
		map_size++;
	}

	if (header->node_id) {
		d = mp_encode_uint(d, IPROTO_NODE_ID);
		d = mp_encode_uint(d, header->node_id);
		map_size++;
	}

	if (header->lsn) {
		d = mp_encode_uint(d, IPROTO_LSN);
		d = mp_encode_uint(d, header->lsn);
		map_size++;
	}

	if (header->tm) {
		d = mp_encode_uint(d, IPROTO_TIMESTAMP);
		d = mp_encode_double(d, header->tm);
		map_size++;
	}

	assert(d <= data + HEADER_LEN_MAX);
	mp_encode_map(data, map_size);
	out->iov_base = data;
	out->iov_len = (d - data);
	out++;

	memcpy(out, header->body, sizeof(*out) * header->bodycnt);
	assert(1 + header->bodycnt <= IPROTO_PACKET_IOVMAX);
	return 1 + header->bodycnt; /* new iovcnt */
}

int
iproto_row_encode(const struct iproto_header *row,
		  struct iovec *out, char fixheader[IPROTO_FIXHEADER_SIZE])
{
	int iovcnt = iproto_header_encode(row, out + 1) + 1;
	uint32_t len = 0;
	for (int i = 1; i < iovcnt; i++)
		len += out[i].iov_len;

	/* Encode length */
	char *data = fixheader;
	data = mp_encode_uint(data, len);
	/* Encode padding */
	ssize_t padding = IPROTO_FIXHEADER_SIZE - (data - fixheader);
	if (padding > 0) {
		data = mp_encode_strl(data, padding - 1);
#if defined(NDEBUG)
		data += padding - 1;
#else
		while (--padding > 0)
			*(data++) = 0; /* valgrind */
#endif
	}
	assert(data == fixheader + IPROTO_FIXHEADER_SIZE);
	out[0].iov_base = fixheader;
	out[0].iov_len = IPROTO_FIXHEADER_SIZE;

	assert(iovcnt <= IPROTO_ROW_IOVMAX);
	return iovcnt;
}
