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
#include "log_io.h"
#include <dirent.h>
#include <fcntl.h>

#include "fiber.h"
#include "crc32.h"
#include "fio.h"
#include "third_party/tarantool_eio.h"
#include "fiob.h"
#include "msgpuck/msgpuck.h"
#include "iproto_constants.h"
#include "scoped_guard.h"
#define MH_UNDEF 1 /* conflicts with mh_nodeids_t */
#include "recovery.h" /* for mh_cluster */
#include "vclock.h"

/*
 * marker is MsgPack fixext2
 * +--------+--------+--------+--------+
 * |  0xd5  |  type  |       data      |
 * +--------+--------+--------+--------+
 */
const log_magic_t row_marker = mp_bswap_u32(0xd5ba0bab); /* host byte order */
const log_magic_t eof_marker = mp_bswap_u32(0xd510aded); /* host byte order */
const char inprogress_suffix[] = ".inprogress";
const char v12[] = "0.12\n";

/* {{{ struct log_dir */

int
log_dir_create(struct log_dir *dir, const char *dirname,
	       enum log_dir_type type)
{
	memset(dir, 0, sizeof(*dir));
	vclockset_new(&dir->index);
	/* Default mode. */
	dir->mode = 0660;
	dir->dirname = strdup(dirname);
	if (type == SNAP) {
		strcpy(dir->open_wflags, "wxd");
		dir->filetype = "SNAP\n";
		dir->filename_ext = ".snap";
		dir->panic_if_error = true;
	} else {
		strcpy(dir->open_wflags, "wx");
		dir->sync_is_async = true;
		dir->filetype = "XLOG\n";
		dir->filename_ext = ".xlog";
	}
	return 0;
}

static void
vclockset_clean(vclockset_t *set) {
	struct vclock *cur = vclockset_first(set);
	while (cur != NULL) {
		struct vclock *next = vclockset_next(set, cur);
		vclockset_remove(set, cur);
		free(cur);
		cur = next;
	}
}

void
log_dir_destroy(struct log_dir *dir)
{
	free(dir->dirname);
	vclockset_clean(&dir->index);
}

static inline void
log_dir_add_to_index(struct log_dir *dir, int64_t signature)
{
	/*
	 * Open xlog and parse vclock
	 */
	struct log_io *wal = log_io_open_for_read(dir, signature, NULL,
						  INPROGRESS);
	if (wal == NULL)
		tnt_raise(ClientError, ER_INVALID_XLOG,
			  format_filename(dir, signature, NONE));
	auto log_guard = make_scoped_guard([&]{
		log_io_close(&wal);
	});

	/*
	 * Check filename
	 */
	int64_t signature_check = vclock_signature(&wal->vclock);
	if (signature_check != signature) {
		tnt_raise(ClientError, ER_INVALID_XLOG_NAME,
			  (long long) signature_check, (long long) signature);
	}

	/*
	 * Check ordering
	 */
	struct vclock *dup = vclockset_search(&dir->index, &wal->vclock);
	if (dup != NULL) {
		tnt_raise(ClientError, ER_INVALID_XLOG_ORDER,
			  (long long) signature,
			  (long long) vclock_signature(dup));
	}

	/*
	 * Update vclockset
	 */
	struct vclock *vclock = (struct vclock *) malloc(sizeof(*vclock));
	if (vclock == NULL) {
		tnt_raise(ClientError, ER_MEMORY_ISSUE,
			  sizeof(*vclock), "log_dir", "vclockset");
	}
	vclock_create(vclock);
	vclock_copy(vclock, &wal->vclock);
	vclockset_insert(&dir->index, vclock);
}

static int
cmp_i64(const void *_a, const void *_b)
{
	const int64_t *a = (const int64_t *) _a, *b = (const int64_t *) _b;
	if (*a == *b)
		return 0;
	return (*a > *b) ? 1 : -1;
}

int
log_dir_scan(struct log_dir *dir)
{
	ssize_t ext_len = strlen(dir->filename_ext);
	DIR *dh = opendir(dir->dirname);

	if (dh == NULL) {
		say_syserror("error reading directory `%s'", dir->dirname);
		return -1;
	}
	auto log_guard = make_scoped_guard([&]{
		closedir(dh);
	});

	int64_t *signts = NULL;
	size_t signts_capacity = 0, signts_size = 0;

	errno = 0;
	struct dirent *dent;
	while ((dent = readdir(dh)) != NULL) {
		char *ext = strchr(dent->d_name, '.');
		if (ext == NULL)
			continue;

		const char *suffix = strchr(ext + 1, '.');
		/*
		 * A valid ending is either .xlog or
		 * .xlog.inprogress, given dir->filename_ext ==
		 * 'xlog'.
		 */
		bool ext_is_ok;
		if (suffix == NULL)
			ext_is_ok = strcmp(ext, dir->filename_ext) == 0;
		else
			ext_is_ok = (strncmp(ext, dir->filename_ext,
					     ext_len) == 0 &&
				     strcmp(suffix, inprogress_suffix) == 0);
		if (!ext_is_ok)
			continue;

		long long signt = strtoll(dent->d_name, &ext, 10);
		if (strncmp(ext, dir->filename_ext, ext_len) != 0) {
			/* d_name doesn't parse entirely, ignore it */
			say_warn("can't parse `%s', skipping", dent->d_name);
			continue;
		}

		if (signt == LLONG_MAX || signt == LLONG_MIN) {
			say_warn("can't parse `%s', skipping", dent->d_name);
			continue;
		}

		if (signts_size == signts_capacity) {
			size_t capacity = signts_capacity > 0 ?
					2 * signts_capacity : 16;
			int64_t *new_signts = (int64_t *) region_alloc(
				&fiber()->gc, sizeof(*signts) * capacity);
			memcpy(new_signts, signts, sizeof(*signts) * signts_size);
			signts = new_signts;
			signts_capacity = capacity;
		}

		signts[signts_size++] = signt;
	}

	if (signts_size == 0) {
		/* Empty directory */
		vclockset_clean(&dir->index);
		return 0;
	}

	qsort(signts, signts_size, sizeof(*signts), cmp_i64);
	struct vclock *cur = vclockset_first(&dir->index);
	for (size_t i = 0; i < signts_size; i++) {
		while (cur != NULL) {
			int64_t signt = vclock_signature(cur);
			if (signt < signts[i]) {
				struct vclock *next =
					vclockset_next(&dir->index, cur);
				vclockset_remove(&dir->index, cur);
				free(cur);
				cur = next;
				continue;
			} else if (signt == signts[i]) {
				cur = vclockset_next(&dir->index, cur);
				goto skip; /* already exists */
			} else /* signt > lsns[i] */ {
				break;
			}
		}

		try {
			log_dir_add_to_index(dir, signts[i]);
		} catch (ClientError *e) {
			e->log();
			say_warn("failed to scan %s",
				 format_filename(dir, signts[i], NONE));
			if (dir->panic_if_error)
				throw;
		}
		skip: ;
	}

	return 0;
}

char *
format_filename(struct log_dir *dir, int64_t signt, enum log_suffix suffix)
{
	static __thread char filename[PATH_MAX + 1];
	const char *suffix_str = suffix == INPROGRESS ? inprogress_suffix : "";
	snprintf(filename, PATH_MAX, "%s/%020lld%s%s",
		 dir->dirname, (long long)signt, dir->filename_ext, suffix_str);
	return filename;
}

/* }}} */

/* {{{ struct log_io_cursor */

static int
row_reader(FILE *f, struct iproto_header *row)
{
	const char *data;

	/* Read fixed header */
	char fixheader[XLOG_FIXHEADER_SIZE - sizeof(log_magic_t)];
	if (fread(fixheader, sizeof(fixheader), 1, f) != 1) {
		if (feof(f))
			return 1;
error:
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "invalid fixed header");
	}

	/* Decode len, previous crc32 and row crc32 */
	data = fixheader;
	if (mp_check(&data, data + sizeof(fixheader)) != 0)
		goto error;
	data = fixheader;

	/* Read length */
	if (mp_typeof(*data) != MP_UINT)
		goto error;
	uint32_t len = mp_decode_uint(&data);
	if (len > IPROTO_BODY_LEN_MAX) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "received packet is too big");
	}

	/* Read previous crc32 */
	if (mp_typeof(*data) != MP_UINT)
		goto error;

	/* Read current crc32 */
	uint32_t crc32p = mp_decode_uint(&data);
	if (mp_typeof(*data) != MP_UINT)
		goto error;
	uint32_t crc32c = mp_decode_uint(&data);
	assert(data <= fixheader + sizeof(fixheader));
	(void) crc32p;
	row->flags = *data;

	/* Allocate memory for body */
	char *bodybuf = (char *) region_alloc(&fiber()->gc, len);

	/* Read header and body */
	if (fread(bodybuf, len, 1, f) != 1)
		return 1;

	/* Validate checksum */
	if (crc32_calc(0, bodybuf, len) != crc32c)
		tnt_raise(ClientError, ER_INVALID_MSGPACK, "invalid crc32");

	data = bodybuf;
	iproto_header_decode(row, &data, bodybuf + len);

	return 0;
}

int
xlog_encode_row(const struct iproto_header *row, struct iovec *iov)
{
	int iovcnt = iproto_header_encode(row, iov + 1) + 1;
	char *fixheader = (char *) region_alloc(&fiber()->gc,
						XLOG_FIXHEADER_SIZE);
	uint32_t len = 0;
	uint32_t crc32p = 0;
	uint32_t crc32c = 0;
	for (int i = 1; i < iovcnt; i++) {
		crc32c = crc32_calc(crc32c, (const char *) iov[i].iov_base,
				    iov[i].iov_len);
		len += iov[i].iov_len;
	}

	char *data = fixheader;
	*(log_magic_t *) data = row_marker;
	data += sizeof(row_marker);
	data = mp_encode_uint(data, len);
	/* Encode crc32 for previous row */
	data = mp_encode_uint(data, crc32p);
	/* Encode crc32 for current row */
	data = mp_encode_uint(data, crc32c);
	*data++ = (char)row->flags;
	/* Encode padding */
	ssize_t padding = XLOG_FIXHEADER_SIZE - (data - fixheader);
	if (padding > 0)
		data = mp_encode_strl(data, padding - 1) + padding - 1;
	assert(data == fixheader + XLOG_FIXHEADER_SIZE);
	iov[0].iov_base = fixheader;
	iov[0].iov_len = XLOG_FIXHEADER_SIZE;

	assert(iovcnt <= XLOG_ROW_IOVMAX);
	return iovcnt;
}

void
log_io_cursor_open(struct log_io_cursor *i, struct log_io *l)
{
	i->log = l;
	i->row_count = 0;
	i->good_offset = ftello(l->f);
	i->eof_read  = false;
}

void
log_io_cursor_close(struct log_io_cursor *i)
{
	struct log_io *l = i->log;
	l->rows += i->row_count;
	/*
	 * Since we don't close log_io
	 * we must rewind log_io to last known
	 * good position if there was an error.
	 * Seek back to last known good offset.
	 */
	fseeko(l->f, i->good_offset, SEEK_SET);
#if 0
	region_free(&fiber()->gc);
#endif
}

/**
 * Read logfile contents using designated format, panic if
 * the log is corrupted/unreadable.
 *
 * @param i	iterator object, encapsulating log specifics.
 *
 */
int
log_io_cursor_next(struct log_io_cursor *i, struct iproto_header *row, bool subsequent)
{
	struct log_io *l = i->log;
	log_magic_t magic;
	off_t marker_offset = 0;

	assert(i->eof_read == false);

	say_debug("log_io_cursor_next: marker:0x%016X/%zu",
		  row_marker, sizeof(row_marker));

#if 0
	/*
	 * Don't let gc pool grow too much. Yet to
	 * it before reading the next row, to make
	 * sure it's not freed along here.
	 */
	region_free_after(&fiber()->gc, 128 * 1024);
#endif

restart:
	if (marker_offset > 0)
		fseeko(l->f, marker_offset + 1, SEEK_SET);

	if (fread(&magic, sizeof(magic), 1, l->f) != 1)
		goto eof;

	while (magic != row_marker) {
		if (subsequent) {
			return 1;
		}
		int c = fgetc(l->f);
		if (c == EOF) {
			say_debug("eof while looking for magic");
			goto eof;
		}
		magic = magic >> 8 |
			((log_magic_t) c & 0xff) << (sizeof(magic)*8 - 8);
	}
	marker_offset = ftello(l->f) - sizeof(row_marker);
	if (i->good_offset != marker_offset) {
		if (subsequent) {
                        return 1;
                }
		say_warn("skipped %jd bytes after 0x%08jx offset",
                         (intmax_t)(marker_offset - i->good_offset),
                         (uintmax_t)i->good_offset);
        }
	say_debug("magic found at 0x%08jx", (uintmax_t)marker_offset);

	try {
		if (row_reader(l->f, row) != 0)
			goto eof;
	} catch (Exception *e) {
		if (l->dir->panic_if_error)
			panic("failed to read row");
		say_warn("failed to read row");
		if (subsequent) {
			return 1;
		}
		goto restart;
	}

	i->good_offset = ftello(l->f);
	i->row_count++;

	if (i->row_count % 100000 == 0)
		say_info("%.1fM rows processed", i->row_count / 1000000.);

	return 0;
eof:
	/*
	 * The only two cases of fully read file:
	 * 1. sizeof(eof_marker) > 0 and it is the last record in file
	 * 2. sizeof(eof_marker) == 0 and there is no unread data in file
	 */
	if (ftello(l->f) == i->good_offset + sizeof(eof_marker)) {
		fseeko(l->f, i->good_offset, SEEK_SET);
		if (fread(&magic, sizeof(magic), 1, l->f) != 1) {

			say_error("can't read eof marker");
		} else if (magic == eof_marker) {
			i->good_offset = ftello(l->f);
			i->eof_read = true;
		} else if (magic != row_marker) {
			say_error("eof marker is corrupt: %lu",
				  (unsigned long) magic);
		} else {
			/*
			 * Row marker at the end of a file: a sign
			 * of a corrupt log file in case of
			 * recovery, but OK in case we're in local
			 * hot standby or replication relay mode
			 * (i.e. data is being written to the
			 * file. Don't pollute the log, the
			 * condition is taken care of up the
			 * stack.
			 */
		}
	}
	/* No more rows. */
	return 1;
}

/* }}} */

int
inprogress_log_rename(struct log_io *l)
{
	char *filename = l->filename;
	char new_filename[PATH_MAX];
	char *suffix = strrchr(filename, '.');

	assert(l->is_inprogress);
	assert(suffix);
	assert(strcmp(suffix, inprogress_suffix) == 0);

	/* Create a new filename without '.inprogress' suffix. */
	memcpy(new_filename, filename, suffix - filename);
	new_filename[suffix - filename] = '\0';

	if (rename(filename, new_filename) != 0) {
		say_syserror("can't rename %s to %s", filename, new_filename);

		return -1;
	}
	l->is_inprogress = false;
	return 0;
}

int
inprogress_log_unlink(char *filename)
{
#ifndef NDEBUG
	char *suffix = strrchr(filename, '.');
	assert(suffix);
	assert(strcmp(suffix, inprogress_suffix) == 0);
#endif
	if (unlink(filename) != 0) {
		/* Don't panic if there is no such file. */
		if (errno == ENOENT)
			return 0;

		say_syserror("can't unlink %s", filename);

		return -1;
	}

	return 0;
}

/* {{{ struct log_io */

int
log_io_close(struct log_io **lptr)
{
	struct log_io *l = *lptr;
	int r;

	if (l->mode == LOG_WRITE) {
		fwrite(&eof_marker, 1, sizeof(log_magic_t), l->f);
		/*
		 * Sync the file before closing, since
		 * otherwise we can end up with a partially
		 * written file in case of a crash.
		 * Do not sync if the file is opened with O_SYNC.
		 */
		if (! strchr(l->dir->open_wflags, 's'))
			log_io_sync(l);
		if (l->is_inprogress && inprogress_log_rename(l) != 0)
			panic("can't rename 'inprogress' WAL");
	}

	r = fclose(l->f);
	if (r < 0)
		say_syserror("can't close");
	free(l);
	*lptr = NULL;
	return r;
}

/** Free log_io memory and destroy it cleanly, without side
 * effects (for use in the atfork handler).
 */
void
log_io_atfork(struct log_io **lptr)
{
	struct log_io *l = *lptr;
	if (l) {
		/*
		 * Close the file descriptor STDIO buffer does not
		 * make its way into the respective file in
		 * fclose().
		 */
		close(fileno(l->f));
		fclose(l->f);
		free(l);
		*lptr = NULL;
	}
}

static int
sync_cb(eio_req *req)
{
	if (req->result)
		say_error("%s: fsync failed, errno: %d",
			  __func__, (int) req->result);

	int fd = (intptr_t) req->data;
	close(fd);
	return 0;
}

int
log_io_sync(struct log_io *l)
{
	if (l->dir->sync_is_async) {
		int fd = dup(fileno(l->f));
		if (fd == -1) {
			say_syserror("%s: dup() failed", __func__);
			return -1;
		}
		eio_fsync(fd, 0, sync_cb, (void *) (intptr_t) fd);
	} else if (fsync(fileno(l->f)) < 0) {
		say_syserror("%s: fsync failed", l->filename);
		return -1;
	}
	return 0;
}

#define SERVER_UUID_KEY "Server"
#define VCLOCK_KEY "VClock"

static int
log_io_write_meta(struct log_io *l, const tt_uuid *server_uuid,
		  const struct vclock *vclock)
{
	char *vstr = NULL;
	if (fprintf(l->f, "%s%s", l->dir->filetype, v12) < 0 ||
	    fprintf(l->f, SERVER_UUID_KEY ": %s\n", tt_uuid_str(server_uuid)) < 0 ||
	    (vstr = vclock_to_string(vclock)) == NULL ||
	    fprintf(l->f, VCLOCK_KEY ": %s\n\n", vstr) < 0) {
		free(vstr);
		return -1;
	}

	return 0;
}

/**
 * Verify that file is of the given format.
 *
 * @param l		log_io object, denoting the file to check.
 * @param[out] errmsg   set if error
 *
 * @return 0 if success, -1 on error.
 */
static int
log_io_verify_meta(struct log_io *l, const tt_uuid *server_uuid)
{
	char filetype[32], version[32], buf[256];
	struct log_dir *dir = l->dir;
	FILE *stream = l->f;

	if (fgets(filetype, sizeof(filetype), stream) == NULL ||
	    fgets(version, sizeof(version), stream) == NULL) {
		say_error("%s: failed to read log file header", l->filename);
		return -1;
	}
	if (strcmp(dir->filetype, filetype) != 0) {
		say_error("%s: unknown filetype", l->filename);
		return -1;
	}

	if (strcmp(v12, version) != 0) {
		say_error("%s: unsupported file format version", l->filename);
		return -1;
	}
	for (;;) {
		if (fgets(buf, sizeof(buf), stream) == NULL) {
			say_error("%s: failed to read log file header",
				  l->filename);
			return -1;
		}
		if (strcmp(buf, "\n") == 0)
			break;

		/* Parse RFC822-like string */
		char *end = buf + strlen(buf);
		if (end > buf && *(end - 1) == '\n') *(--end) = 0; /* skip \n */
		char *key = buf;
		char *val = strchr(buf, ':');
		if (val == NULL) {
			say_error("%s: invalid meta", l->filename);
			return -1;
		}
		*(val++) = 0;
		while (*val == ' ') ++val; /* skip starting spaces */

		if (strcmp(key, SERVER_UUID_KEY) == 0) {
			if ((end - val) != UUID_STR_LEN ||
			    tt_uuid_from_string(val, &l->server_uuid) != 0) {
				say_error("%s: can't parse node uuid",
					  l->filename);
				return -1;
			}
		} else if (strcmp(key, VCLOCK_KEY) == 0){
			size_t offset = vclock_from_string(&l->vclock, val);
			if (offset != 0) {
				say_error("%s: invalid vclock at offset %zd",
					  l->filename, offset);
				return -1;
			}
		} else {
			/* Skip unknown key */
		}
	}

	if (server_uuid != NULL && !tt_uuid_is_nil(server_uuid) &&
	    tt_uuid_cmp(server_uuid, &l->server_uuid)) {
		say_error("%s: invalid server uuid", l->filename);
		return -1;
	}
	return 0;
}

struct log_io *
log_io_open_stream_for_read(struct log_dir *dir, const char *filename,
			    const tt_uuid *server_uuid, enum log_suffix suffix,
			    FILE *file)
{
	struct log_io *l = NULL;
	int save_errno;
	/*
	 * Check fopen() result the caller first thing, to
	 * preserve the errno.
	 */
	if (file == NULL) {
		save_errno = errno;
		say_syserror("%s: failed to open file", filename);
		goto error_1;
	}
	l = (struct log_io *) calloc(1, sizeof(*l));
	if (l == NULL) {
		save_errno = errno;
		say_syserror("%s: memory error", filename);
		goto error_2;
	}
	l->f = file;
	strncpy(l->filename, filename, PATH_MAX);
	l->mode = LOG_READ;
	l->dir = dir;
	l->is_inprogress = (suffix == INPROGRESS);
	vclock_create(&l->vclock);
	if (log_io_verify_meta(l, server_uuid) != 0) {
		save_errno = EINVAL;
		goto error_3;
	}
	return l;

error_3:
	free(l);
error_2:
	fclose(file);
error_1:
	errno = save_errno;
	return NULL;
}

struct log_io *
log_io_open_for_read(struct log_dir *dir, int64_t signature,
		     const tt_uuid *server_uuid, enum log_suffix suffix)
{
	const char *filename = format_filename(dir, signature, suffix);
	FILE *f = fopen(filename, "r");
	if (suffix == INPROGRESS && f == NULL) {
		filename = format_filename(dir, signature, NONE);
		f = fopen(filename, "r");
		suffix = NONE;
	}
	return log_io_open_stream_for_read(dir, filename, server_uuid, suffix, f);
}

/**
 * In case of error, writes a message to the server log
 * and sets errno.
 */
struct log_io *
log_io_open_for_write(struct log_dir *dir, const tt_uuid *server_uuid,
		      const struct vclock *vclock)
{
	char *filename;
	FILE *f = NULL;
	struct log_io *l = NULL;

	int64_t signt = vclock_signature(vclock);
	assert(signt >= 0);

	/*
	* Check whether a file with this name already exists.
	* We don't overwrite existing files.
	*/
	filename = format_filename(dir, signt, NONE);
	if (access(filename, F_OK) == 0) {
		errno = EEXIST;
		goto error;
	}

	/*
	 * Open the <lsn>.<suffix>.inprogress file. If it exists,
	 * open will fail.
	 */
	filename = format_filename(dir, signt, INPROGRESS);
	f = fiob_open(filename, dir->open_wflags);
	if (!f)
		goto error;
	say_info("creating `%s'", filename);
	l = (struct log_io *) calloc(1, sizeof(*l));
	if (l == NULL)
		goto error;
	l->f = f;
	strncpy(l->filename, filename, PATH_MAX);
	l->mode = LOG_WRITE;
	l->dir = dir;
	l->is_inprogress = true;
	setvbuf(l->f, NULL, _IONBF, 0);
	if (log_io_write_meta(l, server_uuid, vclock) != 0)
		goto error;

	return l;
error:
	int save_errno = errno;
	say_syserror("%s: failed to open", filename);
	if (f != NULL) {
		fclose(f);
		unlink(filename); /* try to remove incomplete file */
	}
	errno = save_errno;
	return NULL;
}

/* }}} */

