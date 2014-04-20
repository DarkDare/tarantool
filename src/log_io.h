#ifndef TARANTOOL_LOG_IO_H_INCLUDED
#define TARANTOOL_LOG_IO_H_INCLUDED
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
#include <stdio.h>
#include <limits.h>
#include <stdbool.h>
#include <sys/uio.h>
#include "trivia/util.h"
#include "tarantool_ev.h"
#include "iproto_constants.h"

extern const uint32_t xlog_format;

enum log_format { WAL = 65534 };

enum log_mode {
	LOG_READ,
	LOG_WRITE
};

enum log_suffix { NONE, INPROGRESS };

struct log_dir {
	bool panic_if_error;
	/**
	 * true if the file can by fsync()ed at close
	 * in a separate thread.
	 */
	bool sync_is_async;

	/* Additional flags to apply at fopen(2) to write. */
	char open_wflags[6];
	const char *filetype;
	const char *filename_ext;
	char *dirname;
	/** File create mode in this directory. */
	mode_t mode;
};

extern struct log_dir snap_dir;
extern struct log_dir wal_dir;

int64_t
greatest_lsn(struct log_dir *dir);
char *
format_filename(struct log_dir *dir, int64_t lsn, enum log_suffix suffix);
int64_t
find_including_file(struct log_dir *dir, int64_t target_lsn);

struct log_io {
	struct log_dir *dir;
	FILE *f;

	enum log_mode mode;
	size_t rows;
	int retry;
	char filename[PATH_MAX + 1];

	bool is_inprogress;
};

struct log_io *
log_io_open_for_read(struct log_dir *dir, int64_t lsn, enum log_suffix suffix);
struct log_io *
log_io_open_for_write(struct log_dir *dir, int64_t lsn, enum log_suffix suffix);
struct log_io *
log_io_open(struct log_dir *dir, enum log_mode mode,
	    const char *filename, enum log_suffix suffix, FILE *file);
int
log_io_sync(struct log_io *l);
int
log_io_close(struct log_io **lptr);
void
log_io_atfork(struct log_io **lptr);

struct log_io_cursor
{
	struct log_io *log;
	int row_count;
	off_t good_offset;
	bool eof_read;
};

void
log_io_cursor_open(struct log_io_cursor *i, struct log_io *l);
void
log_io_cursor_close(struct log_io_cursor *i);

int
log_io_cursor_next(struct log_io_cursor *i, struct iproto_packet *packet, bool subsequent);
int
xlog_encode_row(const struct iproto_packet *packet, struct iovec *iov,
		char fixheader[XLOG_FIXHEADER_SIZE]);
enum { XLOG_ROW_IOVMAX = IPROTO_PACKET_IOVMAX + 1 };

typedef uint32_t log_magic_t;

int
inprogress_log_unlink(char *filename);
int
inprogress_log_rename(struct log_io *l);

#endif /* TARANTOOL_LOG_IO_H_INCLUDED */
