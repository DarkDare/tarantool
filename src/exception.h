#ifndef TARANTOOL_EXCEPTION_H_INCLUDED
#define TARANTOOL_EXCEPTION_H_INCLUDED
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
#include "object.h"
#include <stdarg.h>
#include "say.h"

enum { TNT_ERRMSG_MAX = 512 };

class Exception;
typedef Exception *DiagnosticArea;

class Exception: public Object {
protected:
	/** Allocated size. */
	size_t size;
public:
	void *operator new(size_t size);
	void operator delete(void*);
	virtual void raise()
	{
		/* Throw the most specific type of exception */
		throw this;
	}

	const char *type() const;

	const char *
	errmsg() const
	{
		return m_errmsg;
	}

	virtual void log() const;
	virtual ~Exception() {}

	void ref() {
		++m_ref;
	}

	void unref();

	/*
	 * DiagnosticArea methods
	 */

	static void init(DiagnosticArea *what)
	{
		*what = NULL;
	}
	/** Clear the last error saved in the current fiber's TLS */
	static inline void clear(DiagnosticArea *what)
	{
		if (*what != NULL)
			(*what)->unref();
		Exception::init(what);
	}
	/** Move an exception from one thread to another. */
	static void move(DiagnosticArea *from, DiagnosticArea *to)
	{
		Exception::clear(to);
		*to = *from;
		Exception::init(from);
	}

	static void add(DiagnosticArea *to, Exception *e)
	{
		Exception::clear(to);
		*to = e;
		e->ref();
	}
protected:
	Exception(const char *file, unsigned line);

	/* Ref counter */
	size_t m_ref;
	/* file name */
	const char *m_file;
	/* line number */
	unsigned m_line;

	/* error description */
	char m_errmsg[TNT_ERRMSG_MAX];
};

class SystemError: public Exception {
public:

	virtual void raise()
	{
		throw this;
	}

	int
	errnum() const
	{
		return m_errno;
	}

	virtual void log() const;

	SystemError(const char *file, unsigned line,
		    const char *format, ...);
protected:
	SystemError(const char *file, unsigned line);

	void
	init(const char *format, ...);

	void
	init(const char *format, va_list ap);

protected:
	/* system errno */
	int m_errno;
};

class OutOfMemory: public SystemError {
public:
	OutOfMemory(const char *file, unsigned line,
		    size_t amount, const char *allocator,
		    const char *object);
};

/*
 * A helper method for tnt_error() to avoid cyclic includes between
 * fiber.h and exception.h. Please don't use outside of this file.
 */
DiagnosticArea *fiber_get_diagnostic();

#define tnt_error(class, ...) ({					\
	say_debug("%s at %s:%i", #class, __FILE__, __LINE__);		\
	class *e = new class(__FILE__, __LINE__, ##__VA_ARGS__);	\
	DiagnosticArea *diag = fiber_get_diagnostic();			\
	Exception::add(diag, e);					\
	e;								\
})

#define tnt_raise(...) do {						\
	throw tnt_error(__VA_ARGS__);					\
} while (0)

#endif /* TARANTOOL_EXCEPTION_H_INCLUDED */
