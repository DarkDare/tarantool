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
#include "exception.h"
#include "say.h"
#include "fiber.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <typeinfo>

/** out_of_memory::size is zero-initialized by the linker. */
static OutOfMemory out_of_memory(__FILE__, __LINE__,
				 sizeof(OutOfMemory), "malloc", "exception");

void *
Exception::operator new(size_t size)
{
	struct fiber *fiber = fiber();

	if (fiber->exception == &out_of_memory) {
		Exception::clear(&fiber->exception);
	} else if (fiber->exception) {
		assert(fiber->exception->m_ref > 0);
		if (fiber->exception->m_ref == 1 &&
		    fiber->exception->size >= size) {
			/*
			 * Exception has only one reference - the fiber itself.
			 */

			Exception *e = NULL;
			Exception::move(&fiber->exception, &e);
			/* Explicitly call destructor for previous exception */
			e->~Exception();
			e->m_ref = 0;
			/* e is now a memory chunk, not Exception object */
			return (void *) e;
		} else {
			/* Can't reuse exception */
			Exception::clear(&fiber->exception);
		}
	}
	assert(fiber->exception == NULL);
	Exception *e = (Exception *) malloc(size);
	if (e) {
		e->m_ref = 0;
		e->size = size;
		return e;
	}
	fiber->exception = &out_of_memory;
	fiber->exception->ref();
	throw fiber->exception;
}

void
Exception::operator delete(void * /* ptr */)
{
	/*
	 * Direct calling of `delete' is prohibited because Exception
	 * implement reference counting. Please use ref()/unref() and
	 * your object will be destroyed as soon as everybody stop use it.
	 *
	 * However, C++ can call `delete' implicitly on an exception from
	 * constructor. Throwing exceptions from Exception constructor is
	 * very weird idea. Just don't do that.
	 */
	assert(false);
}

DiagnosticArea *
fiber_get_diagnostic()
{
	return &fiber()->exception;
}

void
Exception::unref() {
	assert(m_ref > 0);
	--m_ref;
	if (m_ref == 0) {
		/* Equvivalent to `delete this' */
		this->~Exception();
		free(this);
	}
}

Exception::Exception(const char *file, unsigned line)
	:  m_ref(0), m_file(file), m_line(line) {
	m_errmsg[0] = 0;
	if (this == &out_of_memory) {
		/* A special workaround for out_of_memory static init */
		out_of_memory.m_ref = 1;
		return;
	}
}

const char *
Exception::type() const
{
	const char *name = typeid(*this).name();
	/** A quick & dirty version of name demangle for class names */
	char *res = NULL;
	(void) strtol(name, &res, 10);
	return res && strlen(res) ? res : name;
}

void
Exception::log() const
{
	_say(S_ERROR, m_file, m_line, m_errmsg, "%s", type());
}


SystemError::SystemError(const char *file, unsigned line)
	: Exception(file, line),
	  m_errno(errno)
{
	/* nothing */
}

SystemError::SystemError(const char *file, unsigned line,
			 const char *format, ...)
	:Exception(file, line),
	m_errno(errno)
{
	va_list ap;
	va_start(ap, format);
	init(format, ap);
	va_end(ap);
}

void
SystemError::init(const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	init(format, ap);
	va_end(ap);
}

void
SystemError::init(const char *format, va_list ap)
{
	vsnprintf(m_errmsg, sizeof(m_errmsg), format, ap);
}

void
SystemError::log() const
{
	_say(S_SYSERROR, m_file, m_line, strerror(m_errno), "SystemError %s",
	     m_errmsg);
}

OutOfMemory::OutOfMemory(const char *file, unsigned line,
			 size_t amount, const char *allocator,
			 const char *object)
	:SystemError(file, line)
{
	m_errno = ENOMEM;
	snprintf(m_errmsg, sizeof(m_errmsg),
		 "Failed to allocate %u bytes in %s for %s",
		 (unsigned) amount, allocator, object);
}
