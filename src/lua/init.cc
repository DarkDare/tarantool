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
#include "lua/init.h"
#include "lua/utils.h"
#include "tarantool.h"
#include "box/box.h"
#include "tbuf.h"
#if defined(__FreeBSD__) || defined(__APPLE__)
#include "libgen.h"
#endif

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <lj_cdata.h>
} /* extern "C" */


#include <fiber.h>
#include <session.h>
#include "coeio.h"
#include "lua/fiber.h"
#include "lua/errinj.h"
#include "lua/ipc.h"
#include "lua/errno.h"
#include "lua/socket.h"
#include "lua/bsdsocket.h"
#include "lua/session.h"
#include "lua/cjson.h"
#include "lua/yaml.h"
#include "lua/msgpack.h"
#include <session.h>

#include <ctype.h>
#include "small/region.h"
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>

struct lua_State *tarantool_L;

/* contents of src/lua/ files */
extern char uuid_lua[], session_lua[], msgpackffi_lua[], fun_lua[],
       load_cfg_lua[], interactive_lua[], digest_lua[], init_lua[],
       log_lua[];
static struct region buf_reg;
static const char *lua_sources[] = { init_lua, session_lua, load_cfg_lua, NULL };
static const char *lua_modules[] = { "msgpackffi", msgpackffi_lua,
	"fun", fun_lua, "digest", digest_lua,
	"interactive", interactive_lua,
	"uuid", uuid_lua, "log", log_lua, NULL };

/*
 * {{{ box Lua library: common functions
 */

uint64_t
tarantool_lua_tointeger64(struct lua_State *L, int idx)
{
	uint64_t result = 0;

	switch (lua_type(L, idx)) {
	case LUA_TNUMBER:
		result = lua_tonumber(L, idx);
		break;
	case LUA_TSTRING:
	{
		const char *arg = luaL_checkstring(L, idx);
		char *arge;
		errno = 0;
		result = strtoull(arg, &arge, 10);
		if (errno != 0 || arge == arg)
			luaL_error(L, "lua_tointeger64: bad argument");
		break;
	}
	case LUA_TCDATA:
	{
		uint32_t ctypeid = 0;
		void *cdata = luaL_checkcdata(L, idx, &ctypeid);
		if (ctypeid != CTID_INT64 && ctypeid != CTID_UINT64) {
			luaL_error(L,
				   "lua_tointeger64: unsupported cdata type");
		}
		result = *(uint64_t*)cdata;
		break;
	}
	default:
		luaL_error(L, "lua_tointeger64: unsupported type: %s",
			   lua_typename(L, lua_type(L, idx)));
	}

	return result;
}

const char *
tarantool_lua_tostring(struct lua_State *L, int index)
{
	/* we need an absolute index */
	if (index < 0)
		index = lua_gettop(L) + index + 1;
	lua_getglobal(L, "tostring");
	lua_pushvalue(L, index);
	/* pops both "tostring" and its argument */
	lua_call(L, 1, 1);
	lua_replace(L, index);
	return lua_tostring(L, index);
}

/**
 * Convert lua number or string to lua cdata 64bit number.
 */
static int
lbox_tonumber64(struct lua_State *L)
{
	if (lua_gettop(L) != 1)
		luaL_error(L, "tonumber64: wrong number of arguments");
	uint64_t result = tarantool_lua_tointeger64(L, 1);
	return luaL_pushnumber64(L, result);
}

static int
lbox_coredump(struct lua_State *L __attribute__((unused)))
{
	coredump(60);
	lua_pushstring(L, "ok");
	return 1;
}

static const struct luaL_reg errorlib [] = {
	{NULL, NULL}
};

static void
tarantool_lua_error_init(struct lua_State *L) {
	luaL_register(L, "box.error", errorlib);
	for (int i = 0; i < tnt_error_codes_enum_MAX; i++) {
		const char *name = tnt_error_codes[i].errstr;
		if (strstr(name, "UNUSED") || strstr(name, "RESERVED"))
			continue;
		lua_pushnumber(L, i);
		lua_setfield(L, -2, name);
	}
	lua_pop(L, 1);
}

/* }}} */

/* {{{ package.path for require */

static void
tarantool_lua_setpath(struct lua_State *L, const char *type, ...)
__attribute__((sentinel));

/**
 * Prepend the variable list of arguments to the Lua
 * package search path (or cpath, as defined in 'type').
 */
static void
tarantool_lua_setpath(struct lua_State *L, const char *type, ...)
{
	char path[PATH_MAX];
	va_list args;
	va_start(args, type);
	int off = 0;
	const char *p;
	while ((p = va_arg(args, const char*))) {
		/*
		 * If MODULE_PATH is an empty string, skip it.
		 */
		if (*p == '\0')
			continue;
		off += snprintf(path + off, sizeof(path) - off, "%s;", p);
	}
	va_end(args);
	lua_getglobal(L, "package");
	lua_getfield(L, -1, type);
	snprintf(path + off, sizeof(path) - off, "%s",
	         lua_tostring(L, -1));
	lua_pop(L, 1);
	lua_pushstring(L, path);
	lua_setfield(L, -2, type);
	lua_pop(L, 1);
}

void
tarantool_lua_init(const char *tarantool_bin, int argc, char **argv)
{
	lua_State *L = luaL_newstate();
	if (L == NULL) {
		panic("failed to initialize Lua");
	}
	luaL_openlibs(L);
	/*
	 * Search for Lua modules, apart from the standard
	 * locations in the system-wide Tarantool paths. This way
	 * 2 types of packages become available for use: standard
	 * Lua packages and Tarantool-specific Lua libs
	 */
	tarantool_lua_setpath(L, "path", MODULE_LUAPATH, NULL);
	tarantool_lua_setpath(L, "cpath", MODULE_LIBPATH, NULL);

	lua_register(L, "tonumber64", lbox_tonumber64);
	lua_register(L, "coredump", lbox_coredump);

	tarantool_lua_errinj_init(L);
	tarantool_lua_fiber_init(L);
	tarantool_lua_cjson_init(L);
	tarantool_lua_yaml_init(L);
	tarantool_lua_ipc_init(L);
	tarantool_lua_errno_init(L);
	tarantool_lua_socket_init(L);
	tarantool_lua_bsdsocket_init(L);
	tarantool_lua_session_init(L);
	tarantool_lua_error_init(L);
	luaopen_msgpack(L);
	lua_pop(L, 1);

	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	for (const char **s = lua_modules; *s; s += 2) {
		const char *modname = *s;
		const char *modsrc = *(s + 1);
		const char *modfile = lua_pushfstring(L,
			"@builtin/%s.lua", modname);
		if (luaL_loadbuffer(L, modsrc, strlen(modsrc), modfile))
			panic("Error loading Lua module %s...: %s",
			      modname, lua_tostring(L, -1));
		lua_call(L, 0, 1);
		lua_setfield(L, -3, modname); /* package.loaded.modname = t */
		lua_pop(L, 1); /* chunkname */
	}
	lua_pop(L, 1); /* _LOADED */

	/* Load Lua extension */
	for (const char **s = lua_sources; *s; s++) {
		if (luaL_dostring(L, *s))
			panic("Error loading Lua source %.160s...: %s",
			      *s, lua_tostring(L, -1));
	}

	box_lua_init(L);


	lua_newtable(L);
	lua_pushinteger(L, -1);
	lua_pushstring(L, tarantool_bin);
	lua_settable(L, -3);
	for (int i = 0; i < argc; i++) {
		lua_pushinteger(L, i);
		lua_pushstring(L, argv[i]);
		lua_settable(L, -3);
	}
	lua_setfield(L, LUA_GLOBALSINDEX, "arg");

	/* clear possible left-overs of init */
	lua_settop(L, 0);
	tarantool_L = L;
}

/**
 * Attempt to append 'return ' before the chunk: if the chunk is
 * an expression, this pushes results of the expression onto the
 * stack. If the chunk is a statement, it won't compile. In that
 * case try to run the original string.
 */
static int
tarantool_lua_dostring(struct lua_State *L, const char *str)
{
	struct tbuf *buf = tbuf_new(&fiber()->gc);
	tbuf_printf(buf, "%s%s", "return ", str);
	int r = luaL_loadstring(L, tbuf_str(buf));
	if (r) {
		/* pop the error message */
		lua_pop(L, 1);
		r = luaL_loadstring(L, str);
		if (r)
			return r;
	}
	try {
		lbox_call(L, 0, LUA_MULTRET);
	} catch (FiberCancelException *e) {
		throw;
	} catch (Exception *e) {
		lua_settop(L, 0);
		lua_pushstring(L, e->errmsg());
		return 1;
	}
	return 0;
}

extern "C" {
	int yamlL_encode(lua_State*);
};


static void
tarantool_lua_do(struct lua_State *L, struct tbuf *out, const char *str)
{
	int r = tarantool_lua_dostring(L, str);
	if (r) {
		assert(lua_gettop(L) == 1);
		const char *msg = lua_tostring(L, -1);
		msg = msg ? msg : "";
		lua_newtable(L);
		lua_pushstring(L, "error");
		lua_pushstring(L, msg);
		lua_settable(L, -3);
		lua_replace(L, 1);
		assert(lua_gettop(L) == 1);
	}
	/* Convert Lua stack to YAML and append to the given tbuf */
	int top = lua_gettop(L);
	if (top == 0) {
		tbuf_printf(out, "---\n...\n");
		lua_settop(L, 0);
		return;
	}

	lua_newtable(L);
	for (int i = 1; i <= top; i++) {
		lua_pushnumber(L, i);
		if (lua_isnil(L, i)) {
			/**
			 * When storing a nil in a Lua table,
			 * there is no way to distinguish nil
			 * value from no value. This is a trick
			 * to make sure yaml converter correctly
			 * outputs nil values on the return stack.
			 */
			lua_pushlightuserdata(L, NULL);
		} else {
			lua_pushvalue(L, i);
		}
		lua_rawset(L, -3);
	}
	lua_replace(L, 1);
	lua_settop(L, 1);

	yamlL_encode(L);
	lua_replace(L, 1);
	lua_pop(L, 1);
	tbuf_printf(out, "%s", lua_tostring(L, 1));
	lua_settop(L, 0);
}

void
tarantool_lua(struct lua_State *L,
	      struct tbuf *out, const char *str)
{
	try {
		tarantool_lua_do(L, out, str);
	} catch (...) {
		const char *err = lua_tostring(L, -1);
		tbuf_printf(out, "---\n- error: %s\n...\n", err);
		lua_settop(L, 0);
	}
}

char *history = NULL;

ssize_t
readline_cb(va_list ap)
{
	const char **line = va_arg(ap, const char **);
	*line = readline("tarantool> ");
	return 0;
}

extern "C" void
tarantool_lua_interactive()
{
	char *line;
        region_create(&buf_reg, &cord()->slabc);
	while (true) {
		coeio_custom(readline_cb, TIMEOUT_INFINITY, &line);
		if (line == NULL)
			break;
		struct tbuf *out = tbuf_new(&buf_reg);
		struct lua_State *L = lua_newthread(tarantool_L);
		tarantool_lua(L, out, line);
		lua_pop(tarantool_L, 1);
		printf("%.*s", out->size, out->data);
		region_reset(&buf_reg);
		if (history)
			add_history(line);
		free(line);
	}
        region_destroy(&buf_reg);
}

extern "C" const char *
tarantool_error_message(void)
{
	assert(cord()->exception != NULL); /* called only from error handler */
	return cord()->exception->errmsg();
}

/**
 * Execute start-up script.
 */
static void
run_script(va_list ap)
{
	struct lua_State *L = va_arg(ap, struct lua_State *);
	const char *path = va_arg(ap, const char *);

	/*
	 * Return control to tarantool_lua_run_script.
	 * tarantool_lua_run_script then will start an auxiliary event
	 * loop and re-schedule this fiber.
	 */
	fiber_sleep(0.0);

	/* Create session with ADMIN privileges for interactive mode */
	SessionGuard session_guard(0, 0);

	if (access(path, F_OK) == 0) {
		/* Execute the init file. */
		lua_getglobal(L, "dofile");
		lua_pushstring(L, path);
	} else {
		say_crit("version %s", tarantool_version());
		/* get iteractive from package.loaded */
		lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
		lua_getfield(L, -1, "interactive");
		lua_remove(L, -2); /* remove package.loaded */
	}
	try {
		lbox_call(L, lua_gettop(L) - 1, 0);
	} catch (ClientError *e) {
		panic("%s", e->errmsg());
	}

	/* clear the stack from return values. */
	lua_settop(L, 0);
	/*
	 * The file doesn't exist. It's OK, tarantool may
	 * have no init file.
	 */

	/*
	 * Lua script finished. Stop the auxiliary event loop and
	 * return control back to tarantool_lua_run_script.
	 */
	ev_break(loop(), EVBREAK_ALL);
}

void
tarantool_lua_run_script(char *path)
{
	const char *title = path ? basename(path) : "interactive";
	/*
	 * init script can call box.fiber.yield (including implicitly via
	 * box.insert, box.update, etc...), but box.fiber.yield() today,
	 * when called from 'sched' fiber crashes the server.
	 * To work this problem around we must run init script in
	 * a separate fiber.
	 */
	struct fiber *loader = fiber_new(title, run_script);
	fiber_call(loader, tarantool_L, path);

	/*
	 * Run an auxiliary event loop to re-schedule run_script fiber.
	 * When this fiber finishes, it will call ev_break to stop the loop.
	 */
	ev_run(loop(), 0);
}

void
tarantool_lua_free()
{
	/*
	 * Got to be done prior to anything else, since GC
	 * handlers can refer to other subsystems (e.g. fibers).
	 */
	if (tarantool_L) {
		/* collects garbage, invoking userdata gc */
		lua_close(tarantool_L);
	}
	tarantool_L = NULL;
}

