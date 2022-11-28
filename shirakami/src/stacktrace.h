#pragma once

// file originally created in  takatori::util

#if defined(BOOST_STACKTRACE_USE_BACKTRACE) \
        && !defined(DBOOST_STACKTRACE_BACKTRACE_INCLUDE_FILE) \
        && !__has_include(<backtrace.h>)

// NOTE: clang-tidy cannot detect <backtrace.h>
#undef BOOST_STACKTRACE_USE_BACKTRACE

#endif

#include <boost/stacktrace/stacktrace.hpp>
