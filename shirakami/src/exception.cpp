#include "exception.h"

#include <ostream>

#include <boost/exception/error_info.hpp>
#include <boost/exception/get_error_info.hpp>
#include <boost/exception/info_tuple.hpp>

// file originally created in  takatori::util

namespace sharksfin::shirakami {

/// @brief extended error information about stacktrace.
using stacktrace_info = ::boost::error_info<struct tag_stacktrace, boost::stacktrace::stacktrace>;

void impl::attach_trace(boost::exception& exception, ::boost::stacktrace::stacktrace&& trace) {
    exception << stacktrace_info { std::move(trace) };
}

::boost::stacktrace::stacktrace const* find_trace(std::exception const& exception) {
    return ::boost::get_error_info<stacktrace_info>(exception);
}

void print_trace(std::ostream& out, std::exception const& exception) {
    if (auto const* st = find_trace(exception); st != nullptr) {
        out << *st;
    }
}
} // namespace sharksfin::shirakami
