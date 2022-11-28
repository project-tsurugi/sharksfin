#pragma once

#include <ostream>
#include <stdexcept>

#include <boost/exception/enable_error_info.hpp>

#include "stacktrace.h"

// file originally created in  takatori::util

namespace sharksfin::shirakami {

/// @cond IMPL_DEFS
namespace impl {

void attach_trace(::boost::exception& exception, ::boost::stacktrace::stacktrace&& trace = {});

} // namespace impl
/// @endcond

/**
 * @brief throws the given exception.
 * @details this may attach stacktrace information to the given exception if it is enabled.
 *      Developers can obtain it by find_trace() in the corresponded catch block.
 * @tparam T the exception type
 * @param exception the exception to throw
 * @see find_trace()
 * @see print_trace()
 */
template<class T>
[[noreturn]] void throw_exception(T const& exception) {
    auto enhanced = ::boost::enable_error_info(exception);
    impl::attach_trace(enhanced);
    throw std::move(enhanced); // NOLINT(cert-err60-cpp)
}

/**
 * @brief obtains the stacktrace information in the given exception.
 * @param exception the target exception
 * @return the corresponded stacktrace information
 * @return empty if the exception has no stacktrace information
 * @see throw_exception()
 */
[[nodiscard]] ::boost::stacktrace::stacktrace const* find_trace(std::exception const& exception);

/**
 * @brief appends stacktrace information into the given output stream.
 * @details This will do nothing if the given exception has no stacktrace information.
 * @param out the destination output stream
 * @param exception the source exception
 * @see throw_exception()
 */
void print_trace(std::ostream& out, std::exception const& exception);

} // namespace sharksfin::shirakami
