#include "Error.h"
#include <iostream>
#include "foedus/error_stack.hpp"
#include "sharksfin/StatusCode.h"

namespace sharksfin::foedus {

StatusCode resolve(::foedus::ErrorStack const& result) {
    if (!result.is_error()) {
        return StatusCode::OK;
    }
    std::cout << "Foedus error : " << result << std::endl;
    return StatusCode::ERR_UNKNOWN;
}

StatusCode resolve(::foedus::ErrorCode const& code) {
    if (code == ::foedus::kErrorCodeStrKeyNotFound) {
        return StatusCode::NOT_FOUND;
    }
    if (code != ::foedus::kErrorCodeOk) {
        return resolve(FOEDUS_ERROR_STACK(code));  //NOLINT
    }
    return StatusCode::OK;
}

}  // namespace sharksfin::foedus
