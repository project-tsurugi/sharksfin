/*
 * Copyright 2018-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef SHARKSFIN_SCANOPTIONS_H_
#define SHARKSFIN_SCANOPTIONS_H_

#include <cstddef>
#include <iostream>

namespace sharksfin {

/**
 * @brief represents options for content scan.
 */
class ScanOptions final {
public:
    ///@brief type for the max number of entries to be fetched
    using limit_type = std::size_t;

    ///@brief constant for unlimited scan
    static constexpr limit_type unlimited = 0;

    /**
     * @brief create scan option with default values
     */
    ScanOptions() = default;

    /**
     * @brief accessor for the max number of entries to be fetched
     * @return the max number of entries, or ScanOptions::unlimited if there is no limit
     */
    [[nodiscard]] limit_type limit() const noexcept {
        return limit_;
    }

    /**
     * @brief setter for the max number of entries to be fetched
     * @details this is effective only if legacy_scan is true; it is ignored otherwise.
     * @param arg the max number of entries, or ScanOptions::unlimited to indicate no limit
     * @return *this
     */
    ScanOptions& limit(limit_type arg) noexcept {
        limit_ = arg;
        return *this;
    }

    /**
     * @brief accessor for the reverse flag
     * @return true if the iterator scans in reverse order (from end to begin)
     * @return false otherwise
     */
    [[nodiscard]] bool reverse() const noexcept {
        return reverse_;
    }

    /**
     * @brief setter for the reverse flag
     * @param arg whether or not the iterator scans in reverse order (from end to begin).
     * Current limitation on reverse = true is that end_kind must be EndPointKind::UNBOUND and at most one entry is
     * fetched as the scan result
     * @return *this
     */
    ScanOptions& reverse(bool arg) noexcept {
        reverse_ = arg;
        return *this;
    }

    /**
     * @brief accessor for the legacy scan flag
     * @return true if the legacy scan is used
     * @return false if the iterator based scan (iscan) is used
     */
    [[nodiscard]] bool legacy_scan() const noexcept {
        return legacy_scan_;
    }

    /**
     * @brief setter for the legacy scan flag
     * @details this is a hint for the transaction engine to choose the scan implementation.
     * The transaction engine which doesn't have multiple scan implementations ignores this flag.
     * Defaults to true.
     * Note that limit is effective only if this flag is true.
     * @param arg true to use the legacy scan, false to use the iterator based scan (iscan)
     * @return *this
     */
    ScanOptions& legacy_scan(bool arg) noexcept {
        legacy_scan_ = arg;
        return *this;
    }

private:
    limit_type limit_{ unlimited };
    bool reverse_{ false };
    bool legacy_scan_{ true };
};

/**
 * @brief appends string representation of the given value.
 * @param out the target stream
 * @param value the source value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, ScanOptions const& value) {
    return out << "ScanOptions("
               << "limit=" << value.limit() << ", "
               << "reverse=" << std::boolalpha << value.reverse() << ", "
               << "legacy_scan=" << value.legacy_scan() << std::noboolalpha
               << ")";
}

}  // namespace sharksfin

#endif  // SHARKSFIN_SCANOPTIONS_H_
