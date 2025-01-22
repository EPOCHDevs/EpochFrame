#pragma once

#include <cstdint>    // for int64_t
#include <memory>     // for std::unique_ptr
#include <string>     // (optional, if you want toString-like functions)
#include "common_utils/exceptions.h"
#include "handler/offset_handler.h"


namespace epochframe::datetime {

/**
 * An abstract base class loosely mirroring Pandas' "BaseOffset" concept,
 * but stripped down for C++ usage.
 */
    class Offset {
    public:
        /**
         * Constructor with n (frequency multiple) and normalize flag.
         */
        Offset(std::shared_ptr<OffsetHandler> handler, std::int64_t n = 1, bool normalize = false);

        // ------------------------------------------------------------------
        // Accessors

        std::int64_t getN() const { return m_n; }

        bool isNormalize() const { return m_normalize; }

        // ------------------------------------------------------------------
        // Equality

        bool operator==(const Offset &other) const {
            return m_handler->equals(other.m_handler) && (m_n == other.m_n) && (m_normalize == other.m_normalize);
        }

        bool operator!=(const Offset &other) const {
            return !(*this == other);
        }

        // ------------------------------------------------------------------
        // Hashing

        /**
         * A simple hash struct that can be used in unordered sets/maps if
         * you store references or pointers to BaseOffset. You can override
         * or expand in derived classes if more fields are introduced.
         */
        struct Hash {
            std::size_t operator()(const Offset &offset) const;
        };

        Offset operator+(const Offset &other) const {
            return {m_handler->apply(other.m_handler)};
        }

        void operator+(const Offset &other) {
            m_handler->_apply(other.m_handler);
        }

        Offset operator-(const Offset &other) const {
            return {m_handler, m_n - other.m_n, other.m_normalize};
        }

        void operator-(const Offset &other) {
            m_n -= other.m_n;
        }

        Offset operator-() const {
            return -1 * (*this);
        }

        Offset operator-() const {
            return (*this) * -1;
        }

        Offset operator*() const {
            return (*this) * -1;
        }

//        def __rsub__(self, other):
//        return (-self).__add__(other)

        /**
         * Names and Code
         * */
        std::string name() const {
            return this->rule_code();
        }

        std::string rule_code() const {
            return m_handler->prefix();
        }

        std::string to_string() const;

        bool is_on_offset(Timestamp const &other) const;

        Timestamp rollback(Timestamp const &other) const;

        Timestamp rollforward(Timestamp const &other) const;

        int64_t nanos(Timestamp const &other) const {
            return m_handler->offset().total_nanoseconds() * m_n;
        }

        bool is_month_start(Timestamp const& ts) const {
            return get_start_end_field(ts, "is_month_start");
        }

        bool is_month_end(Timestamp const& ts) const {
            return get_start_end_field(ts, "is_month_end");
        }

        bool is_quarter_start(Timestamp const& ts) const {
            return get_start_end_field(ts, "is_quarter_start");
        }

        bool is_quarter_end(Timestamp const& ts) const {
            return get_start_end_field(ts, "is_quarter_end");
        }

        bool is_year_start(Timestamp const& ts) const {
            return get_start_end_field(ts, "is_year_start");
        }

        bool is_year_end(Timestamp const& ts) const {
            return get_start_end_field(ts, "is_year_end");
        }

    protected:
        // ------------------------------------------------------------------
        // Data
        std::shared_ptr<OffsetHandler> m_handler;
        std::int64_t m_n;      ///< The integer multiple of this offset.
        bool m_normalize;      ///< Whether to normalize to a boundary (e.g., midnight).
        const std::string m_freqStr;

        bool get_start_end_field(Timestamp const& ts, std::string const& field) const;
    };

}  // namespace epochframe
