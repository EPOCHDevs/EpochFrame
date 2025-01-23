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
        explicit Offset(std::shared_ptr<OffsetHandler> handler):m_handler(std::move(handler)) {}

        // ------------------------------------------------------------------
        // Accessors

        std::int64_t getN() const { return m_n; }

        Offset base() const { return Offset{m_handler->base()}; }

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
            std::size_t operator()(const Offset &offset) const {
                return offset.m_handler->hash();
            }
        };

        Timestamp operator+(const Timestamp &other) const {
            return m_handler->apply(other);
        }

        Offset operator-(const Offset &other) const {
            return Offset{m_handler->sub(other.m_handler)};
        }

        void operator-(const Offset &other) {
            m_handler = m_handler->sub(other.m_handler);
        }

        Offset operator*(int64_t other) const {
            return Offset{m_handler->mul(other)};
        }

        Offset operator-() const {
            return (*this) * -1;
        }

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

        Timestamp rollback(Timestamp const &other) const;
        Timestamp rollforward(Timestamp const &other) const;

        bool is_on_offset(Timestamp const &other) const;

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
        std::string m_freqStr;

        bool get_start_end_field(Timestamp const& ts, std::string const& field) const;

    };

    inline Timestamp operator+(Timestamp const& dt, const Offset& offset) {
        return offset + dt;
    };
    inline Timestamp operator-(Timestamp const& dt, const Offset& offset) {
        return (-offset) + dt;
    };
    inline Offset operator*(int64_t dt, const Offset& offset) {
        return offset * dt;
    };
}  // namespace epochframe
