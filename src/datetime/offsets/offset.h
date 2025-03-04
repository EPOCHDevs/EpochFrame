#pragma once

#include <cstdint>    // for int64_t
#include <memory>     // for std::unique_ptr
#include <string>     // (optional, if you want toString-like functions)
#include "handler/offset_handler.h"
#include <epoch_lab_shared/common_utils.h>
#include "timestamp.h"


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
        explicit Offset(std::shared_ptr<OffsetHandler> handler) : m_handler(std::move(handler)) {
            AssertWithTraceFromStream(m_handler != nullptr, "OffsetHandler cannot be null");
        }

        // ------------------------------------------------------------------
        // Accessors

        std::int64_t n() const { return m_handler->n(); }

        // ------------------------------------------------------------------
        // Equality

        bool operator==(const Offset &other) const {
            return m_handler->eq(other.m_handler);
        }

        bool operator!=(const Offset &other) const {
            return m_handler->ne(other.m_handler);
        }

        Timestamp apply(const Timestamp &other) const {
            return m_handler->apply(other);
        }

        /**
         * Names and Code
         * */
        std::string name() const {
            return m_handler->name();
        }

        std::string rule_code() const {
            return m_handler->rule_code();
        }

        std::string repr() const {
            return m_handler->repr();
        }

        friend std::ostream &operator<<(std::ostream &os, const Offset &offset) {
            os << offset.m_handler->repr();
            return os;
        }

        Timestamp rollback(Timestamp const &other) const {
            return m_handler->rollback(other);
        }

        Timestamp rollforward(Timestamp const &other) const {
            return m_handler->rollforward(other);
        }

        bool is_on_offset(Timestamp const &other) const {
            return m_handler->is_on_offset(other);
        }

    protected:
        // ------------------------------------------------------------------
        // Data
        std::shared_ptr<OffsetHandler> m_handler;
    };
}  // namespace epochframe
