//
// Created by adesola on 1/21/25.
//

#include "offset.h"
#include <stdexcept>
#include <sstream>

#include <epoch_lab_shared/macros.h>




namespace epochframe::datetime {

// -------------------
// Offset methods
// -------------------

    Offset::Offset(std::shared_ptr<OffsetHandler> handler,
                   std::int64_t n, bool normalize)
            : m_handler(std::move(handler)), m_n(n), m_normalize(normalize) {
        AssertWithTraceFromFormat(n > 0, "n must be positive");
        auto fStr = m_n == 1 ? rule_code() : fmt::format("{}{}", m_n, rule_code());

        if (!m_handler->offset().is_special()) {
            fStr = fmt::format("{}{}", m_n, to_simple_string(m_handler->offset()));
        }
    }


    bool Offset::is_on_offset(Timestamp const &dt) const {
        if (m_normalize) {
            return dt == ptime(dt.date());
        }
        return m_handler->is_on_offset(dt);
    }

    Timestamp Offset::rollback(Timestamp const &ts) const {

    }

    Timestamp Offset::rollforward(Timestamp const &ts) const {

    }

    std::string Offset::to_string() const {
        // Mimic the __repr__ style:
        // e.g. <BaseOffset: n=2, normalize=true>
        std::ostringstream oss;
        oss << "<BaseOffset: n=" << m_n
            << ", normalize=" << (m_normalize ? "true" : "false") << ">";
        return oss.str();
    }

    bool Offset::get_start_end_field(Timestamp const &ts, std::string const &field) const {
        return ::epochframe::datetime::get_start_end_field(std::vector{ts.value()}, field, name(),
                                                           m_handler->startingMonth().value_or(
                                                                   m_handler->month().value_or(12))).front();
    }
}
