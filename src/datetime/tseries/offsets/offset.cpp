//
// Created by adesola on 1/21/25.
//

#include "offset.h"
#include "datetime/tseries/offsets/handler/base_offset_handler.h"
#include <stdexcept>
#include <sstream>

#include <epoch_lab_shared/macros.h>




namespace epochframe::datetime {

// -------------------
// Offset methods
// -------------------

    bool Offset::is_on_offset(Timestamp const &dt) const {
        if (m_handler->should_normalized() && !is_normalized(dt)) {
            return false;
        }
        return m_handler->is_on_offset(dt);
    }

    Timestamp Offset::rollback(Timestamp const &dt) const {
        if (! is_on_offset(dt)) {
            return dt - base();
        }
        return dt;
    }

    Timestamp Offset::rollforward(Timestamp const & dt) const {
        if (! is_on_offset(dt)) {
            return dt + base();
        }
        return dt;
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
