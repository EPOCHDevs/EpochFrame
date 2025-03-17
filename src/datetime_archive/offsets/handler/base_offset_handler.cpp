//
// Created by adesola on 1/22/25.
//

#include "base_offset_handler.h"
#include <epoch_lab_shared/common_utils.h>
#include "../calendar.h"

namespace epochframe::datetime {
    bool BaseOffsetHandler::eq(const std::shared_ptr<OffsetHandler> &value) const {
        auto other = std::dynamic_pointer_cast<BaseOffsetHandler>(value);
        if (other == nullptr) {
            return false;
        }
        return m_n == other->m_n && m_normalize == other->m_normalize;
    }


    std::shared_ptr<OffsetHandler> BaseOffsetHandler::sub(const std::shared_ptr<OffsetHandler> &value) const {
        auto other = std::dynamic_pointer_cast<BaseOffsetHandler>(value);
        if (other == nullptr) {
            return nullptr;
        }
        return from_base(m_n - other->m_n, m_normalize);
    }

    std::string BaseOffsetHandler::repr() const {
        return fmt::format("<{}{}{}{}>", m_n == 1 ? "" : fmt::format("{} * ", m_n),
                           class_name(), abs(m_n) != 1 ? "s" : "", repr_attrs());
    }

    std::string BaseOffsetHandler::freqstr() const {
        auto code = rule_code();

        std::string fstr = (m_n != 1) ? fmt::format("{}{}", m_n, code) : code;

        if (offset()) {
            fstr += offset_str();
        }
        return fstr;
    }

    bool BaseOffsetHandler::is_on_offset(const Timestamp &dt) const {
        if (m_normalize && !is_normalized(dt)) {
            return false;
        }
        auto b = rsub(radd(dt));
        return dt == b;
    }

    int get_day_of_month(Timestamp const &_date, EpochDateTimeDayOption const &option) {
        switch (option) {
            case EpochDateTimeDayOption::Start:
                return 1;
            case EpochDateTimeDayOption::End:
                return get_days_in_month(_date.year(), _date.month());
            case EpochDateTimeDayOption::BusinessStart:
                return get_firstbday(_date.year(), _date.month());
            case EpochDateTimeDayOption::BusinessEnd:
                return get_lastbday(_date.year(), _date.month());
            default:
                break;
        }
        throw std::runtime_error("ValueError: Invalid day option");
    }

}
