//
// Created by adesola on 1/22/25.
//

#include "base_offset_handler.h"


namespace epochframe::datetime {
    bool BaseOffsetHandler::is_on_offset(const Timestamp &dt) const {
        if (m_normalize && !is_normalized(dt)) {
            return false;
        }
        auto b = rsub(add(dt));
        return dt == b;
    }

}
