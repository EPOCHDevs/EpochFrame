//
// Created by adesola on 2/9/25.
//

#include "business_mixin.h"


namespace epochframe::datetime {
    std::string BusinessMixinHandler::repr_attrs() const {
        if (m_offset)
        {
            std::stringstream ss;
            ss << ": offset=" << *m_offset;
            return ss.str();
        }
        return "";
    }
}
