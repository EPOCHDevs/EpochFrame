//
// Created by adesola on 1/21/25.
//

#pragma once
#include "tick.h"


namespace epochframe::datetime {
    template<typename Timedelta, size_t nanoSecondIncrement, DateTimeUnit unit>
    std::unique_ptr<OffsetHandler> TickHandler<Timedelta, nanoSecondIncrement, unit>::next_higher_resolution() const {
        if constexpr (std::is_same_v<T, days>) {
            return std::make_unique<Hour>(m_n*24);
        }
        else if constexpr (std::is_same_v<T, hours>) {
            return std::make_unique<Minute>(m_n*60);
        }
        else if constexpr (std::is_same_v<T, minutes>) {
            return std::make_unique<Second>(m_n*60);
        }
        else if constexpr (std::is_same_v<T, seconds>) {
            return std::make_unique<Milli>(m_n*1000);
        }
        else if constexpr (std::is_same_v<T, milliseconds>) {
            return std::make_unique<Micro>(m_n*1000);
        }
        else if constexpr (std::is_same_v<T, microseconds >) {
            return std::make_unique<Nano>(m_n*1000);
        }
        else {
            static_assert(false, "Invalid type");
        }
    }
}
