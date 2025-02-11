//
// Created by adesola on 1/21/25.
//

#include "tick.h"


namespace epochframe::datetime {
    template<size_t nanoSecondIncrement>
    std::shared_ptr<OffsetHandler>
    TickHandler<nanoSecondIncrement>::next_higher_resolution() const {
        if constexpr (Day::nanos_inc == nanoSecondIncrement) {
            return std::make_shared<Hour>(n() * 24);
        } else if constexpr (Hour::nanos_inc == nanoSecondIncrement) {
            return std::make_shared<Minute>(n() * 60);
        } else if constexpr (Minute::nanos_inc == nanoSecondIncrement) {
            return std::make_shared<Second>(n() * 60);
        } else if constexpr (Second::nanos_inc == nanoSecondIncrement) {
            return std::make_shared<Milli>(n() * 1000);
        } else if constexpr (Milli::nanos_inc == nanoSecondIncrement) {
            return std::make_shared<Micro>(n() * 1000);
        } else if constexpr (Micro::nanos_inc == nanoSecondIncrement) {
            return std::make_shared<Nano>(n() * 1000);
        }
        throw std::runtime_error("Could not convert to integer offset at any resolution");
    }

    template<size_t nanoSecondIncrement>
    size_t TickHandler<nanoSecondIncrement>::hash() const {
        auto seed = BaseOffsetHandler::hash();
        boost::hash_combine(seed, nanoSecondIncrement);
        return seed;
    }

    template<size_t nanoSecondIncrement>
    std::shared_ptr<OffsetHandler> TickHandler<nanoSecondIncrement>::fmul(double other) const {
        auto _n = other * n();
        if (static_cast<int64_t>(_n) == _n) {
            return from_base(static_cast<int64_t>(_n), false);
        }
        return next_higher_resolution()->fmul(other);
    }

    template<size_t nanoSecondIncrement>
    std::shared_ptr<OffsetHandler>
    TickHandler<nanoSecondIncrement>::add(const std::shared_ptr<OffsetHandler> &other) const {
        auto otherPtr = std::dynamic_pointer_cast<TickHandlerBase>(other);
        if (!otherPtr) {
            return nullptr;
        }

        if (std::dynamic_pointer_cast<TickHandler<nanoSecondIncrement>>(other)) {
            return from_base(n() + other->n(), false);
        } else {
            return delta_to_tick(as_timedelta() + otherPtr->as_timedelta());
        }
    }

    template<size_t nanoSecondIncrement>
    Timestamp
    TickHandler<nanoSecondIncrement>::apply(const epochframe::datetime::Timestamp &value) const {
        return value + as_timedelta();
    }

    std::shared_ptr<OffsetHandler> delta_to_tick(Timedelta const &delta) {
        if (delta.microseconds() == 0 && delta.nanoseconds() == 0) {
            if (delta.seconds() == 0) {
                return std::make_shared<Day>(delta.days());
            } else {
                auto seconds = delta.days() * 86400 + delta.seconds();
                if (seconds % 3600 == 0) {
                    return std::make_shared<Hour>(seconds / 3600);
                } else if (seconds % 60 == 0) {
                    return std::make_shared<Minute>(seconds / 60);
                } else {
                    return std::make_shared<Second>(seconds);
                }
            }
        }
        else {
            auto nanos = delta_to_nanoseconds(delta);
            if (nanos % ONE_MILLION == 0) {
                return std::make_shared<Milli>(static_cast<int>(nanos / ONE_MILLION));
            }
            else if (nanos % 1000 == 0) {
                return std::make_shared<Micro>(static_cast<int>(nanos / 1000));
            }
            else {
                return std::make_shared<Nano>(static_cast<int>(nanos));
            }

        }
    }
}
