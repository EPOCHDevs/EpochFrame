//
// Created by adesola on 1/21/25.
//

#pragma once
#include <memory>
#include "../timestamps.h"

namespace epochframe::datetime {
    struct OffsetHandler {
        virtual ~OffsetHandler() = default;

        virtual std::shared_ptr<OffsetHandler> apply(std::shared_ptr<OffsetHandler> const &value) const = 0;

        virtual void _apply(std::shared_ptr<OffsetHandler> const &value) = 0;

        virtual bool equals(std::shared_ptr<OffsetHandler> const &value) = 0;

        virtual std::string prefix() const = 0;

        virtual time_duration offset() const = 0;

        virtual bool is_on_offset(Timestamp const &value) const = 0;

        virtual std::optional<int> startingMonth() const = 0;

        virtual std::optional<int> month() const = 0;
    };

    using OffsetHandlerPtr = std::shared_ptr<OffsetHandler>;
}
