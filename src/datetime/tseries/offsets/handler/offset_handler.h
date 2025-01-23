//
// Created by adesola on 1/21/25.
//

#pragma once
#include <memory>
#include "../timestamps.h"
#include "datetime/tseries/offsets/offset.h"

namespace epochframe::datetime {
    struct OffsetHandler {
        virtual ~OffsetHandler() = default;

        virtual bool should_normalized() const = 0;

        virtual bool eq(std::shared_ptr<OffsetHandler>  const &value) const = 0;
        virtual bool le(std::shared_ptr<OffsetHandler>  const &value) const = 0;
        virtual bool lt(std::shared_ptr<OffsetHandler>  const &value) const = 0;
        virtual bool gq(std::shared_ptr<OffsetHandler>  const &value) const = 0;
        virtual bool gt(std::shared_ptr<OffsetHandler>  const &value) const = 0;

        virtual size_t hash() const = 0;
        virtual time_duration timedelta() const = 0;
        virtual Timestamp add(Timestamp const&) const = 0;
        virtual std::shared_ptr<OffsetHandler> sub(std::shared_ptr<OffsetHandler> const &other) const = 0;
        virtual std::shared_ptr<OffsetHandler> rsub(std::shared_ptr<OffsetHandler> const &other) const = 0;
        virtual std::shared_ptr<OffsetHandler> mul(int64_t) const = 0;

        virtual std::shared_ptr<OffsetHandler> base() const = 0;

        virtual Timestamp apply(Timestamp const &value) const = 0;

        virtual std::string prefix() const = 0;

        virtual std::optional<int> startingMonth() const = 0;

        virtual std::optional<int> month() const = 0;

        virtual int64_t nanos() const = 0;

        virtual bool is_on_offset(Timestamp const &value) const = 0;
        virtual Timestamp rollback(Timestamp const &other) const = 0;
        virtual Timestamp rollforward(Timestamp const &other) const = 0;

        virtual std::shared_ptr<OffsetHandler> make(int ) const = 0;
    };

    using OffsetHandlerPtr = std::shared_ptr<OffsetHandler>;
}
