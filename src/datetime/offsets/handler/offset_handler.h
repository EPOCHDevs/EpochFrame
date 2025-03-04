//
// Created by adesola on 1/21/25.
//

#pragma once
#include <memory>
#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/date_time/gregorian/gregorian.hpp"
#include <optional>
#include <variant>
#include "../timedelta.h"


using namespace boost::posix_time;
using namespace boost::gregorian;

namespace epochframe::datetime {
    typedef boost::date_time::subsecond_duration<time_duration, 1000000000> nanosec;
    typedef boost::date_time::subsecond_duration<time_duration, 1000000000> nanoseconds;

    struct OffsetHandler {
        virtual ~OffsetHandler() = default;

        virtual bool should_normalized() const = 0;

        virtual int64_t n() const = 0;

        virtual bool eq(std::shared_ptr<OffsetHandler> const &value) const = 0;

        [[nodiscard]] virtual bool ne(std::shared_ptr<OffsetHandler> const &value) const {
            return not eq(value);
        }

        virtual bool le(std::shared_ptr<OffsetHandler> const &value) const = 0;

        virtual bool lt(std::shared_ptr<OffsetHandler> const &value) const = 0;

        virtual bool ge(std::shared_ptr<OffsetHandler> const &value) const = 0;

        virtual bool gt(std::shared_ptr<OffsetHandler> const &value) const = 0;

        virtual size_t hash() const = 0;

        virtual std::shared_ptr<OffsetHandler> add(std::shared_ptr<OffsetHandler> const &) const = 0;

        virtual class Timestamp add(Timestamp const &) const = 0;

        virtual Timestamp radd(Timestamp const &) const = 0;

        virtual std::shared_ptr<OffsetHandler> sub(std::shared_ptr<OffsetHandler> const &other) const = 0;

        virtual Timestamp rsub(Timestamp const &other) const = 0;

        virtual std::shared_ptr<OffsetHandler> mul(int64_t) const = 0;

        virtual std::shared_ptr<OffsetHandler> fmul(double) const = 0;

        virtual std::shared_ptr<OffsetHandler> div(int64_t) const = 0;

        virtual std::shared_ptr<OffsetHandler> rmul(int64_t) const = 0;

        virtual std::shared_ptr<OffsetHandler> negate() const = 0;

        virtual std::shared_ptr<OffsetHandler> base() const = 0;

        virtual std::shared_ptr<OffsetHandler> copy() const = 0;

        virtual std::string class_name() const = 0;

        virtual std::string repr() const = 0;

        virtual std::string repr_attrs() const = 0;

        virtual Timestamp apply(Timestamp const &value) const = 0;

        virtual std::string name() const = 0;

        virtual std::string prefix() const = 0;

        virtual std::string rule_code() const = 0;

        virtual std::string freqstr() const = 0;

        virtual std::optional<Timedelta> offset() const = 0;

        virtual std::string offset_str() const = 0;

        virtual int starting_month() const = 0;

        virtual int month() const = 0;

        virtual int64_t nanos() const = 0;

        virtual bool is_on_offset(Timestamp const &value) const = 0;

        virtual Timestamp rollback(Timestamp const &other) const = 0;

        virtual Timestamp rollforward(Timestamp const &other) const = 0;

        virtual bool is_month_start(const Timestamp &) const = 0;

        virtual bool is_month_end(const Timestamp &) const = 0;

        virtual bool is_quarter_start(const Timestamp &) const = 0;

        virtual bool is_quarter_end(const Timestamp &) const = 0;

        virtual bool is_year_start(const Timestamp &) const = 0;

        virtual bool is_year_end(const Timestamp &) const = 0;
    };

    using OffsetHandlerPtr = std::shared_ptr<OffsetHandler>;
}
