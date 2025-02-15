//
// Created by adesola on 2/15/25.
//

#pragma once
#include "date_offsets/date_offsets.h"


namespace epochframe::factory::offset {

    DateOffsetHandlerPtr nanos(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<NanoHandler>(n, tz);
    }

    DateOffsetHandlerPtr micro(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<MicroHandler>(n, tz);
    }

    DateOffsetHandlerPtr millis(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<MilliHandler>(n, tz);
    }

    DateOffsetHandlerPtr seconds(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<SecondHandler>(n, tz);
    }

    DateOffsetHandlerPtr minutes(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<MinuteHandler>(n, tz);
    }

    DateOffsetHandlerPtr hours(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<HourHandler>(n, tz);
    }

    DateOffsetHandlerPtr days(int64_t n, const std::optional<Timezone> &tz = std::nullopt) {
        return std::make_shared<DayHandler>(n, tz);
    }
}
