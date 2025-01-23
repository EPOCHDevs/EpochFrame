//
// Created by adesola on 1/22/25.
//

#pragma once
#include "tick.h"


namespace epochframe::datetime {
    class DateOffset : public OffsetHandler{
    public:
        bool is_on_offset(const epochframe::datetime::Timestamp &) const override {
            return false;
        }
    };

    class BusinessMixin : public OffsetHandler{

    };

    class BusinessDay : public BusinessMixin{
    public:
        bool is_on_offset(const epochframe::datetime::Timestamp &value) const override {
            return value.weekday() < 5;
        }
    };

    class BusinessHour : public BusinessMixin{
    public:
        bool is_on_offset(const epochframe::datetime::Timestamp &value) const override {
            if (m_n >= 0)
            {

            }
        }
    };
}
