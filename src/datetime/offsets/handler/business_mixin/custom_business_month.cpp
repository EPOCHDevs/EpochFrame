//
// Created by adesola on 2/9/25.
//

#include "custom_business_month.h"
#include "custom_business_day.h"


namespace epochframe::datetime {
    CustomBusinessMonthHandler::CustomBusinessMonthHandler(BusinessMixinOption option, std::string prefix) : BusinessMixinHandler(option) {
        m_prefix = std::move(prefix);
        m_monthRoll = month_roll();
        m_cBDayRoll = cbday_roll();
        m_internalOffset = make_offset();
    }

    OffsetHandlerPtr CustomBusinessMonthHandler::make_offset() const {
        RelativeDeltaOffsetHandlerOption option{1, false};
        return prefix().ends_with("S") ? std::make_shared<MonthBegin>(option) : std::make_shared<MonthEnd>(option);
    }

    Timestamp CustomBusinessMonthHandler::apply(const epochframe::datetime::Timestamp &other) const {
        auto cu_month_offset_date = m_monthRoll(other);
        auto compare_date = m_cBDayRoll(cu_month_offset_date);
        auto n_ = roll_convention(other.day(), n(), compare_date.day());
        auto result = cu_month_offset_date + m_internalOffset->rmul(n_);
        if (m_offset)
            return result + (*m_offset);
        return result;
    }

    std::function<Timestamp(Timestamp const&)> CustomBusinessMonthHandler::cbday_roll() const {
        CustomBusinessDayHandler cbday{
                BusinessMixinOption{
                        {.n = 1,
                                .normalize = false,
                                .offset = TimeDelta{nanoseconds(0)}}}
        };
        return prefix().ends_with("S") ?
               [=](const epochframe::datetime::Timestamp &value) {
                   return cbday.rollforward(value);
               } :
               [=](const epochframe::datetime::Timestamp &value) {
                   return cbday.rollback(value);
               };
    }

    std::function<Timestamp(Timestamp const&)> CustomBusinessMonthHandler::month_roll() const {
        AssertWithTraceFromStream(m_offset, "month_roll requires valid offset");
        return prefix().ends_with("S") ?
               [=](const epochframe::datetime::Timestamp &value) {
                   return m_offset->rollforward(value);
               } :
               [=](const epochframe::datetime::Timestamp &value) {
                   return m_offset->rollback(value);
               };
    }
}
