#pragma once
#include <epoch_core/enum_wrapper.h>
#include <optional>

CREATE_ENUM(EpochDayOfWeek, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday);

namespace epoch_frame
{
    class Weekday
    {
      public:
        Weekday(epoch_core::EpochDayOfWeek day_of_week, std::optional<int> n = std::nullopt)
            : day_of_week(day_of_week), m_n(n)
        {
        }

        Weekday operator()(int n) const
        {
            return n == this->m_n ? *this : Weekday(day_of_week, n);
        }

        bool operator==(Weekday const& other) const
        {
            return day_of_week == other.day_of_week && m_n == other.m_n;
        }

        bool operator!=(Weekday const& other) const
        {
            return !(*this == other);
        }

        std::string repr() const
        {
            constexpr std::array<const char*, 7> day_of_week_names = {"MO", "TU", "WE", "TH",
                                                                      "FR", "SA", "SU"};
            auto s = day_of_week_names[static_cast<int>(day_of_week)];
            if (!m_n.has_value())
            {
                return s;
            }
            return std::format("{}({})", s, m_n.value());
        }

        friend std::ostream& operator<<(std::ostream& os, Weekday const& weekday)
        {
            return os << weekday.repr();
        }

        epoch_core::EpochDayOfWeek weekday() const
        {
            return day_of_week;
        }

        std::optional<int> n() const
        {
            return m_n;
        }

      private:
        epoch_core::EpochDayOfWeek     day_of_week;
        std::optional<int> m_n;
    };

    // Define weekday constants for testing
    static const Weekday MO(epoch_core::EpochDayOfWeek::Monday);
    static const Weekday TU(epoch_core::EpochDayOfWeek::Tuesday);
    static const Weekday WE(epoch_core::EpochDayOfWeek::Wednesday);
    static const Weekday TH(epoch_core::EpochDayOfWeek::Thursday);
    static const Weekday FR(epoch_core::EpochDayOfWeek::Friday);
    static const Weekday SA(epoch_core::EpochDayOfWeek::Saturday);
    static const Weekday SU(epoch_core::EpochDayOfWeek::Sunday);
} // namespace epoch_frame
