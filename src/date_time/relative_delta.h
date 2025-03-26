#pragma once
#include "relative_delta_options.h"

namespace epoch_frame {
        class RelativeDelta {
            public:
                // Default constructor for testing
                RelativeDelta() = default;
                RelativeDelta(const RelativeDeltaOption &option);

                RelativeDelta normalized() const;

                Date operator+(Date const &dt) const;

                DateTime operator+(DateTime const &dt) const;

                RelativeDelta operator+(RelativeDelta const &dt) const;

                RelativeDelta operator+(TimeDelta const &dt) const;

                Date operator-(Date const &dt) const;

                friend RelativeDelta operator+(TimeDelta const &dt, const RelativeDelta &delta) {
                    return delta + dt;
                }

                friend DateTime operator+(DateTime const &dt, const RelativeDelta &delta) {
                    return delta + dt;
                }

                friend Date operator+(Date const &dt, const RelativeDelta &delta) {
                    return delta + dt;
                }

                friend Date operator-(Date const &other, const RelativeDelta &delta) {
                    return other + (-delta);
                }

                friend DateTime operator-(DateTime const &other, const RelativeDelta &delta) {
                    return other + (-delta);
                }

                RelativeDelta operator-(RelativeDelta const &dt) const;

                [[nodiscard]] RelativeDelta abs() const;

                RelativeDelta operator-() const;

                explicit operator bool() const;

                RelativeDelta operator*(double const &other) const;

                friend RelativeDelta operator*(double const &other, const RelativeDelta &delta) {
                    return delta * other;
                }

                bool operator==(const RelativeDelta &other) const;

                bool operator!=(const RelativeDelta &other) const {
                    return !(*this == other);
                }

                RelativeDelta operator/(double const &other) const;

                // Getter methods for testing
                int64_t years() const { return m_years; }
                int64_t months() const { return m_months; }
                int64_t days() const { return m_days; }
                int64_t leapdays() const { return m_leapdays; }
                int64_t hours() const { return m_hours; }
                int64_t minutes() const { return m_minutes; }
                int64_t seconds() const { return m_seconds; }
                int64_t microseconds() const { return m_microseconds; }

                // Optional values getters
                std::optional<uint32_t> year() const { return m_year; }
                std::optional<uint32_t> month() const { return m_month; }
                std::optional<uint32_t> day() const { return m_day; }
                std::optional<Weekday> weekday() const { return m_weekday; }
                std::optional<int64_t> hour() const { return m_hour; }
                std::optional<int64_t> minute() const { return m_minute; }
                std::optional<int64_t> second() const { return m_second; }
                std::optional<int64_t> microsecond() const { return m_microsecond; }

            std::string repr() const;

            friend std::ostream &operator<<(std::ostream &os, const RelativeDelta &dt) {
                return os << dt.repr();
            }

            int64_t weeks() const { 
                return static_cast<int64_t>(m_days / 7);
            }

            void set_weeks(int64_t value) {
                m_days = m_days - (weeks() * 7) + value * 7;
            }

            private:
                double m_years{0};
                double m_months{0};
                double m_days{0};
                double m_leapdays{0};
                double m_hours{0};
                double m_minutes{0};
                double m_seconds{0};
                double m_microseconds{0};
                std::optional<uint32_t> m_year{std::nullopt};
                std::optional<uint32_t> m_month{std::nullopt};
                std::optional<uint32_t> m_day{std::nullopt};
                std::optional<Weekday> m_weekday{std::nullopt};
                std::optional<int64_t> m_hour{std::nullopt};
                std::optional<int64_t> m_minute{std::nullopt};
                std::optional<int64_t> m_second{std::nullopt};
                std::optional<int64_t> m_microsecond{std::nullopt};
                bool m_has_time{false};

                void set_months(int64_t months);

                void fix();
    };

Date easter(int year);
    
}
