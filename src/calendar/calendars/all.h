#pragma once
#include "epoch_frame/calendar_common.h"
#include "epoch_frame/market_calendar.h"
#include "epoch_frame/index.h"

namespace epoch_frame::calendar
{
    constexpr const char* EST = "America/New_York";
    constexpr const char* CET = "Europe/London";
    constexpr const char* UTC = "UTC";
    constexpr const char* PST = "America/Los_Angeles";
    constexpr const char* MST = "America/Denver";
    constexpr const char* CST = "America/Chicago";
    constexpr const char* AST = "America/Halifax";
    constexpr const char* HST = "Pacific/Honolulu";

    struct NYSEExchangeCalendar : public MarketCalendar
    {

        NYSEExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                             std::optional<MarketTime> const& close_time = std::nullopt);

        IndexPtr valid_days(const Date& start_date, const Date& end_date,
                            std::string const& tz = "UTC") const override;

        Series days_at_time(IndexPtr const&                                       days,
                            const std::variant<Time, epoch_core::MarketTimeType>& market_time,
                            int64_t day_offset = 0) const override;

        IndexPtr date_range_htf(Date const& start, Date const& end,
                                std::optional<int64_t> periods = {}) const override;
    };

    struct CMEEquityExchangeCalendar : public MarketCalendar
    {
        explicit CMEEquityExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                                  std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEAgricultureExchangeCalendar : public MarketCalendar
    {
        explicit CMEAgricultureExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                                       std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEBondExchangeCalendar : public MarketCalendar
    {
        explicit CMEBondExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                                std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CFEExchangeCalendar : public MarketCalendar
    {
        explicit CFEExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CBOEEquityOptionsExchangeCalendar : public MarketCalendar
    {
        explicit CBOEEquityOptionsExchangeCalendar(
            std::optional<MarketTime> const& open_time  = std::nullopt,
            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CBOEIndexOptionsExchangeCalendar : public MarketCalendar
    {
        explicit CBOEIndexOptionsExchangeCalendar(
            std::optional<MarketTime> const& open_time  = std::nullopt,
            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    // CME Globex Calendars
    struct CMEGlobexFXExchangeCalendar : public MarketCalendar
    {
        explicit CMEGlobexFXExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                                    std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEGlobexCryptoExchangeCalendar : public MarketCalendar
    {
        explicit CMEGlobexCryptoExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                                        std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEGlobexEquitiesExchangeCalendar : public MarketCalendar
    {
        explicit CMEGlobexEquitiesExchangeCalendar(
            std::optional<MarketTime> const& open_time  = std::nullopt,
            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEGlobexLivestockExchangeCalendar : public MarketCalendar
    {
        explicit CMEGlobexLivestockExchangeCalendar(
            std::optional<MarketTime> const& open_time  = std::nullopt,
            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEGlobexGrainsAndOilseedsExchangeCalendar : public MarketCalendar
    {
        explicit CMEGlobexGrainsAndOilseedsExchangeCalendar(
            std::optional<MarketTime> const& open_time  = std::nullopt,
            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEGlobexFixedIncomeCalendar : public MarketCalendar
    {
        explicit CMEGlobexFixedIncomeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                                     std::optional<MarketTime> const& close_time = std::nullopt);
    };

    struct CMEGlobexEnergyAndMetalsExchangeCalendar : public MarketCalendar
    {
        explicit CMEGlobexEnergyAndMetalsExchangeCalendar(
            std::optional<MarketTime> const& open_time  = std::nullopt,
            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    // ICE Calendar
    struct ICEExchangeCalendar : public MarketCalendar
    {
        explicit ICEExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                            std::optional<MarketTime> const& close_time = std::nullopt);
    };

    // FX Calendar
    struct FXExchangeCalendar : public MarketCalendar
    {
        explicit FXExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                           std::optional<MarketTime> const& close_time = std::nullopt);
    };

    // Crypto Calendar
    struct CryptoExchangeCalendar : public MarketCalendar
    {
        explicit CryptoExchangeCalendar(std::optional<MarketTime> const& open_time  = std::nullopt,
                               std::optional<MarketTime> const& close_time = std::nullopt);
    };
} // namespace epoch_frame::calendar
