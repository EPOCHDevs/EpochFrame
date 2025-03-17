//
// Created by adesola on 1/22/25.
//

#pragma once
#include "offset_handler.h"
#include "../timestamp.h"
#include <epoch_lab_shared/enum_wrapper.h>

CREATE_ENUM(EpochDateTimeDayOption, BusinessEnd, BusinessStart, End, Start);

namespace epochframe::datetime {
    inline bool is_normalized(Timestamp const &dt) {
        if (dt.hour() != 0 || dt.minute() != 0 or dt.second() != 0 || dt.microsecond() != 0 ||
            dt.nanosecond() != 0) {
            return false;
        }
        return true;
    }

    int get_day_of_month(Timestamp const& , EpochDateTimeDayOption const&);

    class BaseOffsetHandler : public OffsetHandler {
    public:
        explicit BaseOffsetHandler(int64_t n = 1, bool normalize = false) : m_n(n), m_normalize(normalize) {}

        [[nodiscard]] bool should_normalize() const { return m_normalize; }

        bool eq(const std::shared_ptr<OffsetHandler> &value) const override;

        size_t hash() const override {
            return std::hash<int64_t>{}(m_n) ^ std::hash<bool>{}(m_normalize);
        }

        std::shared_ptr<OffsetHandler> add(std::shared_ptr<OffsetHandler> const &) const {
            throw std::runtime_error("Offset does not support adding OffsetHandler.");
        }

        [[nodiscard]] Timestamp add(const epochframe::datetime::Timestamp &other) const override {
            return apply(other);
        }

        [[nodiscard]] Timestamp radd(const epochframe::datetime::Timestamp &other) const override {
            return add(other);
        }

        std::shared_ptr<OffsetHandler> sub(const std::shared_ptr<OffsetHandler> &other) const override;

        Timestamp rsub(const Timestamp &other) const override {
            return negate()->add(other);
        }

        virtual std::shared_ptr<OffsetHandler> fmul(double ) const {
            throw std::runtime_error("Offset does not support fmul");
        }

        std::shared_ptr<OffsetHandler> mul(int64_t other) const override {
            return from_base(m_n * other, m_normalize);
        }

        std::shared_ptr<OffsetHandler> rmul(int64_t other) const override {
            return mul(other);
        }

        std::shared_ptr<OffsetHandler> negate() const override {
            return mul(-1);
        }

        std::shared_ptr<OffsetHandler> div(int64_t) const override {
            throw std::runtime_error("Offset does not support div");
        }

        std::shared_ptr<OffsetHandler> copy() const override {
            return mul(1);
        }

        virtual std::string class_name() const override {
            return "BaseOffset";
        }

        std::string repr_attrs() const override {
            return "";
        }

        std::string repr() const override;

        std::shared_ptr<OffsetHandler> base() const override {
            return from_base(1, m_normalize);
        }

        std::string name() const override {
            return rule_code();
        }

        [[nodiscard]] std::string rule_code() const override {
            return prefix();
        }

        [[nodiscard]] std::string freqstr() const override;

        [[nodiscard]] std::optional<Timedelta> offset() const override {
            return std::nullopt;
        }

        [[nodiscard]] std::string offset_str() const override {
            return "";
        }

        [[nodiscard]] Timestamp rollback(const epochframe::datetime::Timestamp &other) const override {
            return is_on_offset(other) ? other : from_base(1, m_normalize)->rsub(other);
        }

        [[nodiscard]] Timestamp rollforward(const epochframe::datetime::Timestamp &other) const override {
            return is_on_offset(other) ? other : from_base(1, m_normalize)->add(other);
        }

        virtual int64_t get_offset_day(Timestamp const &other) const {
            return get_day_of_month(other, day_opt());
        }

        [[nodiscard]] bool is_on_offset(const epochframe::datetime::Timestamp &value) const override;

        bool is_month_start(const Timestamp &ts) const override {
            return ts.get_start_end_field("is_month_start", *this);
        }

        bool is_month_end(const Timestamp &ts) const override {
            return ts.get_start_end_field("is_month_end", *this);
        }

        bool is_quarter_start(const Timestamp &ts) const override {
            return ts.get_start_end_field("is_quarter_start", *this);
        }

        bool is_quarter_end(const Timestamp &ts) const override {
            return ts.get_start_end_field("is_quarter_end", *this);
        }

        bool is_year_start(const Timestamp &ts) const override {
            return ts.get_start_end_field("is_year_start", *this);
        }

        bool is_year_end(const Timestamp &ts) const override {
            return ts.get_start_end_field("is_year_end", *this);
        }

        virtual int starting_month() const override {
            return month();
        }

        virtual int month() const override {
            return 12;
        }

        bool should_normalized() const override {
            return m_normalize;
        }

    protected:
        [[nodiscard]] virtual int64_t n() const { return m_n; }

        virtual std::shared_ptr<OffsetHandler> from_base(std::int64_t n, bool normalize) const = 0;

        virtual EpochDateTimeDayOption day_opt() const {
            return EpochDateTimeDayOption::BusinessEnd;
        }

    private:
        std::int64_t m_n;      ///< The integer multiple of this offset.
        bool m_normalize;      ///< Whether to normalize to a boundary (e.g., midnight).
    };
}
