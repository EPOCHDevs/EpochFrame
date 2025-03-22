#pragma once

#include <unordered_map>
#include <any>

CREATE_ENUM(EpochFrameMarketTimeType,
    MarketOpen,
    MarketClose,
    BreakStart,
    BreakEnd,
    Pre,
    Post
);

namespace epoch_frame::calendar {

template<typename T>
    class ProtectedDict {
        using K = EpochFrameMarketTimeType;

        public:
            ProtectedDict( auto&&... args) : m_dict(std::unordered_map<K, T>(std::forward<decltype(args)>(args)...)) {}

            void _set(const K& key, const T& value) {
                _setitem_(key, value);
            }

            void _del(const K& key) {
                _delitem_(key);
            }

            const std::unordered_map<K, T>& dict() const {
                return this->m_dict;
            }

            bool contains(const K& key) const {
                return this->m_dict.contains(key);
            }

            T operator[](const K& key) const {
                try {
                    return this->m_dict.at(key);
                } catch (const std::out_of_range& e) {
                    throw std::runtime_error("Key " + EpochFrameMarketTimeTypeWrapper::ToString(key) + " not found in dict");
                }
            }

        auto begin() const { return this->m_dict.cbegin(); }
        auto end() const { return this->m_dict.cend();}

        bool empty() const { return this->m_dict.empty(); }
      private:
            bool INIT_RAN_NORMALLY = true;
            std::unordered_map<K, T> m_dict;

            void _setitem_(const K& key, const T& value) {
                if (!this->INIT_RAN_NORMALLY) {
                    this->m_dict.insert_or_assign(key, value);
                } else {
                    throw std::runtime_error("You cannot set a value directly, you can change regular_market_times "
                                             "using .change_time, .add_time or .remove_time.");
                }
            }

            void _delitem_(const K& key) {
                if (!this->INIT_RAN_NORMALLY) {
                    this->m_dict.erase(key);
                } else {
                    throw std::runtime_error("You cannot delete a value directly, you can change regular_market_times "
                                             "using .change_time, .add_time or .remove_time.");
                }
            }
    };

}

