//
// Created by adesola on 1/21/25.
//

#pragma once
#include <vector>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>

using namespace boost::posix_time;
using namespace boost::gregorian;

namespace epochframe::datetime {

    std::vector<bool> get_start_end_field(std::vector<ptime> const &dtIndex,
                                          std::string field,
                                          std::string const &freqName,
                                          int month_kw = 12);

    std::vector<std::string> get_date_name_field(std::vector<ptime> const &dtIndex, bool);
}
