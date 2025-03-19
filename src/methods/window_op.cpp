#include "window_op.h"
#include "common/arrow_compute_utils.h"
namespace epochframe {
    IndexPtr WindowOperation::resolve_index(int64_t periods) const {
        auto index = m_data.first;
        return index;
    }

    // TableComponent WindowOperation::shift(int64_t periods) const {
    //     arrow::compute::PairwiseOptions options{periods};
    //     return apply("shift", &options);
    // }

    // TableComponent WindowOperation::pct_change(int64_t periods) const {
    //     auto diff_result = diff(periods);
    //     auto shift_result = shift(-periods);
    //     return diff_result / shift_result;
    // }

    TableComponent WindowOperation::diff(int64_t periods) const {
        auto diff_result = arrow_utils::call_compute_diff(m_data.second, periods);
        auto index = resolve_index(periods);
        return m_data;
    }
}