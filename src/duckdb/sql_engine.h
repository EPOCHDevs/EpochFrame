#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>

namespace epoch_frame {

// Abstract SQL engine interface - simplified to single-table API
class SQLEngine {
public:
    virtual ~SQLEngine() = default;

    // Get the singleton SQL engine instance
    static SQLEngine& get();
};

} // namespace epoch_frame