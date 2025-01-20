//
// Created by adesola on 1/20/25.
//

#pragma once
#include "epochframe/aliases.h"


namespace epochframe::factory::index {
    IndexPtr range(uint64_t start, uint64_t stop, uint64_t step = 1);
}