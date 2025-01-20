//
// Created by adesola on 1/20/25.
//

#pragma once

#include "epochframe/aliases.h"


namespace epochframe {

    class NDFrame {
    protected:
        IndexPtr m_index;

    public:
        explicit NDFrame(IndexPtr index);

        virtual ~NDFrame() = default;

        virtual Shape2D shape() const = 0;
        // ...
    };

} // namespace epochframe
