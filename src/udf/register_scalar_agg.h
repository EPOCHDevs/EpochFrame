#pragma once

#include <arrow/compute/api_aggregate.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <arrow/util/type_fwd.h>


namespace arrow {
struct UdfOptions {
  std::string func_name;
  compute::Arity arity;
  compute::FunctionDoc func_doc;
  std::vector<std::shared_ptr<DataType>> input_types;
  std::shared_ptr<DataType> output_type;
};

struct UdfContext {
  MemoryPool* pool;
  int64_t batch_length;
};

   Status RegisterScalarAggregateFunction(PyObject* function, UdfWrapperCallback cb,
                                       const UdfOptions& options);
}