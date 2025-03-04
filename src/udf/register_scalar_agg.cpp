#include "register_scalar_agg.h"

namespace epochframe {
 Status RegisterScalarAggregateFunction(PyObject* function, UdfWrapperCallback cb,
                                       const UdfOptions& options) {

  auto* registry = compute::GetFunctionRegistry();

  static auto default_scalar_aggregate_options =
      compute::ScalarAggregateOptions::Defaults();
      
  auto aggregate_func = std::make_shared<compute::ScalarAggregateFunction>(
      options.func_name, options.arity, options.func_doc,
      &default_scalar_aggregate_options);

  std::vector<compute::InputType> input_types;
  for (const auto& in_dtype : options.input_types) {
    input_types.emplace_back(in_dtype);
  }
  compute::OutputType output_type(options.output_type);

  // Take reference before wrapping with OwnedRefNoGIL
  Py_INCREF(function);
  auto function_ref = std::make_shared<OwnedRefNoGIL>(function);

  compute::KernelInit init = [cb, function_ref, options](
                                 compute::KernelContext* ctx,
                                 const compute::KernelInitArgs& args)
      -> Result<std::unique_ptr<compute::KernelState>> {
    return std::make_unique<PythonUdfScalarAggregatorImpl>(
        function_ref, cb, options.input_types, options.output_type);
  };

  auto sig = compute::KernelSignature::Make(
      std::move(input_types), std::move(output_type), options.arity.is_varargs);
  compute::ScalarAggregateKernel kernel(std::move(sig), std::move(init),
                                        AggregateUdfConsume, AggregateUdfMerge,
                                        AggregateUdfFinalize, /*ordered=*/false);
  RETURN_NOT_OK(aggregate_func->AddKernel(std::move(kernel)));
  RETURN_NOT_OK(registry->AddFunction(std::move(aggregate_func)));
  return Status::OK();
}
}