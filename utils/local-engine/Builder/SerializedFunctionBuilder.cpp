#include "SerializedFunctionBuilder.h"
dbms::SerializedScalarFunctionBuilder::SerializedScalarFunctionBuilder(
    int functionId,
    const DB::NamesAndTypesList & args,
    const std::string & func_name,
    bool is_deterministic,
    const DB::DataTypePtr & outputType)
    : function_id(functionId), arguments(args), name(func_name), deterministic(is_deterministic), output_type(outputType)
{
}
std::unique_ptr<io::substrait::FunctionSignature_Scalar> dbms::SerializedScalarFunctionBuilder::build()
{
    this->function = std::make_unique<io::substrait::FunctionSignature_Scalar>();
    function->mutable_name()->Add(std::move(this->name));
    function->mutable_id()->set_id(this->function_id);
    function->set_deterministic(this->deterministic);
    convertDataTypeToDerivationExpression(function->mutable_output_type(), this->output_type);
    function->mutable_normal();
    for (const auto &arg : this->arguments) {
        auto *s_arg = function->mutable_arguments()->Add();
        convertNameAndTypeToArgument(s_arg, arg);
    }
    return std::move(function);
}
void dbms::convertDataTypeToDerivationExpression(io::substrait::DerivationExpression * expression, DB::DataTypePtr type)
{
    DB::WhichDataType which(type);
    if (which.isDate())
    {
        auto * date = expression->mutable_date();
        date->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isInt32())
    {
        auto * int_32 = expression->mutable_i32();
        int_32->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isInt64())
    {
        auto * int_64 = expression->mutable_i64();
        int_64->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isFloat32())
    {
        auto * float_32 = expression->mutable_fp32();
        float_32->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isFloat64())
    {
        auto * float_64 = expression->mutable_fp64();
        float_64->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isInt8())
    {
        auto * boolean = expression->mutable_bool_();
        boolean->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else
    {
        throw std::runtime_error("unsupported data type " + std::string(type->getFamilyName()));
    }
}

void dbms::convertNameAndTypeToArgument(io::substrait::FunctionSignature_Argument *argument, DB::NameAndTypePair arg)
{
    argument->set_name(arg.name);
    DB::WhichDataType which(arg.type);
    auto * p_type = argument->mutable_type()->mutable_type();
    if (which.isDate())
    {
        auto * date = p_type->mutable_date();
        date->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isInt32())
    {
        auto * int_32 = p_type->mutable_i32();
        int_32->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isInt64())
    {
        auto * int_64 = p_type->mutable_i64();
        int_64->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isFloat32())
    {
        auto * float_32 = p_type->mutable_fp32();
        float_32->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isFloat64())
    {
        auto * float_64 = p_type->mutable_fp64();
        float_64->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else if (which.isInt8())
    {
        auto * boolean = p_type->mutable_bool_();
        boolean->set_nullability(io::substrait::Type_Nullability_REQUIRED);
    }
    else
    {
        throw std::runtime_error("unsupported data type " + std::string(arg.type->getFamilyName()));
    }
}
