#pragma once
#include <Core/NamesAndTypes.h>
#include <Substrait/function.pb.h>

namespace dbms
{

void convertDataTypeToDerivationExpression(io::substrait::DerivationExpression* expression, DB::DataTypePtr type);

void convertNameAndTypeToArgument(io::substrait::FunctionSignature_Argument* argument, DB::NameAndTypePair args);

class SerializedFunctionBuilder
{
};

class SerializedScalarFunctionBuilder
{
public:
    SerializedScalarFunctionBuilder(
        int functionId,
        const DB::NamesAndTypesList & args,
        const std::string & func_name,
        bool is_deterministic,
        const DB::DataTypePtr & outputType);
    std::unique_ptr<io::substrait::FunctionSignature_Scalar> build();
private:
    int function_id;
    DB::NamesAndTypesList arguments;
    std::string name;
    bool deterministic;
    DB::DataTypePtr output_type;
    std::unique_ptr<io::substrait::FunctionSignature_Scalar> function;
};
}
