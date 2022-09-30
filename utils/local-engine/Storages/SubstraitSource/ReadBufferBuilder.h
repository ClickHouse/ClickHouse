#pragma once
#include <substrait/plan.pb.h>
#include <functional>
#include <memory>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>
namespace local_engine
{
class ReadBufferBuilder
{
public:
    explicit ReadBufferBuilder(DB::ContextPtr context_) : context(context_) {}
    virtual ~ReadBufferBuilder() = default;
    /// build a new read buffer
    virtual std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) = 0;
protected:
    DB::ContextPtr context;
};

using ReadBufferBuilderPtr = std::shared_ptr<ReadBufferBuilder>;

class ReadBufferBuilderFactory : public boost::noncopyable
{
public:
    using NewBuilder = std::function<ReadBufferBuilderPtr(DB::ContextPtr)>;
    static ReadBufferBuilderFactory & instance();
    ReadBufferBuilderPtr createBuilder(const String & schema, DB::ContextPtr context);

    void registerBuilder(const String & schema, NewBuilder newer);

private:
    std::map<String, NewBuilder> builders;
};

void registerReadBufferBuildes(ReadBufferBuilderFactory & factory);
}
