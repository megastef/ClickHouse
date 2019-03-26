#pragma once
#include <Processors/IProcessor.h>

namespace DB
{

class QueryPipeline
{
public:
    QueryPipeline() = default;

    /// Each source must have single output port and no inputs. All outputs must have same header.
    void init(Processors sources);

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header)>;

    void addSimpleTransform(ProcessorGetter getter);

private:

    /// All added processors.
    Processors processors;

    /// Last processor for each independent "stream".
    Processors streams;

    /// Common header for each stream.
    Block current_header;
};

}
