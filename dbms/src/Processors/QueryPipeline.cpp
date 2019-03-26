#include <Processors/QueryPipeline.h>
#include <IO/WriteHelpers.h>

namespace DB
{

void QueryPipeline::init(Processors sources)
{
    if (!processors.empty())
        throw Exception("Pipeline has already been initialized.", ErrorCodes::LOGICAL_ERROR);

    if (sources.empty())
        throw Exception("Can't initialize pipeline with empty source list.", ErrorCodes::LOGICAL_ERROR);

    for (auto & source : sources)
    {
        if (!source->getInputs().empty())
            throw Exception("Source for query pipeline shouldn't have any input, but " + source->getName() + " has " +
                            toString(source->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

        if (source->getOutputs().size() != 1)
            throw Exception("Source for query pipeline should have single output, but " + source->getName() + " has " +
                            toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

        auto & header = source->getOutputs().front().getHeader();

        if (header)
            assertBlocksHaveEqualStructure(current_header, header, "QueryPipeline");
        else
            current_header = header;

        processors.emplace_back(source);
    }

    streams = std::move(sources);
}

void QueryPipeline::addSimpleTransform(ProcessorGetter getter)
{
    Block header;

    for (auto & stream : streams)
    {
        auto processor = getter(current_header);

        if (processor->getInputs().size() != 1)
            throw Exception("Processor for query pipeline transform should have single input, "
                            "but " + processor->getName() + " has " +
                            toString(processor->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

        if (processor->getOutputs().size() != 1)
            throw Exception("Processor for query pipeline transform should have single output, "
                            "but " + processor->getName() + " has " +
                            toString(processor->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

        auto & out_header = processor->getOutputs().front().getHeader();

        if (header)
            assertBlocksHaveEqualStructure(header, out_header, "QueryPipeline");
        else
            header = out_header;

        connect(stream->getOutputs().front(), processor->getInputs().front());
        processors.emplace_back(processor);
        stream = std::move(processor);
    }
}


}
