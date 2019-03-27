#include <Processors/QueryPipeline.h>
#include <IO/WriteHelpers.h>
#include "ResizeProcessor.h"
#include "ConcatProcessor.h"

namespace DB
{

void QueryPipeline::checkInitialized()
{
    if (!initialized())
        throw Exception("QueryPipeline wasn't initialized.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::checkSource(const ProcessorPtr & source)
{
    if (!source->getInputs().empty())
        throw Exception("Source for query pipeline shouldn't have any input, but " + source->getName() + " has " +
                        toString(source->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (source->getOutputs().size() != 1)
        throw Exception("Source for query pipeline should have single output, but " + source->getName() + " has " +
                        toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::init(Processors sources)
{
    if (initialized())
        throw Exception("Pipeline has already been initialized.", ErrorCodes::LOGICAL_ERROR);

    if (sources.empty())
        throw Exception("Can't initialize pipeline with empty source list.", ErrorCodes::LOGICAL_ERROR);

    for (auto & source : sources)
    {
        checkSource(source);

        auto & header = source->getOutputs().front().getHeader();

        if (header)
            assertBlocksHaveEqualStructure(current_header, header, "QueryPipeline");
        else
            current_header = header;

        streams.emplace_back(&source->getOutputs().front());
        processors.emplace_back(std::move(source));
    }
}

void QueryPipeline::addSimpleTransform(ProcessorGetter getter)
{
    checkInitialized();

    Block header;

    for (auto & stream : streams)
    {
        auto transform = getter(current_header);

        if (transform->getInputs().size() != 1)
            throw Exception("Processor for query pipeline transform should have single input, "
                            "but " + transform->getName() + " has " +
                            toString(transform->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

        if (transform->getOutputs().size() != 1)
            throw Exception("Processor for query pipeline transform should have single output, "
                            "but " + transform->getName() + " has " +
                            toString(transform->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

        auto & out_header = transform->getOutputs().front().getHeader();

        if (header)
            assertBlocksHaveEqualStructure(header, out_header, "QueryPipeline");
        else
            header = out_header;

        connect(*stream, transform->getInputs().front());
        stream = &transform->getOutputs().front();
        processors.emplace_back(std::move(transform));
    }

    current_header = std::move(header);
}

void QueryPipeline::addDelayedStream(ProcessorPtr source)
{
    checkInitialized();

    if (has_delayed_stream)
        throw Exception("QueryPipeline already has stream with non joined data.", ErrorCodes::LOGICAL_ERROR);

    checkSource(source);
    assertBlocksHaveEqualStructure(current_header, source->getOutputs().front().getHeader(), "QueryPipeline");

    has_delayed_stream = !streams.empty();
    streams.emplace_back(&source->getOutputs().front());
    processors.emplace_back(std::move(source));
}

void QueryPipeline::concatDelayedStream()
{
    if (!has_delayed_stream)
        return;

    auto resize = std::make_shared<ResizeProcessor>(current_header, getNumMainStreams(), 1);
    auto stream = streams.begin();
    for (auto & input : resize->getInputs())
        connect(**(stream++), input);

    auto concat = std::make_shared<ConcatProcessor>(current_header, 2);
    connect(resize->getOutputs().front(), concat->getInputs().front());
    connect(*streams.back(), concat->getInputs().back());

    streams = { &concat->getOutputs().front() };
    processors.emplace_back(std::move(resize));
    processors.emplace_back(std::move(concat));
    has_delayed_stream = false;
}

void QueryPipeline::resize(size_t num_streams)
{
    concatDelayedStream();

    if (num_streams == getNumStreams())
        return;

    auto resize = std::make_shared<ResizeProcessor>(current_header, getNumStreams(), num_streams);
    auto stream = streams.begin();
    for (auto & input : resize->getInputs())
        connect(**(stream++), input);

    streams.clear();
    streams.reserve(num_streams);
    for (auto & output : resize->getOutputs())
        streams.emplace_back(&output);
}


}
