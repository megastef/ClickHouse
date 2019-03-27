#pragma once
#include <Processors/IProcessor.h>

namespace DB
{

class TableStructureReadLock;
using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;

class QueryPipeline
{
public:
    QueryPipeline() = default;

    /// Each source must have single output port and no inputs. All outputs must have same header.
    void init(Processors sources);
    bool initialized() { return !processors.empty(); }

    using ProcessorGetter = std::function<ProcessorPtr(const Block & header)>;

    void addSimpleTransform(ProcessorGetter getter);

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);

    void resize(size_t num_streams);

    size_t getNumStreams() const { return streams.size(); }
    size_t getNumMainStreams() const { return streams.size() - (has_delayed_stream ? 1 : 0); }

    const Block & getHeader() const { return current_header; }

    void addTableLock(const TableStructureReadLockPtr & lock) { table_locks.push_back(lock); }

private:

    /// All added processors.
    Processors processors;

    /// Port for each independent "stream".
    std::vector<OutputPort *> streams;

    /// Common header for each stream.
    Block current_header;

    TableStructureReadLocks table_locks;

    bool has_delayed_stream = false;

    void checkInitialized();
    void checkSource(const ProcessorPtr & source);
    void concatDelayedStream();
};

}
