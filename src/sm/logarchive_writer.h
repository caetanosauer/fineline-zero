#ifndef LOGARCHIVE_WRITER_H
#define LOGARCHIVE_WRITER_H

#include <vector>

#include "basics.h"
#include "lsn.h"
#include "thread_wrapper.h"

class AsyncRingBuffer;
class ArchiveIndex;
class logrec_t;

/** \brief Asynchronous writer thread to produce run files on disk
 *
 * Consumes blocks of data produced by the BlockAssembly component and
 * writes them to the corresponding run files on disk. Metadata on each
 * block is used to control to which run each block belongs and what LSN
 * ranges are contained in each run (see BlockAssembly).
 *
 * \author Caetano Sauer
 */
class WriterThread : public thread_wrapper_t {
private:

    AsyncRingBuffer* buf;
    ArchiveIndex* index;
    unsigned fsyncFrequency;
    run_number_t currentRun;
    unsigned level;
    PageID maxPIDInRun;
    unsigned appendBlockCount;

public:
    virtual void run();

    ArchiveIndex* getIndex() { return index; }

    /*
     * Called by processFlushRequest to forcibly start a new run
     */
    void resetCurrentRun()
    {
        currentRun++;
        maxPIDInRun = std::numeric_limits<PageID>::min();
    }

    WriterThread(AsyncRingBuffer* writebuf, ArchiveIndex* index, unsigned level, unsigned fsyncFrequency)
        :
            buf(writebuf), index(index), fsyncFrequency(fsyncFrequency),
            currentRun(0), level(level),
            maxPIDInRun(std::numeric_limits<PageID>::min()), appendBlockCount(0)
    {
    }

    virtual ~WriterThread() {}
};

/** \brief Component that consumes a partially-sorted log record stream and
 * generates indexed runs from it.
 *
 * This class serves two purposes:
 * - It assembles individual log records into blocks which are written to
 *   persistent storage using an asynchronous writer thread (see
 *   WriterThread).
 * - For each block generated, it generates an entry on the archive index,
 *   allowing direct access to each block based on log record attributes
 *   (page id & lsn).
 *
 * The writer thread is controlled solely using an asynchronous ring
 * buffer. This works because the writer should keep writing as long as
 * there are blocks available -- unlike the reader thread, which must stop
 * once a certain LSN is reached.
 *
 * Each generated block contains a <b>header</b>, which specifies the run
 * number, the offset up to which valid log records are found within that
 * block, and the LSN of the last log record in the block. The run number
 * is used by the writer thread to write blocks to the correct run file --
 * once it changes from one block to another, it must close the currently
 * generated run file an open a new one. The LSN in the last block header
 * is then used to rename the file with the correct LSN range. (We used to
 * control these LSN boundaries with an additional queue structure, but it
 * required too many dependencies between modules that are otherwise
 * independent)
 *
 * \author Caetano Sauer
 */
class BlockAssembly {
public:
    BlockAssembly(ArchiveIndex* index, size_t blockSize, unsigned level = 1, bool compression = true,
            unsigned fsyncFrequency = 1);
    virtual ~BlockAssembly();

    bool start(run_number_t run);
    bool add(logrec_t* lr);
    void finish();
    void shutdown();
    bool hasPendingBlocks();

    void resetWriter() { writer->resetCurrentRun(); }
    size_t getBlockSize() { return blockSize; }
    PageID getCurrentMaxPID() { return maxPID; }

    // methods that abstract block metadata
    static run_number_t getRunFromBlock(const char* b);
    static size_t getEndOfBlock(const char* b);
    static PageID getMaxPIDFromBlock(const char* b);
private:
    char* dest;
    AsyncRingBuffer* writebuf;
    WriterThread* writer;
    ArchiveIndex* archIndex;
    const size_t blockSize;
    size_t pos;
    size_t fpos;

    run_number_t lastRun;
    PageID currentPID;
    size_t currentPIDpos;
    size_t currentPIDfpos;
    bool enableCompression;

    PageID maxPIDInRun;
    std::vector<pair<PageID, size_t>> buckets;

    unsigned level;
    PageID maxPID;
public:
    struct BlockHeader {
        uint32_t end;
        PageID maxPID;
        run_number_t run;
    };

};

#endif
