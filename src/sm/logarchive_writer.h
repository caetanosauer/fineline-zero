#ifndef LOGARCHIVE_WRITER_H
#define LOGARCHIVE_WRITER_H

#include <vector>

#include "basics.h"
#include "lsn.h"
#include "thread_wrapper.h"

class ArchiveIndex;
class logrec_t;

/** \brief Component that consumes a partially-sorted log record stream and
 * generates indexed runs from it.
 *
 * This class serves two purposes:
 * - It assembles individual log records into blocks which are written to
 *   runs on persistent storage .
 * - For each block generated, it generates an entry on the archive index,
 *   allowing direct access to each block based on log record attributes
 *   (page id & lsn).
 *
 * \author Caetano Sauer
 */
class BlockAssembly {
public:
    BlockAssembly(ArchiveIndex* index, size_t blockSize, unsigned level = 1, bool compression = true);
    virtual ~BlockAssembly();

    bool start(run_number_t run);
    bool add(logrec_t* lr);
    void finish();
    void shutdown();

    PageID getCurrentMaxPID() { return maxPID; }

private:
    char* dest;
    ArchiveIndex* archIndex;
    size_t blockSize;
    size_t pos;
    size_t fpos;
    size_t blockCount;

    run_number_t lastRun;
    PageID currentPID;
    size_t currentPIDpos;
    size_t currentPIDfpos;
    bool enableCompression;
    PageID maxPIDInRun;

    std::vector<pair<PageID, size_t>> buckets;

    unsigned level;
    PageID maxPID;
};

#endif
