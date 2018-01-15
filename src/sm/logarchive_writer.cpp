#include "logarchive_writer.h"

#include "logarchive_index.h"

BlockAssembly::BlockAssembly(ArchiveIndex* index, size_t blockSize, unsigned level, bool compression)
    : lastRun(0), currentPID(0), blockSize(blockSize), level(level),
    maxPID(std::numeric_limits<PageID>::min())
{
    archIndex = index;
    dest = new char[blockSize];
    index->openNewRun(level);
}

BlockAssembly::~BlockAssembly()
{
    delete[] dest;
}

bool BlockAssembly::start(run_number_t run)
{
    if (run != lastRun) {
        w_assert1(lastRun == 0 || run == lastRun + 1);
        /*
         * Selection (producer) guarantees that logrec fits in block.
         * lastLSN is the LSN of the first log record in the new block
         * -- it will be used as the upper bound when renaming the file
         *  of the current run. This same LSN will be used as lower
         *  bound on the next run, which allows us to verify whether
         *  holes exist in the archive.
         */
        W_COERCE(archIndex->closeCurrentRun(lastRun, level, maxPIDInRun));
        DBGTHRD(<< "Opening file for new run " << run);

        archIndex->startNewRun(level);
        fpos = 0;
        lastRun = run;
        currentPID = std::numeric_limits<PageID>::max();
    }

    pos = 0;
    currentPIDpos = 0;
    currentPIDfpos = fpos;
    maxPID = std::numeric_limits<PageID>::min();
    blockCount = 0;
    buckets.clear();

    return true;
}

bool BlockAssembly::add(logrec_t* lr)
{
    w_assert0(dest);
    w_assert1(lr->valid_header());

    // Verify if we still have space for this log record
    size_t available = blockSize - (pos + logrec_t::get_skip_log().length());
    if (lr->length() > available) {
        // If this is a page_img logrec, we might still have space for it because
        // the preceding log records of the same PID will be dropped
        if (enableCompression && lr->type() == page_img_format_log) {
            size_t imgAvailable = blockSize - (currentPIDpos + logrec_t::get_skip_log().length());
            bool hasSpaceForPageImg = lr->pid() == currentPID && lr->length() < imgAvailable;
            if (!hasSpaceForPageImg) { return false; }
        }
        else { return false; }
    }

    // New PID coming in: reset current PID stuff and check if it's time to add new bucket
    if (lr->pid() != currentPID) {
        currentPID = lr->pid();
        currentPIDpos = pos;
        currentPIDfpos = fpos;
        buckets.emplace_back(currentPID, fpos);
        if (currentPID > maxPID) { maxPID = currentPID; }
    }

    if (enableCompression && lr->type() == page_img_format_log) {
        // Keep track of compression efficicency
        ADD_TSTAT(la_img_compressed_bytes, pos - currentPIDpos);
        //  Simply discard all log records produced for the current PID do far
        pos = currentPIDpos;
        fpos = currentPIDfpos;
    }
    w_assert1(pos > 0 || fpos % blockSize == 0);

    memcpy(dest + pos, lr, lr->length());

    pos += lr->length();
    fpos += lr->length();
    return true;
}

void BlockAssembly::finish()
{
    w_assert0(dest);
    w_assert0(archIndex);

    archIndex->newBlock(buckets, level);
    if (maxPID > maxPIDInRun) { maxPIDInRun = maxPID; }

    DBGTHRD(<< "Writing block " << blockCount++ << " into run " << lastRun);
    archIndex->append(dest, pos, level);
}

void BlockAssembly::shutdown()
{
    W_COERCE(archIndex->closeCurrentRun(lastRun, level, maxPIDInRun));
}
