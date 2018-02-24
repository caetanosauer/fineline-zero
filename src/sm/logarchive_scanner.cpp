#include "logarchive_scanner.h"

#include <vector>

#include "stopwatch.h"
#include "smthread.h"
#include "log_consumer.h" // for LogScanner

// CS TODO: Aligning with the Linux standard FS block size
// We could try using 512 (typical hard drive sector) at some point,
// but none of this is actually standardized or portable
const size_t IO_ALIGN = 512;

thread_local std::vector<MergeInput> ArchiveScan::_mergeInputVector;

bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b)
{
    if (a.keyPID != b.keyPID) { return a.keyPID > b.keyPID; }
    return a.keyVersion > b.keyVersion;
}

ArchiveScan::ArchiveScan(std::shared_ptr<ArchiveIndex> archIndex)
    : archIndex(archIndex), prevVersion(0), prevPID(0), singlePage(false), lastProbedRun(0)
{
    clear();
}

void ArchiveScan::open(PageID startPID, PageID endPID, run_number_t runBegin,
        run_number_t runEnd)
{
    w_assert0(archIndex);
    clear();
    auto& inputs = _mergeInputVector;

    archIndex->probe(inputs, startPID, endPID, runBegin, runEnd);
    lastProbedRun = runEnd;

    singlePage = (endPID == startPID+1);

    heapBegin = inputs.begin();
    auto it = inputs.rbegin();
    while (it != inputs.rend())
    {
        if (it->open(startPID)) {
            auto lr = it->logrec();
            it++;
            if (singlePage && lr->type() == page_img_format_log) {
                // Any entries beyond it (including it are ignored)
                heapBegin = it.base();
                ADD_TSTAT(la_img_trimmed, heapBegin - inputs.begin());
                break;
            }
        }
        else {
            std::advance(it, 1);
            inputs.erase(it.base());
        }
    }

    heapEnd = inputs.end();
    std::make_heap(heapBegin, heapEnd, mergeInputCmpGt);
}

bool ArchiveScan::finished()
{
    return heapBegin == heapEnd;
}

void ArchiveScan::clear()
{
    auto& inputs = _mergeInputVector;
    for (auto it : inputs) {
        archIndex->closeScan(it.runFile->runid);
    }
    inputs.clear();
    heapBegin = inputs.end();
    heapEnd = inputs.end();
    prevVersion = 0;
    prevPID = 0;
}

bool ArchiveScan::next(logrec_t*& lr)
{
    if (finished()) { return false; }

    // CS: This optimization does not work with FineLine, because the mapping
    // of individual page updates to the run they end up in the log archive
    // is not a monotonic function (e.g., update 1 might be on run 2, update 2
    // on run 1, and update 3 on run 1 again).
    // if (singlePage) {
    //     if (!heapBegin->finished()) {
    //         lr = heapBegin->logrec();
    //         heapBegin->next();
    //     }
    //     else {
    //         heapBegin++;
    //         return next(lr);
    //     }
    // }
    // else
    {
        std::pop_heap(heapBegin, heapEnd, mergeInputCmpGt);
        auto top = std::prev(heapEnd);
        if (!top->finished()) {
            lr = top->logrec();
            w_assert1(lr->page_version() == top->keyVersion && lr->pid() == top->keyPID);
            top->next();
            std::push_heap(heapBegin, heapEnd, mergeInputCmpGt);
        }
        else {
            heapEnd--;
            return next(lr);
        }
    }

    prevVersion = lr->page_version();
    prevPID = lr->pid();

    return true;
}

ArchiveScan::~ArchiveScan()
{
    clear();
}

void ArchiveScan::dumpHeap()
{
    for (auto it = heapBegin; it != heapEnd; it++) {
        std::cout << *(it->logrec()) << std::endl;
    }
}

logrec_t* MergeInput::logrec()
{
    return reinterpret_cast<logrec_t*>(runFile->getOffset(pos));
}

bool MergeInput::open(PageID startPID)
{
    if (!finished()) {
        auto lr = logrec();
        keyVersion = lr->page_version();
        keyPID = lr->pid();

        // advance index until firstPID is reached
        if (keyPID < startPID) {
            while (!finished() && lr->pid() < startPID) {
                ADD_TSTAT(la_skipped_bytes, lr->length());
                next();
                lr = logrec();
            }
            if (finished()) {
                INC_TSTAT(la_wasted_read);
                return false;
            }
        }
    }
    else {
        INC_TSTAT(la_wasted_read);
        return false;
    }

    w_assert1(keyVersion == logrec()->page_version());
    return true;
}

bool MergeInput::finished()
{
    if (!runFile || runFile->length == 0) { return true; }
    auto lr = logrec();
    return lr->type() == skip_log || (endPID != 0 && lr->pid() >= endPID);
}

void MergeInput::next()
{
    w_assert1(!finished());
    pos += logrec()->length();
    w_assert1(logrec()->valid_header());
    keyPID = logrec()->pid();
    keyVersion = logrec()->page_version();
}
