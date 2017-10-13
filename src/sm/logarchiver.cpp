#include "w_defines.h"

#define SM_SOURCE
#define LOGARCHIVER_C

#include "logarchiver.h"
#include "sm_options.h"
#include "log_core.h"
#include "xct_logger.h"
#include "bf_tree.h" // to check for warmup
#include "logarchive_scanner.h" // CS TODO just for RunMerger -- remove

#include <algorithm>
#include <sm_base.h>

#include "stopwatch.h"

typedef fixed_lists_mem_t::slot_t slot_t;

const static int DFT_BLOCK_SIZE = 1024 * 1024; // 1MB = 128 pages

LogArchiver::LogArchiver(
        ArchiveIndex* d, LogConsumer* c, ArchiverHeap* h, BlockAssembly* b)
    :
    consumer(c), heap(h), blkAssemb(b),
    shutdownFlag(false), control(&shutdownFlag), selfManaged(false),
    flushReqLSN(lsn_t::null)
{
    index.reset(d);
    nextActLSN = lsn_t(index->getLastRun() + 1, 0);
}

LogArchiver::LogArchiver(const sm_options& options)
    :
    shutdownFlag(false), control(&shutdownFlag), selfManaged(true),
    flushReqLSN(lsn_t::null)
{
    constexpr size_t defaultWorkspaceSize = 1600;
    size_t workspaceSize = 1024 * 1024 * // convert MB -> B
        options.get_int_option("sm_archiver_workspace_size", defaultWorkspaceSize);

    size_t blockSize = DFT_BLOCK_SIZE;
    // CS TODO: archiver currently only works with 1MB blocks
        // options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);

    // FINELINE: log archive always eager
    eager = true;
    readWholeBlocks = options.get_bool_option(
            "sm_archiver_read_whole_blocks", DFT_READ_WHOLE_BLOCKS);
    slowLogGracePeriod = options.get_int_option(
            "sm_archiver_slow_log_grace_period", DFT_GRACE_PERIOD);
    bool compression = options.get_int_option("sm_page_img_compression", 0);

    index = std::make_shared<ArchiveIndex>(options);
    nextActLSN = lsn_t(index->getLastRun() + 1, 0);
    w_assert1(nextActLSN.hi() > 0);

    constexpr bool startFromFirstLogPartition = true;
    if (smlevel_0::log && nextActLSN == lsn_t(1,0) && startFromFirstLogPartition) {
        std::vector<partition_number_t> partitions;
        smlevel_0::log->get_storage()->list_partitions(partitions);
        if (partitions.size() > 0) {
            nextActLSN = lsn_t(partitions[0], 0);
        }
    }

    consumer = new LogConsumer(nextActLSN, blockSize);
    heap = new ArchiverHeap(workspaceSize);
    blkAssemb = new BlockAssembly(index.get(), 1 /*level*/, compression);

    merger = nullptr;
    if (options.get_bool_option("sm_archiver_merging", false)) {
        merger = new MergerDaemon(options, index);
        merger->fork();
        merger->wakeup();
    }
}

/*
 * Shutdown sets the finished flag on read and write buffers, which makes
 * the reader and writer threads finish processing the current block and
 * then exit. The replacement-selection will exit as soon as it requests
 * a block and receives a null pointer. If the shutdown flag is set, the
 * method exits without error.
 *
 * Thread safety: since all we do with the shutdown flag is set it to true,
 * we do not worry about race conditions. A memory barrier is also not
 * required, because other threads don't have to immediately see that
 * the flag was set. As long as it is eventually set, it is OK.
 */
void LogArchiver::shutdown()
{
    // FINELINE
    // CS TODO BUG: we need some sort of pin mechanism (e.g., shared_ptr) for shutdown,
    // because threads may still be accessing the log archive here.
    // this flag indicates that reader and writer threads delivering null
    // blocks is not an error, but a termination condition
    archiveUntil(smlevel_0::log->durable_lsn().hi());
    DBGOUT(<< "CONSUMER SHUTDOWN STARTING");
    consumer->shutdown();
    DBGOUT(<< "LOG ARCHIVER SHUTDOWN STARTING");
    shutdownFlag = true;
    join();
    DBGOUT(<< "BLKASSEMB SHUTDOWN STARTING");
    // CS FINELINE TODO: this shutdown does not close the current run anymore, so
    // the shutdown is always dirty. A clean shutdown would require invoking
    // archiveUntil with the durable LSN, similar to the sm construction.
    blkAssemb->shutdown();
    DBGOUT(<< "MERGER SHUTDOWN STARTING");
    if (merger) { merger->stop(); }
}

LogArchiver::~LogArchiver()
{
    if (!shutdownFlag) {
        shutdown();
    }
    if (selfManaged) {
        delete blkAssemb;
        delete consumer;
        delete heap;
        index = nullptr;
        if (merger) { delete merger; }
    }
}

/**
 * Selection part of replacement-selection algorithm. Takes the smallest
 * record from the heap and copies it to the write buffer, one IO block
 * at a time. The block header contains the run number (1 byte) and the
 * logical size of the block (4 bytes). The former is required so that
 * the asynchronous writer thread knows when to start a new run file.
 * The latter simplifies the write process by not allowing records to
 * be split in the middle by block boundaries.
 */
bool LogArchiver::selection()
{
    if (heap->size() == 0) {
        // if there are no elements in the heap, we have nothing to write
        // -> return and wait for next activation
        DBGTHRD(<< "Selection got empty heap -- sleeping");
        return false;
    }

    run_number_t run = heap->topRun();
    if (!blkAssemb->start(run)) {
        return false;
    }

    DBGTHRD(<< "Producing block for selection on run " << run);
    while (true) {
        if (heap->size() == 0 || run != heap->topRun()) {
            break;
        }

        logrec_t* lr = heap->top();
        if (blkAssemb->add(lr)) {
            // DBGTHRD(<< "Selecting for output: " << *lr);
            heap->pop();
            // w_assert3(run != heap->topRun() ||
            //     heap->top()->pid()>= lr->pid());
        }
        else {
            break;
        }
    }
    blkAssemb->finish();

    return true;
}

ArchiverHeap::ArchiverHeap(size_t workspaceSize)
    : w_heap(heapCmp)
{
    workspace = new fixed_lists_mem_t(workspaceSize, 32 /*incr*/, 16384 /*max*/);
}

ArchiverHeap::~ArchiverHeap()
{
    delete workspace;
}

slot_t ArchiverHeap::allocate(size_t length)
{
    slot_t dest(NULL, 0);
    W_COERCE(workspace->allocate(length, dest));

    if (!dest.address) {
        // workspace full -> do selection until space available
        DBGTHRD(<< "Heap full! Size: " << w_heap.NumElements()
                << " alloc size: " << length);
        // FINELINE
        // if (!filledFirst) {
        //     // first run generated by first full load of w_heap
        //     currentRun++;
        //     filledFirst = true;
        //     DBGTHRD(<< "Heap full for the first time; start run 1");
        // }
    }

    return dest;
}

bool ArchiverHeap::push(logrec_t* lr, run_number_t run, bool duplicate)
{
    w_assert1(lr->valid_header());
    slot_t dest = allocate(lr->length());
    if (!dest.address) {
        DBGTHRD(<< "heap full for logrec: " << lr->type_str());
        return false;
    }

    w_assert1(dest.length >= lr->length());
    PageID pid = lr->pid();
    auto version = lr->page_version();
    memcpy(dest.address, lr, lr->length());

    // CS: Multi-page log records are replicated so that each page can be
    // recovered from the log archive independently.  Note that this is not
    // required for Restart or Single-page recovery because following the
    // per-page log chain of both pages eventually lands on the same multi-page
    // log record. For restore, it must be duplicated because log records are
    // sorted and there is no chain.
    if (duplicate) {
        auto lr2 = reinterpret_cast<logrec_t*>(dest.address);
        // If we have to duplciate the log record, make sure there is room by
        // calling recursively without duplication. Note that the original
        // contents were already saved with the memcpy operation above.
        auto pid2 = lr->pid2();
        auto version2 = lr->page2_version();
        auto orig_len = lr->length();
        lr2->remove_info_for_pid(lr->pid());
        lr2->set_pid(pid2);
        lr2->set_page_version(version2);
        w_assert1(lr2->valid_header());
        // w_assert1(lr->valid_header(lsn));
        if (!push(lr2, run, false)) {
            // If duplicated did not fit, then insertion of the original must
            // also fail. We have to free its memory from the workspace. Since
            // nothing was added to the heap yet, it stays untouched.
            W_COERCE(workspace->free(dest));
            return false;
        }
        // now that a compressed log record of pid2 has been pushed, compress
        // this log record, i.e., of pid1. Must copy into scratch memory,
        // because lr is coming from unmodifiable mmaped memory.
        memcpy(dest.address, lr, lr->length());
        lr2->remove_info_for_pid(lr2->pid2());
    }

    //DBGTHRD(<< "Processing logrec " << lr->lsn_ck() << ", type " <<
    //        lr->type() << "(" << lr->type_str() << ") length " <<
    //        lr->length() << " into run " << (int) currentRun);

    // insert key and pointer into w_heap
    HeapEntry k(run, pid, version, dest);

    // CS: caution: AddElementDontHeapify does NOT work!!!
    w_heap.AddElement(k);

    return true;
}

void ArchiverHeap::pop()
{
    // DBGTHRD(<< "Selecting for output: "
    //         << *((logrec_t*) w_heap.First().slot.address));
    logrec_t* lr = ((logrec_t*) w_heap.First().slot.address);
    w_assert1(lr->valid_header());

    W_COERCE(workspace->free(w_heap.First().slot));
    w_heap.RemoveFirst();

    // FINELINE
    // if (size() == 0) {
    //     // If heap becomes empty, run generation must be reset with a new run
    //     filledFirst = false;
    //     currentRun++;
    // }
}

logrec_t* ArchiverHeap::top()
{
    logrec_t* lr = (logrec_t*) w_heap.First().slot.address;
    w_assert1(lr->valid_header());
    return lr;
}

// gt is actually a less than function, to produce ascending order
bool ArchiverHeap::Cmp::gt(const HeapEntry& a,
        const HeapEntry& b) const
{
    if (a.run != b.run) {
        return a.run < b.run;
    }
    if (a.pid != b.pid) {
        return a.pid< b.pid;
    }
    return a.version < b.version;
}

/**
 * Replacement part of replacement-selection algorithm. Fetches log records
 * from the read buffer into the sort workspace and adds a correspondent
 * entry to the heap. When workspace is full, invoke selection until there
 * is space available for the current log record.
 *
 * Unlike standard replacement selection, runs are limited to the size of
 * the workspace, in order to maintain a simple non-overlapping mapping
 * between regions of the input file (i.e., the recovery log) and the runs.
 * To achieve that, we change the logic that assigns run numbers to incoming
 * records:
 *
 * a) Standard RS: if incoming key is larger than the last record written,
 * assign to current run, otherwise to the next run.
 * b) Log-archiving RS: keep track of run number currently being written,
 * always assigning the incoming records to a greater run. Once all records
 * from the current run are removed from the heap, increment the counter.
 * To start, initial input records are assigned to run 1 until the workspace
 * is full, after which incoming records are assigned to run 2.
 */
void LogArchiver::replacement()
{
    while(true) {
        logrec_t* lr;
        lsn_t lsn {lsn_t::null};
        if (!consumer->next(lr, &lsn)) {
            w_assert0(readWholeBlocks ||
                    control.endLSN <= consumer->getNextLSN());
            if (control.endLSN < consumer->getNextLSN()) {
                // nextLSN may be greater than endLSN due to skip
                control.endLSN = consumer->getNextLSN();
                // TODO: in which correct situation can this assert fail???
                // w_assert0(control.endLSN.hi() == 0);
                DBGTHRD(<< "Replacement changed endLSN to " << control.endLSN);
            }
            return;
        }

        if (!lr->is_redo()) {
            continue;
        }

        w_assert1(lr->valid_header());
        w_assert1(lsn.hi() > 0);
        const run_number_t run = lsn.hi();
        pushIntoHeap(lr, run, lr->is_multi_page());
    }
}

void LogArchiver::pushIntoHeap(logrec_t* lr, run_number_t run, bool duplicate)
{
    while (!heap->push(lr, run, duplicate)) {
        if (heap->size() == 0) {
            W_FATAL_MSG(fcINTERNAL,
                    << "Heap empty but push not possible!");
        }

        // heap full -- invoke selection and try again
        if (heap->size() == 0) {
            // CS TODO this happens sometimes for very large page_img_format
            // logrecs. Inside this if, we should "reset" the heap and also
            // makesure that the log record is smaller than the max block.
            W_FATAL_MSG(fcINTERNAL,
                    << "Heap empty but push not possible!");
        }

        DBGTHRD(<< "Heap full! Invoking selection");
        bool success = selection();

        w_assert0(success || heap->size() == 0);
    }
}

void LogArchiver::activate(lsn_t endLSN, bool wait)
{
    if (eager) return;

    w_assert0(smlevel_0::log);
    if (endLSN == lsn_t::null) {
        endLSN = smlevel_0::log->durable_lsn();
    }

    while (!control.activate(wait, endLSN)) {
        if (!wait) break;
    }
}

bool LogArchiver::waitForActivation()
{
    if (eager) {
        lsn_t newEnd = smlevel_0::log->durable_lsn();
        while (control.endLSN == newEnd) {
            // we're going faster than log, sleep a bit (1ms)
            ::usleep(1000);
            newEnd = smlevel_0::log->durable_lsn();

            if (shutdownFlag) {
                return false;
            }

            // Flushing requested (e.g., by restore manager)
            if (flushReqLSN != lsn_t::null) {
                return true;
            }

            if (newEnd.lo() == 0) {
                // If durable_lsn is at the beginning of a new log partition,
                // it can happen that at this point the file was not created
                // yet, which would cause the reader thread to fail.
                continue;
            }
        }
        control.endLSN = newEnd;
    }
    else {
        bool activated = control.waitForActivation();
        if (!activated) {
            return false;
        }
    }

    if (shutdownFlag) {
        return false;
    }

    return true;
}

bool LogArchiver::processFlushRequest()
{
    if (flushReqLSN != lsn_t::null) {
        DBGTHRD(<< "Archive flush requested until LSN " << flushReqLSN);
        if (getNextConsumedLSN() < flushReqLSN) {
            // if logrec hasn't been read into heap yet, then selection
            // will never reach it. Do another round until heap has
            // consumed it.
            if (control.endLSN < flushReqLSN) {
                control.endLSN = flushReqLSN;
            }
            DBGTHRD(<< "LSN requested for flush hasn't been consumed yet. "
                    << "Trying again after another round");
            return false;
        }
        else {
            // consume whole heap
            while (selection()) {}
            // Heap empty: Wait for all blocks to be consumed and writen out
            w_assert0(heap->size() == 0);
            while (blkAssemb->hasPendingBlocks()) {
                ::usleep(10000); // 10ms
            }

            DBGTHRD(<< "processFlushRequest forcing closure of run "
                    << flushReqLSN.hi());

            // Forcibly close current run to guarantee that LSN is persisted
            PageID maxPID = blkAssemb->getCurrentMaxPID();
            W_COERCE(index->closeCurrentRun(flushReqLSN.hi(), 1 /* level */, maxPID));
            // blkAssemb->resetWriter();

            // CS FINELINE TODO: must guarantee that flushReqLSN.hi() will not
            // be appended to anymore!

            /* Now we know that the requested LSN has been processed by the
             * heap and all archiver temporary memory has been flushed. Thus,
             * we know it has been fully processed and all relevant log records
             * are available in the archive.
             */
            flushReqLSN = lsn_t::null;
            lintel::atomic_thread_fence(lintel::memory_order_release);
            return true;
        }
    }
    return false;
}

bool LogArchiver::isLogTooSlow()
{
    if (!eager) { return false; }

    int minActWindow = index->getBlockSize();

    auto isSmallWindow = [minActWindow](lsn_t endLSN, lsn_t nextLSN) {
        int nextHi = nextLSN.hi();
        int nextLo = nextLSN.lo();
        int endHi = endLSN.hi();
        int endLo = endLSN.lo();
        return (endHi == nextHi && endLo - nextLo< minActWindow) ||
            (endHi == nextHi + 1 && endLo < minActWindow);
    };

    if (isSmallWindow(control.endLSN, nextActLSN))
    {
        // If this happens to often, the block size should be decreased.
        ::usleep(slowLogGracePeriod);
        // To better exploit device bandwidth, we only start archiving if
        // at least one block worth of log is available for consuption.
        // This happens when the log is growing too slow.
        // However, if it seems like log activity has stopped (i.e.,
        // durable_lsn did not advance since we started), then we proceed
        // with the small activation window.
        bool logStopped = control.endLSN == smlevel_0::log->durable_lsn();
        if (!isSmallWindow(control.endLSN, nextActLSN) && !logStopped) {
            return false;
        }
        INC_TSTAT(la_log_slow);
        DBGTHRD(<< "Log growing too slow");
        return true;
    }
    return false;
}

bool LogArchiver::shouldActivate(bool logTooSlow)
{
    if (flushReqLSN == control.endLSN) {
        return control.endLSN > nextActLSN;
    }

    // CS TODO: temporary hack -- do not kick-off archiver until buffer pool has warmed up
    // if (!smlevel_0::bf || !smlevel_0::bf->is_warmup_done()) {
    //     return false;
    // }

    if (logTooSlow && control.endLSN == smlevel_0::log->durable_lsn()) {
        // Special case: log is not only groing too slow, but it has actually
        // halted. This means the application/experiment probably already
        // finished and is just waiting for the archiver. In that case, we
        // allow the activation with a small window. However, it may not be
        // a window of size zero (s.t. endLSN == nextActLSN)
        DBGTHRD(<< "Log seems halted -- accepting small window");
        return control.endLSN > nextActLSN;
    }

    // Try to keep activation window at block boundaries to better utilize
    // I/O bandwidth
    if (eager && readWholeBlocks && !logTooSlow) {
        size_t boundary = index->getBlockSize() *
            (control.endLSN.lo() / index->getBlockSize());
        control.endLSN = lsn_t(control.endLSN.hi(), boundary);
        if (control.endLSN <= nextActLSN) {
            return false;
        }
        if (control.endLSN.lo() == 0) {
            // If durable_lsn is at the beginning of a new log partition,
            // it can happen that at this point the file was not created
            // yet, which would cause the reader thread to fail. This does
            // not happen with eager archiving, so we should eventually
            // remove it
            return false;
        }
        DBGTHRD(<< "Adjusted activation window to block boundary " <<
                control.endLSN);
    }

    if (control.endLSN == lsn_t::null
            || control.endLSN <= nextActLSN)
    {
        DBGTHRD(<< "Archiver already passed this range. Continuing...");
        return false;
    }

    w_assert1(control.endLSN > nextActLSN);
    return true;
}

void LogArchiver::run()
{
    while(true) {
        CRITICAL_SECTION(cs, control.mutex);

        if (!waitForActivation()) {
            break;
        }
        bool logTooSlow = isLogTooSlow();

        if (processFlushRequest()) {
            continue;
        }

        if (!shouldActivate(logTooSlow)) {
            continue;
        }
        INC_TSTAT(la_activations);

        DBGOUT(<< "Log archiver activated from " << nextActLSN << " to "
                << control.endLSN);

        consumer->open(control.endLSN, readWholeBlocks && !logTooSlow);

        replacement();

        /*
         * Selection is not invoked here because log archiving should be a
         * continuous process, and so the heap should not be emptied at
         * every invocation. Instead, selection is invoked by the replacement
         * method when the heap is full. This also has the advantage that the
         * heap is kept as full as possible, which generates larger runs.
         * A consequence of this scheme is that the activation of the log
         * archiver until an LSN X just means that all log records up to X will
         * be inserted into the heap, and not that they will be persited into
         * runs. This means that log recycling must not rely on activation
         * cycles, but on signals/events generated by the writer thread (TODO)
         */

        // nextActLSN = consumer->getNextLSN();
        nextActLSN = control.endLSN;
        DBGOUT(<< "Log archiver consumed all log records until LSN "
                << nextActLSN);

        if (!eager) {
            control.endLSN = lsn_t::null;
            control.activated = false;
        }
    }

    // Perform selection until all remaining entries are flushed out of
    // the heap into runs. Last run boundary is also enqueued.
    DBGOUT(<< "Archiver exiting -- last round of selection to empty heap");
    while (selection()) {}
    DBGOUT(<< "Archiver done!");

    w_assert0(heap->size() == 0);
}

bool LogArchiver::requestFlushAsync(lsn_t reqLSN)
{
    if (reqLSN == lsn_t::null) {
        return false;
    }
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    if (flushReqLSN != lsn_t::null) {
        return false;
    }
    flushReqLSN = reqLSN;
    lintel::atomic_thread_fence(lintel::memory_order_release);

    // Other thread may race with us and win -- recheck
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    if (flushReqLSN != reqLSN) {
        return false;
    }
    return true;
}

void LogArchiver::requestFlushSync(lsn_t reqLSN)
{
    smlevel_0::log->flush(reqLSN);
    DBGTHRD(<< "Requesting flush until LSN " << reqLSN);
    if (!eager) {
        activate(reqLSN);
    }
    while (!requestFlushAsync(reqLSN)) {
        usleep(1000); // 1ms
    }
    // When log archiver is done processing the flush request, it will set
    // flushReqLSN back to null. This method only guarantees that the flush
    // request was processed. The caller must still wait for the desired run to
    // be persisted -- if it so wishes.
    while(true) {
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        if (flushReqLSN == lsn_t::null) {
            break;
        }
        ::usleep(10000); // 10ms
    }
}

void LogArchiver::archiveUntil(run_number_t run)
{
    // FINELINE
    smlevel_0::log->flush_all();
    lsn_t until = smlevel_0::log->durable_lsn();

    // wait for log record to be consumed
    while (getNextConsumedLSN() < until) {
        activate(until, true);
        ::usleep(10000); // 10ms
    }

    if (index->getLastRun() < run) {
        requestFlushSync(until);
    }
}

MergerDaemon::MergerDaemon(const sm_options& options,
        std::shared_ptr<ArchiveIndex> in, std::shared_ptr<ArchiveIndex> out)
    :
    // CS TODO: interval should come from merge policy
    worker_thread_t(0),
     indir(in), outdir(out)
{
    _fanin = options.get_int_option("sm_archiver_fanin", 5);
    _compression = options.get_int_option("sm_page_img_compression", 0) > 0;
    if (!outdir) { outdir = indir; }
    w_assert0(indir && outdir);
}

void MergerDaemon::do_work()
{
    // For now, constantly merge runs of level 1 into level 2
    doMerge(1, _fanin);
}

bool runComp(const RunId& a, const RunId& b)
{
    return a.begin < b.begin;
}

rc_t MergerDaemon::doMerge(unsigned level, unsigned fanin)
{
    list<RunId> stats, statsNext;
    indir->listFileStats(stats, level);
    indir->listFileStats(statsNext, level+1);

    if (stats.size() < fanin) {
        // CS TODO: merge policies
        DBGOUT3(<< "Not enough runs to merge: " << stats.size());
        ::sleep(1);
        return RCOK;
    }

    // sort list by run number, since only contiguous runs are merged
    stats.sort(runComp);
    statsNext.sort(runComp);

    // grab first run which is missing from next level
    auto nextRun = stats.front().begin;
    if (statsNext.size() > 0) {
        nextRun = statsNext.back().end + 1;
    }

    // collect 'fanin' runs in the current level starting from nextLSN
    auto begin = stats.begin();
    while (begin != stats.end() && nextRun > begin->begin) { begin++; }
    auto end = begin;
    unsigned count = 0;
    while (count < fanin && end != stats.end()) {
        end++;
        count++;
    }
    if (count < fanin) {
        // CS TODO: merge policies
        DBGOUT3(<< "Not enough runs to merge");
        ::sleep(1);
        return RCOK;
    }

    {
        ArchiveScan scan {outdir};
        scan.openForMerge(begin, end);
        // CS TODO: outdir may not have the same runs in the merged level,
        // which will screw up the assignment of endLSN boundaries. Here,
        // we should check that and, if needed, create an empty run from the
        // boundaries of level in indir.
        BlockAssembly blkAssemb(outdir.get(), level+1, _compression);
        outdir->openNewRun(level+1);

        constexpr int runNumber = 0;
        if (!scan.finished()) {
            logrec_t* lr;
            blkAssemb.start(runNumber);
            while (scan.next(lr)) {
                if (!blkAssemb.add(lr)) {
                    blkAssemb.finish();
                    blkAssemb.start(runNumber);
                    blkAssemb.add(lr);
                }
            }
            blkAssemb.finish();
        }

        blkAssemb.shutdown();
    }

    return RCOK;
}

