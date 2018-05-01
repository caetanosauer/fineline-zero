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

const static int DFT_BLOCK_SIZE = 8 * 1024 * 1024;

LogArchiver::LogArchiver(const sm_options& options)
    : shutdownFlag(false), flushReqLSN(lsn_t::null)
{
    constexpr size_t defaultWorkspaceSize = 1600;
    size_t workspaceSize = 1024 * 1024 * // convert MB -> B
        options.get_int_option("sm_archiver_workspace_size", defaultWorkspaceSize);
    size_t archBlockSize = options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);
    bool compression = options.get_int_option("sm_page_img_compression", 0) > 0;

    index = std::make_shared<ArchiveIndex>(options);
    nextLSN = lsn_t(index->getLastRun() + 1, 0);
    w_assert1(nextLSN.hi() > 0);

    constexpr bool startFromFirstLogPartition = true;
    if (smlevel_0::log && nextLSN == lsn_t(1,0) && startFromFirstLogPartition) {
        std::vector<partition_number_t> partitions;
        smlevel_0::log->get_storage()->list_partitions(partitions);
        if (partitions.size() > 0) {
            nextLSN = lsn_t(partitions[0], 0);
        }
    }

    heap = new ArchiverHeapSimple();
    unsigned fsyncFrequency = options.get_bool_option("sm_arch_fsync_frequency", 1);
    blkAssemb = new BlockAssembly(index.get(), archBlockSize, 1 /*level*/, compression, fsyncFrequency);

    merger = nullptr;
    if (options.get_bool_option("sm_archiver_merging", false)) {
        merger = new MergerDaemon(options, index);
        merger->fork();
        merger->wakeup();
    }

    currPartition = smlevel_0::log->get_storage()->get_partition(nextLSN.hi());
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
    // We may only remove records form heap that are in the selection run or beyond
    if (run > selectionRun) { return false; }
    if (!blkAssemb->start(run)) { return false; }

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

void ArchiverHeapSimple::push(logrec_t* lr, run_number_t run)
{
    w_assert1(lr->valid_header());

    //DBGTHRD(<< "Processing logrec " << lr->lsn_ck() << ", type " <<
    //        lr->type() << "(" << lr->type_str() << ") length " <<
    //        lr->length() << " into run " << (int) currentRun);

    // insert key and pointer into w_heap
    HeapEntry k(run, lr);

    // CS: caution: AddElementDontHeapify does NOT work!!!
    w_heap.AddElement(k);
}

void ArchiverHeapSimple::pop()
{
    w_heap.RemoveFirst();
}

logrec_t* ArchiverHeapSimple::top()
{
    logrec_t* lr = (logrec_t*) w_heap.First().lr;
    w_assert1(lr->valid_header());
    return lr;
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
        if (nextLSN >= endRoundLSN) {
            nextLSN = endRoundLSN;
            break;
        }
        if (nextLSN.hi() != currPartition->num()) {
            selectionRun = currPartition->num();
            currPartition = smlevel_0::log->get_storage()->get_partition(nextLSN.hi());
        }

        auto lr = smlevel_0::log->fetch_direct(currPartition, nextLSN);

        if (lr->type() == skip_log) {
            nextLSN = lsn_t(nextLSN.hi() + 1, 0);
            continue;
        }

        auto lsn = nextLSN;
        nextLSN += lr->length();

        if (bytesReadyForSelection > blkAssemb->getBlockSize()  && heap->topRun() == selectionRun) {
            bool success = selection();
            if (success) { bytesReadyForSelection = 0; }
        }

        if (!lr->is_redo()) { continue; }

        w_assert1(lr->valid_header());
        w_assert1(lsn.hi() > 0);
        const run_number_t run = lsn.hi();
        heap->push(lr, run);

        bytesReadyForSelection += lr->length();
    }
}

void LogArchiver::run()
{
    while(true) {
        endRoundLSN = smlevel_0::log->durable_lsn();
        while (nextLSN == endRoundLSN) {
            // we're going faster than log, call selection and sleep a bit (1ms)
            selection(); // called to make sure we make progress on archiving if logging is slow or stuck
            ::usleep(1000);
            endRoundLSN = smlevel_0::log->durable_lsn();

            if (shutdownFlag) { break; }

            // Flushing requested (e.g., by restore manager)
            if (flushReqLSN != lsn_t::null) { break; }
        }

        if (endRoundLSN.lo() == 0) {
            // If durable_lsn is at the beginning of a new log partition,
            // it can happen that at this point the file was not created
            // yet, which would cause the reader thread to fail.
            continue;
        }

        if (shutdownFlag) { break; }

        INC_TSTAT(la_activations);
        DBGOUT(<< "Log archiver activated from " << nextLSN << " to " << endRoundLSN);

        replacement();

        if (flushReqLSN != lsn_t::null) {
            w_assert0(endRoundLSN >= flushReqLSN);
            // consume whole heap
            selectionRun = currPartition->num();
            while (selection()) {}
            // Heap empty: Wait for all blocks to be consumed and writen out
            w_assert0(heap->size() == 0);
            while (blkAssemb->hasPendingBlocks()) {
                ::usleep(10000); // 10ms
            }

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
        }

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

        DBGOUT(<< "Log archiver consumed all log records until LSN " << endRoundLSN);
    }

    // Perform selection until all remaining entries are flushed out of
    // the heap into runs. Last run boundary is also enqueued.
    DBGOUT(<< "Archiver exiting -- last round of selection to empty heap");
    selectionRun = currPartition->num();
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
    while (nextLSN < until) {
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
    _blockSize = options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);
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
        BlockAssembly blkAssemb(outdir.get(), _blockSize, level+1, _compression);
        outdir->openNewRun(level+1);

        constexpr int runNumber = 1;
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

