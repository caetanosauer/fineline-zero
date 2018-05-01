#ifndef LOGARCHIVER_H
#define LOGARCHIVER_H

#include "worker_thread.h"
#include "sm_base.h"
#include "logarchive_index.h"
#include "logarchive_writer.h"
#include "w_heap.h"
#include "log_storage.h"
#include "mem_mgmt.h"

#include <queue>
#include <set>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

class sm_options;
class LogScanner;

/**
 * Version of ArchiverHeap that does not use an internal memory manager, instead storing the given
 * logrec_t pointers directly. Works very well if the log is scanned with mmap.
 * Only stores log records of a single run! (i.e., no run number in heap entries)
 */
class ArchiverHeapSimple
{
public:
    ArchiverHeapSimple() : w_heap{heapCmp} {};

    logrec_t* top();
    void pop();
    run_number_t topRun() { return w_heap.First().run; }
    size_t size() { return w_heap.NumElements(); }
    void push(logrec_t* lr, run_number_t run);

private:
    struct HeapEntry {
        logrec_t* lr;
        // Normalized key -- TODO
        // uint64_t key;
        PageID pid;
        uint32_t version;
        run_number_t run;

        HeapEntry() : lr{nullptr}, pid{0}, version{0}, run{0} {};

        HeapEntry(run_number_t run, logrec_t* lr) : lr{lr}, pid{lr->pid()}, version{lr->page_version()}, run{run}
        {
            // // page version (4B)
            // key = static_cast<uint64_t>(lr->version());
            // // page id (4B)
            // key &= (static_cast<uint64_t>(lr->pid()) << 32);
        }

        struct Cmp {
            bool gt(const HeapEntry& a, const HeapEntry& b) const {
                if (a.run != b.run) {
                    return a.run < b.run;
                }
                if (a.pid != b.pid) {
                    return a.pid< b.pid;
                }
                return a.version < b.version;
            }
        };
    };

    HeapEntry::Cmp heapCmp;
    Heap<HeapEntry, HeapEntry::Cmp> w_heap;
};

/**
 * Basic service to merge existing log archive runs into larger ones.
 * Currently, the merge logic only supports the *very limited* use case of
 * merging all N run files into a smaller n, depending on a given fan-in
 * and size limits. Currently, it is used simply to run our restore
 * experiments with different number of runs for the same log archive
 * volume.
 *
 * In a proper implementation, we have to support useful policies, with the
 * restriction that only consecutive runs can be merged. The biggest
 * limitation right now is that we reuse the logic of BlockAssembly, but
 * its control logic -- especially the coordination with the WriterThread
 * -- is quite restricted to the usual case of a consumption of log records
 * from the standard recovery log, i.e., ascending LSNs and run numbers,
 * startLSN coming from the existing run files, etc. We have to make that
 * logic clever and more abstract; or simply don't reuse the BlockAssembly
 * infrastructure.
 */
class MergerDaemon : public worker_thread_t {
public:
    MergerDaemon(const sm_options&,
        std::shared_ptr<ArchiveIndex> in,
        std::shared_ptr<ArchiveIndex> ou = nullptr);

    virtual ~MergerDaemon() {}

    virtual void do_work();

    rc_t doMerge(unsigned level, unsigned fanin);

private:
    std::shared_ptr<ArchiveIndex> indir;
    std::shared_ptr<ArchiveIndex> outdir;
    unsigned _fanin;
    bool _compression;
    size_t _blockSize;
};

/** \brief Implementation of a log archiver using asynchronous reader and
 * writer threads.
 *
 * The log archiver runs as a background daemon whose execution is controlled
 * by an ArchiverControl object. Once a log archiver thread is created and
 * forked, it waits for an activation to start working. The caller thread must
 * invoke the activate() method to perform this activation.
 *
 * Log archiving works in <b>activation cycles</b>, in which it first waits for
 * an activation and then consumes the recovery log up to a given LSN value
 * (\see activate(bool, lsn_t)).  This cycle is executed in an infinite loop
 * until the method shutdown() is invoked.  Once shutdown is invoked, the
 * current cycle is <b>not</b> interrupted. Instead, it finishes consuming the
 * log until the LSN given in the last successful activation and only then it
 * exits. The destructor also invokes shutdown() if not done yet.
 *
 * The class LogArchiver itself serves merely as an orchestrator of its
 * components, which are:
 * - LogArchiver::LogConsumer, which encapsulates a reader thread and parsing
 *   individual log records from the recovery log.
 * - LogArchiver::ArchiverHeap, which performs run generation by sorting the
 *   input stream given by the log consumer.
 * - LogArchiver::BlockAssembly, which consumes the sorted output from the
 *   heap, builds indexed blocks of log records (used for instant restore), and
 *   passes them over to the asynchronous writer thread
 * - LogArchiver::ArchiveDirectory, which represents the set of sorted runs
 *   that compose the log archive itself. It manages filesystem operations to
 *   read from and write to the log archive, controls access to the archive
 *   index, and provides scanning facilities used by restore.
 *
 * One activation cycle consists of consuming all log records from the log
 * consumer, which must first be opened with the given "end LSN". Each log
 * record is then inserted into the heap until it becomes full. Then, log
 * records are removed from the heap (usually in bulk, e.g., one block at a
 * time) and passed to the block assembly component. The cycle finishes once
 * all log records up to the given LSN are <b>inserted into the heap</b>, which
 * does not necessarily mean that the persistent log archive will contain all
 * those log records. The only way to enforce that is to perform a shutdown.
 * This design maintains the heap always as full as possible, which generates
 * runs whose size is (i) as large as possible and (ii) independent of the
 * activation behavior.
 *
 * In the typical operation mode, a LogArchiver instance is constructed using
 * the sm_options provided by the user, but for tests and external experiments,
 * it can also be constructed by passing instances of these four components
 * above.
 *
 * A note on processing older log partitions (TODO): Before we implemented the
 * archiver, the log manager would delete a partition once it was eliminated
 * from the list of 8 open partitions. The compiler flag KEEP_LOG_PARTITIONS
 * was used to omit the delete operation, leaving the complete history of the
 * database in the log directory. However, if log archiving is enabled, it
 * should take over the responsibility of deleting old log partitions.
 * Currently, if the flag is not set and the archiver cannot keep up with the
 * growth of the log, partitions would be lost from archiving.
 *
 * \sa LogArchiver::LogConsumer
 * \sa LogArchiver::ArchiverHeap
 * \sa LogArchiver::BlockAssembly
 * \sa LogArchiver::ArchiveDirectory
 *
 * \author Caetano Sauer
 */
class LogArchiver : public thread_wrapper_t {
public:
    LogArchiver(const sm_options& options);
    virtual ~LogArchiver();

    virtual void run();
    void activate(lsn_t endLSN = lsn_t::null, bool wait = true);
    void shutdown();
    bool requestFlushAsync(lsn_t);
    void requestFlushSync(lsn_t);
    void archiveUntil(run_number_t);

    std::shared_ptr<ArchiveIndex> getIndex() { return index; }
    lsn_t getNextConsumedLSN() { return nextLSN; }

    /*
     * IMPORTANT: the block size must be a multiple of the log
     * page size to ensure that logrec headers are not truncated
     */
    const static bool DFT_EAGER = true;
    const static bool DFT_READ_WHOLE_BLOCKS = true;
    const static int DFT_GRACE_PERIOD = 1000000; // 1 sec

private:
    std::shared_ptr<ArchiveIndex> index;
    ArchiverHeapSimple* heap;
    BlockAssembly* blkAssemb;
    MergerDaemon* merger;

    std::atomic<bool> shutdownFlag;
    lsn_t flushReqLSN;
    lsn_t nextLSN;
    lsn_t endRoundLSN;
    shared_ptr<partition_t> currPartition;
    run_number_t selectionRun = 0;
    size_t bytesReadyForSelection = 0;

    void replacement();
    bool selection();

};

#endif
