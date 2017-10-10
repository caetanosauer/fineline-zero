/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

// CS TODO: this has to come before sm_base because w_base.h defines
// a macro called "align", which is probably the name of a function
// or something inside boost regex
#include <boost/regex.hpp>

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <atomic>
#include <thread>
#include <chrono>
#include <algorithm>

#include "w_defines.h"
#include "sm_base.h"
#include "bf_tree.h"
#include "log_storage.h"
#include "log_core.h"
#include "latches.h"

const string log_storage::log_prefix = "log.";
const string log_storage::log_regex = "log\\.[1-9][0-9]*";

class partition_recycler_t : public thread_wrapper_t
{
public:
    partition_recycler_t(log_storage* storage)
        : storage(storage), retire(false)
    {}

    virtual ~partition_recycler_t() {}

    void run()
    {
        while (!retire) {
            unique_lock<mutex> lck(_recycler_mutex);
            _recycler_condvar.wait(lck);
            if (retire) { break; }
            storage->delete_old_partitions();
        }
    }

    void wakeup()
    {
        unique_lock<mutex> lck(_recycler_mutex);
        _recycler_condvar.notify_one();
    }

    log_storage* storage;

    std::atomic<bool> retire;
    std::condition_variable _recycler_condvar;
    std::mutex _recycler_mutex;
};

/*
 * Opens log files in logdir and initializes partitions as well as the
 * given LSN's. The buffer given in prime_buf is primed with the contents
 * found in the last block of the last partition -- this logic was moved
 * from the various prime methods of the old log_core.
 */
log_storage::log_storage(const sm_options& options)
{
    // CS TODO: log record refactoring
    _skip_log = new logrec_t;
    _skip_log->init_header(skip_log);
    // _skip_log->construct();

    std::string logdir = options.get_string_option("sm_logdir", "log");
    if (logdir.empty()) {
        cerr << "ERROR: sm_logdir must be set to enable logging." << endl;
        W_FATAL(eCRASH);
    }
    _logpath = logdir;

    bool reformat = options.get_bool_option("sm_format", false);

    if (!fs::exists(_logpath)) {
        if (reformat) {
            fs::create_directories(_logpath);
        } else {
            cerr << "Error: could not open the log directory " << logdir <<endl;
            W_COERCE(RC(eOS));
        }
    }

    off_t psize = off_t(options.get_int_option("sm_log_partition_size", 1024));
    // option given in MB -> convert to B
    psize = psize * 1024 * 1024;
    // round to next multiple of the log buffer segment size
    psize = (psize / log_core::SEGMENT_SIZE) * log_core::SEGMENT_SIZE;
    _partition_size = psize;

    // maximum number of partitions on the filesystem
    _max_partitions = options.get_int_option("sm_log_max_partitions", 0);

    _delete_old_partitions = options.get_bool_option("sm_log_delete_old_partitions", true);

    partition_number_t  last_partition = 1;

    fs::directory_iterator it(_logpath), eod;
    boost::regex log_rx(log_regex, boost::regex::basic);
    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();

        if (boost::regex_match(fname, log_rx)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }

            long pnum = std::stoi(fname.substr(log_prefix.length()));
            _partitions[pnum] = make_shared<partition_t>(this, pnum);

            if (pnum >= last_partition) {
                last_partition = pnum;
            }
        }
        else {
            cerr << "log_storage: cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }

    }

    auto p = get_partition(last_partition);
    if (!p) {
        create_partition(last_partition);
        p = get_partition(last_partition);
        w_assert0(p);
    }

    W_COERCE(p->open());
    _curr_partition = p;

    if(!p) {
        cerr << "ERROR: could not open log file for partition "
            << last_partition << endl;
        W_FATAL(eINTERNAL);
    }

    w_assert3(p->num() == last_partition);
}

log_storage::~log_storage()
{
    if (_recycler_thread) {
        _recycler_thread->retire = true;
        _recycler_thread->wakeup();
        _recycler_thread->join();
        _recycler_thread = nullptr;
    }

    spinlock_write_critical_section cs(&_partition_map_latch);

    partition_map_t::iterator it = _partitions.begin();
    while (it != _partitions.end()) {
        auto p = it->second;
        p->close();
        it++;
    }

    _partitions.clear();

    delete _skip_log;
}

shared_ptr<partition_t> log_storage::get_partition_for_flush(lsn_t start_lsn,
        long start1, long end1, long start2, long end2)
{
    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    // time to open a new partition? (used to be in log_core::insert,
    // now called by log flush daemon)
    // This will open a new file when the given start_lsn has a
    // different file() portion from the current partition()'s
    // partition number, so the start_lsn is the clue.
    auto p = curr_partition();
    if(start_lsn.file() != p->num()) {
        partition_number_t n = p->num();
        w_assert3(start_lsn.file() == n+1);
        w_assert3(n != 0);

        {
            W_COERCE(p->close());
            p = create_partition(n+1);
            W_COERCE(p->open());
        }
    }

    return p;
}

shared_ptr<partition_t> log_storage::get_partition(partition_number_t n) const
{
    spinlock_read_critical_section cs(&_partition_map_latch);
    partition_map_t::const_iterator it = _partitions.find(n);
    if (it == _partitions.end()) { return nullptr; }
    return it->second;
}

shared_ptr<partition_t> log_storage::create_partition(partition_number_t pnum)
{
    // we should also free up if necessary, as done in close_min
    auto p = get_partition(pnum);
    if (p) {
        W_FATAL_MSG(eINTERNAL, << "Partition " << pnum << " already exists");
    }

    p = make_shared<partition_t>(this, pnum);
    p->set_size(0);

    w_assert3(_partitions.find(pnum) == _partitions.end());

    {
        // Add partition to map but only exit function once it has been
        // reduced to _max_partitions
        spinlock_write_critical_section cs(&_partition_map_latch);
        w_assert1(!_curr_partition || _curr_partition->num() == pnum - 1);
        _partitions[pnum] = p;
        _curr_partition = p;
    }

    wakeup_recycler();

    // The check below does not require the mutex
    if (_max_partitions > 0 && _partitions.size() > _max_partitions) {
        // Log full! Try to clean-up old partitions.
        try_delete(pnum);
    }

    return p;
}

void log_storage::wakeup_recycler()
{
    if (!_delete_old_partitions) { return; }

    if (!_recycler_thread) {
        _recycler_thread.reset(new partition_recycler_t(this));
        _recycler_thread->fork();
    }
    _recycler_thread->wakeup();
}

unsigned log_storage::delete_old_partitions(partition_number_t older_than)
{
    if (!smlevel_0::log || !smlevel_0::bf || !_delete_old_partitions) { return 0; }

    if (older_than == 0) {
        older_than = smlevel_0::bf->get_archived_run();
    }

    list<shared_ptr<partition_t>> to_be_deleted;
    partition_number_t highest_deleted = 0;

    {
        spinlock_write_critical_section cs(&_partition_map_latch);

        partition_map_t::iterator it = _partitions.begin();
        while (it != _partitions.end()) {
            if (it->first < older_than) {
                if (it->first > highest_deleted) {
                    highest_deleted = it->first;
                }
                to_be_deleted.push_front(it->second);
                it = _partitions.erase(it);
            }
            else { it++; }
        }

    }

    // Waint until the partitions to be deleted are not referenced anymore
    while (to_be_deleted.size() > 0) {
        auto p = to_be_deleted.front();
        to_be_deleted.pop_front();
        while (!p.unique()) {
            std::this_thread::sleep_for(chrono::milliseconds(1));
        }
        // Now this partition is owned exclusively by me.  Other threads cannot
        // increment reference counters because objects were removed from map,
        // and the critical section above guarantees visibility.
        p->destroy();
    }

    return to_be_deleted.size();
}

shared_ptr<partition_t> log_storage::curr_partition() const
{
    spinlock_read_critical_section cs(&_partition_map_latch);
    return _curr_partition;
}

void log_storage::list_partitions(std::vector<partition_number_t>& vec) const
{
    vec.clear();
    {
        spinlock_read_critical_section cs(&_partition_map_latch);

        for (auto p : _partitions) {
            vec.push_back(p.first);
        }
    }
    std::sort(vec.begin(), vec.end());
}

string log_storage::make_log_name(partition_number_t pnum) const
{
    return make_log_path(pnum).string();
}

fs::path log_storage::make_log_path(partition_number_t pnum) const
{
    return _logpath / fs::path(log_prefix + to_string(pnum));
}

void log_storage::try_delete(partition_number_t pnum)
{
    /*
     * Log full -- we must delete a partition before continuing.  But we can't
     * invoke normal checkpoint & cleaner because they will attempt to generate
     * log records and block as well.  First we check if the oldest active
     * transaction (as known by the last checkpoint) has its begin in the
     * oldest partition file. If that's true, then no partition can be deleted
     * and we are stuck -- in other words, the log is "wedged". To avoid this,
     * a log space reservations scheme is required, but since we removed the
     * old and messy scheme, we must fail here. Since this is a research
     * prototype and this is quite a corner case, we don't worry too much about
     * it.
     */
    lsn_t min_xct_lsn = smlevel_0::log->get_oldest_active_lsn();
    if (min_xct_lsn.hi() == pnum - _max_partitions) {
        throw runtime_error("Log wedged! Cannot recycle partitions due to \
                old active transaction");
    }

    /*
     * Now check if any dirty page rec_lsn is in the oldest partition. If
     * that's true, then we're also stuck like above, because our cleaning
     * & checkpoint mechanisms require generating log records. We could simply
     * force all dirty pages from the buffer pool without generating log
     * records -- that would mean that those older log records would not be
     * required for recovery. However, the log analysis logic would not know
     * that without page_write log records. Again, it seems like the solution
     * is to have a reservation scheme, where enough log space is always reserved
     * for a full page cleaner round (e.g., one logrec for each frame)
     */
    lsn_t min_rec_lsn = lsn_t(smlevel_0::bf->get_archived_run() + 1, 0);
    if (min_rec_lsn.hi() == pnum - _max_partitions) {
        throw runtime_error("Log wedged! Cannot recycle partitions due to \
                old dirty pages");
    }

    /*
     * Once we get here, we must be able to delete at least one partition
     * CS-TODO: there's potentially a deadlock here, since
     * delete_old_partitions will wait until the partition's shared_ptr has no
     * other references -- if a thread is holding a reference but waiting to
     * insert something in the full log, we get stuck.
     */
    unsigned deleted = delete_old_partitions();
    if (deleted == 0) {
        throw runtime_error("Log wedged! Cannot recycle partitions with \
                the available checkpoint information. Try increasing \
                max_partitions or partition_size.");
    }
}

size_t log_storage::get_byte_distance(lsn_t a, lsn_t b) const
{
    if (a.is_null()) { a = lsn_t(1,0); }
    if (b.is_null()) { b = lsn_t(1,0); }
    if (a > b) { std::swap(a,b); }

    if (a.hi() == b.hi()) {
        return b.lo() - a.lo();
    }
    else {
        size_t rest = b.lo() + (_partition_size - a.lo());
        return _partition_size * (b.hi() - a.hi() - 1) + rest;
    }
}
