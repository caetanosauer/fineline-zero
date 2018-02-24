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

class partition_recycler_t : public worker_thread_t
{
public:
    partition_recycler_t(log_storage* storage)
        : storage(storage)
    {}

    void do_work()
    {
	storage->delete_old_partitions();
    }

    log_storage* storage;
};

/*
 * Opens log files in logdir and initializes partitions as well as the
 * given LSN's. The buffer given in prime_buf is primed with the contents
 * found in the last block of the last partition -- this logic was moved
 * from the various prime methods of the old log_core.
 */
log_storage::log_storage(const sm_options& options)
    : _curr_partition(nullptr)
{
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
    w_assert0(psize > 0);
    _partition_size = psize;

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
            _partitions[pnum]->open();

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
    if (p) { _curr_partition = p; }
}

log_storage::~log_storage()
{
    if (_recycler_thread) {
        _recycler_thread->stop();
    }
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
        p = create_partition(n+1);
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
    auto p = get_partition(pnum);
    if (p) {
        W_FATAL_MSG(eINTERNAL, << "Partition " << pnum << " already exists");
    }

    p = make_shared<partition_t>(this, pnum);
    p->open();

    w_assert3(_partitions.find(pnum) == _partitions.end());

    {
        spinlock_write_critical_section cs(&_partition_map_latch);
        w_assert1(!_curr_partition || _curr_partition->num() == pnum - 1);
        _partitions[pnum] = p;
        _curr_partition = p;
    }

    wakeup_recycler();

    return p;
}

void log_storage::wakeup_recycler()
{
    if (!_recycler_thread) {
        _recycler_thread.reset(new partition_recycler_t(this));
        _recycler_thread->fork();
    }
    _recycler_thread->wakeup();
}

unsigned log_storage::delete_old_partitions(partition_number_t older_than)
{
    if (!smlevel_0::log || !smlevel_0::bf) { return 0; }

    if (older_than == 0) {
        older_than = smlevel_0::bf->get_archived_run();
    }

    unsigned count = 0;
    {
        spinlock_write_critical_section cs(&_partition_map_latch);

        partition_map_t::iterator it = _partitions.begin();
        while (it != _partitions.end()) {
            if (it->first < older_than) {
                if (_delete_old_partitions) { it->second->mark_for_deletion(); }
                it = _partitions.erase(it);
                count++;
            }
            else { it++; }
        }

    }

    return count;
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
