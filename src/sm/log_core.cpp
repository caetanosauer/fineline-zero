/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore'>

 $Id: log_core.cpp,v 1.20 2010/12/08 17:37:42 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOG_CORE_C

#include "thread_wrapper.h"
#include "sm_options.h"
#include "sm_base.h"
#include "logrec.h"
#include "log_core.h"
#include "log_carray.h"
#include "log_lsn_tracker.h"
#include "xct_logger.h"
#include "bf_tree.h"
#include "fixable_page_h.h"

#include <algorithm>
#include <sstream>
#include <fstream>

// files and stuff
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

class ticker_thread_t : public thread_wrapper_t
{
public:
    ticker_thread_t(bool msec = false, bool print_tput = false)
        : msec(msec), print_tput(print_tput), even_round(true)
    {
        interval_usec = 1000; // 1ms
        if (!msec) { interval_usec *= 1000; }
        stop = false;

        stats[0].fill(0);
        stats[1].fill(0);
    }

    void shutdown()
    {
        stop = true;
    }

    void run()
    {
        std::ofstream ofs;
        std::ofstream ofs2;
        if (print_tput) {
            ofs.open("tput.txt", std::ofstream::out | std::ofstream::trunc);
            // ofs2.open("evict_time.txt", std::ofstream::out | std::ofstream::trunc);
        }

        while (true) {
            if (stop) { break; }

            std::this_thread::sleep_for(std::chrono::microseconds(interval_usec));

            if (print_tput) {
                auto& st = stats[even_round ? 0 : 1];
                auto& prev_st = stats[even_round ? 1 : 0];

                ss_m::gather_stats(st);
                auto diff = st[enum_to_base(sm_stat_id::commit_xct_cnt)] -
                    prev_st[enum_to_base(sm_stat_id::commit_xct_cnt)];
                ofs << diff << std::endl;

                // auto duration = st[enum_to_base(sm_stat_id::bf_evict_duration)] -
                //     prev_st[enum_to_base(sm_stat_id::bf_evict_duration)];
                // auto count = st[enum_to_base(sm_stat_id::bf_evict)] -
                //     prev_st[enum_to_base(sm_stat_id::bf_evict)];
                // auto evict_attempts = st[enum_to_base(sm_stat_id::bf_eviction_attempts)] -
                //     prev_st[enum_to_base(sm_stat_id::bf_eviction_attempts)];
                // auto evict_time = count > 0 ? (duration / count) : 0;

                // ofs2 << evict_time << "\t" << evict_attempts << std::endl;
            }

            if (msec) { Logger::log_sys<tick_msec_log>(); }
            else { Logger::log_sys<tick_sec_log>(); }

            even_round ^= true;
        }

        if (print_tput) { ofs.close(); }
        // if (print_tput) { ofs2.close(); }
    }

private:
    int interval_usec;
    bool msec;
    bool print_tput;
    std::atomic<bool> stop;

    std::array<sm_stats_t, 2> stats;
    bool even_round;
};

class flush_daemon_thread_t : public thread_wrapper_t
{
    log_core* _log;
public:
    flush_daemon_thread_t(log_core* log) :
         _log(log)
    {
        smthread_t::set_lock_timeout(timeout_t::WAIT_NOT_USED);
    }

    virtual void run() { _log->flush_daemon(); }
};

void log_core::start_flush_daemon()
{
    _flush_daemon_running = true;
    _flush_daemon->fork();
}

logrec_t* log_core::fetch_direct(shared_ptr<partition_t> p, lsn_t lsn)
{
    logrec_t* res;
    w_assert1(p);
    w_assert1(p->num() == lsn.hi());
    p->read(res, lsn);
    w_assert1(res->valid_header());
    return res;
}

void log_core::shutdown()
{
    // gnats 52:  RACE: We set _shutting_down and signal between the time
    // the daemon checks _shutting_down (false) and waits.
    //
    // So we need to notice if the daemon got the message.
    // It tells us it did by resetting the flag after noticing
    // that the flag is true.
    // There should be no interference with these two settings
    // of the flag since they happen strictly in that sequence.
    //
    _shutting_down = true;
    while (*&_shutting_down) {
        CRITICAL_SECTION(cs, _wait_flush_lock);
        // The only thread that should be waiting
        // on the _flush_cond is the log flush daemon.
        // Yet somehow we wedge here.
        DO_PTHREAD(pthread_cond_broadcast(&_flush_cond));
    }
    _flush_daemon->join();
    _flush_daemon_running = false;
    delete _flush_daemon;
    _flush_daemon=NULL;
}

/*********************************************************************
 *
 *  log_core::log_core(bufsize, reformat)
 *
 *  Open and scan logdir for master lsn and last log file.
 *  Truncate last incomplete log record (if there is any)
 *  from the last log file.
 *
 *********************************************************************/
log_core::log_core(const sm_options& options)
    :
      _start(0),
      _end(0),
      _waiting_for_flush(false),
      _shutting_down(false),
      _flush_daemon_running(false)
{
    _segsize = SEGMENT_SIZE;

    DO_PTHREAD(pthread_mutex_init(&_wait_flush_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_wait_cond, NULL));
    DO_PTHREAD(pthread_cond_init(&_flush_cond, NULL));

    uint32_t carray_slots = options.get_int_option("sm_carray_slots",
                        ConsolidationArray::DEFAULT_ACTIVE_SLOT_COUNT);
    _carray = new ConsolidationArray(carray_slots);

    /* Create thread o flush the log */
    _flush_daemon = new flush_daemon_thread_t(this);

    _buf = new char[_segsize];

    _storage = new log_storage(options);

    auto curr_p = _storage->curr_partition();
    auto pnum = (curr_p ? curr_p->num() : 0) + 1;
    auto p = _storage->create_partition(pnum);
    _curr_lsn = _durable_lsn = _flush_lsn = lsn_t(pnum, 0);
    cerr << "Initialized curr_lsn to " << _curr_lsn << endl;

    _oldest_lsn_tracker = new PoorMansOldestLsnTracker(1 << 20);

    /* FRJ: the new code assumes that the buffer is always aligned
       with some buffer-sized multiple of the partition, so we need to
       return how far into the current segment we are.
        CS: moved this code from the _prime method
     */
    long offset = _durable_lsn.lo() % segsize();
    long base = _durable_lsn.lo() - offset;
    lsn_t start_lsn(_durable_lsn.hi(), base);

    // This should happend only in recovery/startup case.  So let's assert
    // that there is no log daemon running yet. If we ever fire this
    // assert, we'd better see why and it means we might have to protect
    // _cur_epoch and _start/_end with a critical section on _insert_lock.
    w_assert1(_flush_daemon_running == false);
    _buf_epoch = _cur_epoch = epoch(start_lsn, base, offset, offset);
    _end = _start = _durable_lsn.lo();

    _ticker = NULL;
    if (options.get_bool_option("sm_ticker_enable", false)) {
        bool msec = options.get_bool_option("sm_ticker_msec", false);
        bool print_tput = options.get_bool_option("sm_ticker_print_tput", false);
        _ticker = new ticker_thread_t(msec, print_tput);
    }

    _group_commit_size = options.get_int_option("sm_group_commit_size", 0);
    _group_commit_timeout = options.get_int_option("sm_group_commit_timeout", 0);

    _page_img_compression = options.get_int_option("sm_page_img_compression", 0);

    directIO = options.get_bool_option("sm_log_o_direct", false);

    if (1) {
        cerr << "Log _start " << start_byte() << " end_byte() " << end_byte() << endl
            << "Log _curr_lsn " << _curr_lsn << " _durable_lsn " << _durable_lsn << endl;
        cerr << "Curr epoch  base_lsn " << _cur_epoch.base_lsn
            << "  base " << _cur_epoch.base
            << "  start " << _cur_epoch.start
            << "  end " << _cur_epoch.end << endl;
        cerr << "Old epoch  base_lsn " << _old_epoch.base_lsn
            << "  base " << _old_epoch.base
            << "  start " << _old_epoch.start
            << "  end " << _old_epoch.end << endl;
    }
}

rc_t log_core::init()
{
    // Consider this the beginning of log analysis so that
    // we can factor in the time it takes to load the fetch buffers
    if (_ticker) {
        _ticker->fork();
    }
    start_flush_daemon();

    return RCOK;
}

log_core::~log_core()
{
    if (_ticker) {
        _ticker->shutdown();
        _ticker->join();
        delete _ticker;
    }

    delete _storage;
    delete _oldest_lsn_tracker;

    delete [] _buf;
    _buf = NULL;

    delete _carray;

    DO_PTHREAD(pthread_mutex_destroy(&_wait_flush_lock));
    DO_PTHREAD(pthread_cond_destroy(&_wait_cond));
    DO_PTHREAD(pthread_cond_destroy(&_flush_cond));
}

void log_core::_acquire_buffer_space(CArraySlot* info, long recsize)
{
    w_assert2(recsize > 0);


  /* Copy our data into the log buffer and update/create epochs.
   * Re: Racing flush daemon over
   * epochs, _start, _end, _curr_lsn, _durable_lsn :
   *
   * _start is set by  _prime at startup (no log flush daemon yet)
   *                   and flush_daemon_work, not by insert
   * _end is set by  _prime at startup (no log flush daemon yet)
   *                   and insert, not by log flush daemon
   * _old_epoch is  protected by _flush_lock
   * _cur_epoch is  protected by _flush_lock EXCEPT
   *                when insert does not wrap, it sets _cur_epoch.end,
   *                which is only read by log flush daemon
   *                The _end is only set after the memcopy is done,
   *                so this should be safe.
   * _curr_lsn is set by  insert (and at startup)
   * _durable_lsn is set by flush daemon
   *
   * NOTE: _end, _start, epochs updated in
   * _prime(), but that is called only for
   * opening a partition for append in startup/recovery case,
   * in which case there is no race to be had.
   *
   * It is also updated below.
   */

    /*
    * Make sure there's actually space available in the
    * log buffer,
    * accounting for the fact that flushes (by the daemon)
    * always work with full blocks. (they round down start of
    * flush to beginning of block and round up/pad end of flush to
    * end of block).
    *
    * If not, kick the flush daemon to make space.
    */
    // CS: TODO commented out waiting-for-space stuff
    //while(*&_waiting_for_space ||
    while(
            end_byte() - start_byte() + recsize > segsize() - 2* log_storage::BLOCK_SIZE)
    {
        _insert_lock.release(&info->me);
        {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            while(end_byte() - start_byte() + recsize > segsize() - 2* log_storage::BLOCK_SIZE)
            {
                // CS: changed from waiting_for_space to waiting_for_flush
                _waiting_for_flush = true;
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                DO_PTHREAD(pthread_cond_wait(&_wait_cond, &_wait_flush_lock));
            }
        }
        _insert_lock.acquire(&info->me);
    }
    // lfence because someone else might have entered and left during above release/acquire.
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    // Having ics now should mean that even if another insert snuck in here,
    // we're ok since we recheck the condition. However, we *could* starve here.


  /* _curr_lsn, _cur_epoch.end, and end_byte() are all strongly related.
   *
   * _curr_lsn is the lsn of first byte past the tail of the log.
   *    Tail of the log is the last byte of the last record inserted (does not
   *    include the skip_log record).  Inserted records go to _curr_lsn,
   *    _curr_lsn moves with records inserted.
   *
   * _cur_epoch.end and end_byte() are convenience variables to avoid doing
   * expensive operations like modulus and division on the _curr_lsn
   * (lsn of next record to be inserted):
   *
   * _cur_epoch.end is the position of the current lsn relative to the
   *    segsize() log buffer.
   *    It is relative to the log buffer, and it wraps (modulo the
   *    segment size, which is the log buffer size). ("spill")
   *
   * end_byte()/_end is the non-wrapping version of _cur_epoch.end:
   *     at log init time it is set to a value in the range [0, segsize())
   *     and is  incremented by every log insert for the lifetime of
   *     the database.
   *     It is a byte-offset from the beginning of the log, considering that
   *     partitions are not "contiguous" in this sense.  Each partition has
   *     a lot of byte-offsets that aren't really in the log because
   *     we limit the size of a partition.
   *
   * _durable_lsn is the lsn of the first byte past the last
   *     log record written to the file (not counting the skip log record).
   *
   * _cur_epoch.start and start_byte() are convenience variables to avoid doing
   * expensive operations like modulus and division on the _durable_lsn
   *
   * _cur_epoch.start is the position of the durable lsn relative to the
   *     segsize() log buffer.   Because the _cur_epoch.end wraps, start
   *     could become > end and this would create a mess.  For this
   *     reason, when the log buffer wraps, we create a new
   *     epoch. The old epoch represents the entire unflushed
   *     portion of the old log buffer (including a portion of the
   *     presently-inserted log record) and the new epoch represents
   *     the next segment (logically, log buffer), containing
   *     the wrapped portion of the presently-inserted log record.
   *
   *     If, however, by starting a new segment to handle the wrap, we
   *     would exceed the partition size, we create a new epoch and
   *     new segment to hold the entire log record -- log records do not
   *     span partitions.   So we make the old epoch represent
   *     the unflushed portion of the old log buffer (not including any
   *     part of this record) and the new epoch represents the first segment
   *     in the new partition, and contains the entire presently-inserted
   *     log record.  We write the inserted log record at the beginning
   *     of the log buffer.
   *
   * start_byte()/_start is the non-wrapping version of _cur_epoch.start:
   *     At log init time it is set to 0
   *     and is bumped to match the durable lsn by every log flush.
   *     It is a byte-offset from the beginning of the log, considering that
   *     partitions are not "contiguous" in this sense.  Each partition has
   *     a lot of byte-offsets that aren't really in the log because
   *     we limit the size of a partition.
   *
   * start_byte() through end_byte() tell us the unflushed portion of the
   * log.
   */

  /* An epoch fits within a segment */
    w_assert2(_buf_epoch.end >= 0 && _buf_epoch.end <= segsize());

  /* end_byte() is the byte-offset-from-start-of-log-file
   * version of _cur_epoch.end */
    w_assert2(_buf_epoch.end % segsize() == end_byte() % segsize());

  /* _curr_lsn is the lsn of the next-to-be-inserted log record, i.e.,
   * the next byte of the log to be written
   */
  /* _cur_epoch.end is the offset into the log buffer of the _curr_lsn */
    w_assert2(_buf_epoch.end % segsize() == _curr_lsn.lo() % segsize());
  /* _curr_epoch.end should never be > segsize at this point;
   * that would indicate a wraparound is in progress when we entered this
   */
    w_assert2(end_byte() >= start_byte());

    // The following should be true since we waited on a condition
    // variable while
    // end_byte() - start_byte() + recsize > segsize() - 2*BLOCK_SIZE
    w_assert2(end_byte() - start_byte() <= segsize() - 2* log_storage::BLOCK_SIZE);


    long end = _buf_epoch.end;
    long old_end = _buf_epoch.base + end;
    long new_end = end + recsize;
    // set spillsize to the portion of the new record that
    // wraps around to the beginning of the log buffer(segment)
    long spillsize = new_end - segsize();
    lsn_t curr_lsn = _curr_lsn;
    lsn_t next_lsn = _buf_epoch.base_lsn + new_end;
    long new_base = -1;
    long start_pos = end;

    if(spillsize <= 0) {
        // update epoch for next log insert
        _buf_epoch.end = new_end;
    }
    // next_lsn is first byte after the tail of the log.
    else if(next_lsn.lo() <= _storage->get_partition_size()) {
        // wrap within a partition
        _buf_epoch.base_lsn += _segsize;
        _buf_epoch.base += _segsize;
        _buf_epoch.start = 0;
        _buf_epoch.end = new_end = spillsize;
    }
    else {
        curr_lsn = first_lsn(next_lsn.hi()+1);
        next_lsn = curr_lsn + recsize;
        new_base = _buf_epoch.base + _segsize;
        start_pos = 0;
        _buf_epoch = epoch(curr_lsn, new_base, 0, new_end=recsize);
    }

    // let the world know
    _curr_lsn = next_lsn;
    _end = _buf_epoch.base + new_end;

    _carray->join_expose(info);
    _insert_lock.release(&info->me);

    info->lsn = curr_lsn; // where will we end up on disk?
    info->old_end = old_end; // lets us serialize with our predecessor after memcpy
    info->start_pos = start_pos; // != old_end when partitions wrap
    info->pos = start_pos + recsize; // coordinates groups of threads sharing a log allocation
    info->new_end = new_end; // eventually assigned to _cur_epoch
    info->new_base = new_base; // positive if we started a new partition
    info->error = w_error_ok;
}

/**
 * Finish current log partition and start writing to a new one.
 */
rc_t log_core::truncate()
{
    // We want exclusive access to the log, so no CArray
    mcs_lock::qnode me;
    _insert_lock.acquire(&me);

    // create new empty epoch at the beginning of the new partition
    _curr_lsn = first_lsn(_buf_epoch.base_lsn.hi()+1);
    long new_base = _buf_epoch.base + _segsize;
    _buf_epoch = epoch(_curr_lsn, new_base, 0, 0);
    _end = new_base;

    // Update epochs so flush daemon can take over
    {
        CRITICAL_SECTION(cs, _flush_lock);
        // CS TODO: I don't know why this assertion should hold,
        // but here it fails sometimes.
        // w_assert3(_old_epoch.start == _old_epoch.end);
        _old_epoch = _cur_epoch;
        _cur_epoch = epoch(_curr_lsn, new_base, 0, 0);
    }

    _insert_lock.release(&me);

    return RCOK;
}

lsn_t log_core::_copy_to_buffer(logrec_t &rec, long pos, long recsize, CArraySlot* info)
{
    /*
      do the memcpy (or two)
    */
    lsn_t rlsn = info->lsn + pos;
    // CS FINELINE TODO: xct_end must have lsn as member; fix log priming
    // rec.set_lsn_ck(rlsn);

    _copy_raw(info, pos, (char const*) &rec, recsize);

    return rlsn;
}

bool log_core::_update_epochs(CArraySlot* info) {
    w_assert1(info->vthis()->count == ConsolidationArray::SLOT_FINISHED);
    // Wait for our predecessor to catch up if we're ahead.
    // Even though the end pointer we're checking wraps regularly, we
    // already have to limit each address in the buffer to one active
    // writer or data corruption will result.
    if (CARRAY_RELEASE_DELEGATION) {
        if(_carray->wait_for_expose(info)) {
            return true; // we delegated!
        }
    } else {
        // If delegated-buffer-release is off, we simply spin until predecessors complete.
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        while(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base != info->old_end);
    }

    // now update the epoch(s)
    while (info != NULL) {
        w_assert1(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base == info->old_end);
        if(info->new_base > 0) {
            // new partition! update epochs to reflect this

            // I just wrote part of the log record to the beginning of the
            // log buffer. How do I know that it didn't interfere with what
            // the flush daemon is writing? Because at the beginning of
            // this method, I waited until the log flush daemon ensured that
            // I had room to insert this entire record (with a fudge factor
            // of 2*BLOCK_SIZE)

            // update epochs
            CRITICAL_SECTION(cs, _flush_lock);
            w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = _cur_epoch;
            _cur_epoch = epoch(info->lsn, info->new_base, 0, info->new_end);
        }
        else if(info->pos > _segsize) {
            // wrapped buffer! update epochs
            CRITICAL_SECTION(cs, _flush_lock);
            w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = epoch(_cur_epoch.base_lsn, _cur_epoch.base,
                        _cur_epoch.start, segsize());
            _cur_epoch.base_lsn += segsize();
            _cur_epoch.base += segsize();
            _cur_epoch.start = 0;
            _cur_epoch.end = info->new_end;
        }
        else {
            // normal update -- no need for a lock if we just increment its end
            w_assert1(_cur_epoch.start < info->new_end);
            _cur_epoch.end = info->new_end;
        }

        // we might have to also release delegated buffer(s).
        info = _carray->grab_delegated_expose(info);
    }

    return false;
}

rc_t log_core::_join_carray(CArraySlot*& info, long& pos, int32_t size)
{
    /* Copy our data into the buffer and update/create epochs. Note
       that, while we may race the flush daemon to update the epoch
       record, it will not touch the buffer until after we succeed so
       there is no race with memcpy(). If we do lose an epoch update
       race, it is only because the flush changed old_epoch.begin to
       equal old_epoch.end. The mutex ensures we don't race with any
       other inserts.
    */

    // consolidate
    carray_status_t old_count;
    info = _carray->join_slot(size, old_count);

    pos = ConsolidationArray::extract_carray_log_size(old_count);
    if(old_count == ConsolidationArray::SLOT_AVAILABLE) {
        /* First to arrive. Acquire the lock on behalf of the whole
        * group, claim the first 'size' bytes, then make the rest
        * visible to waiting threads.
        */
        _insert_lock.acquire(&info->me);

        w_assert1(info->vthis()->count > ConsolidationArray::SLOT_AVAILABLE);
        w_assert1(pos == 0);

        // swap out this slot and mark it busy
        _carray->replace_active_slot(info);

        // negate the count to signal waiting threads and mark the slot busy
        old_count = lintel::unsafe::atomic_exchange<carray_status_t>(
            &info->count, ConsolidationArray::SLOT_PENDING);
        long combined_size = ConsolidationArray::extract_carray_log_size(old_count);

        // grab space for everyone in one go (releases the lock)
        _acquire_buffer_space(info, combined_size);

        // now let everyone else see it
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        info->count = ConsolidationArray::SLOT_FINISHED - combined_size;
    }
    else {
        // Not first. Wait for the owner to tell us what's going on.
        w_assert1(old_count > ConsolidationArray::SLOT_AVAILABLE);
        _carray->wait_for_leader(info);
    }
    return RCOK;
}

rc_t log_core::_leave_carray(CArraySlot* info, int32_t size)
{
    // last one to leave cleans up
    carray_status_t end_count = lintel::unsafe::atomic_fetch_add<carray_status_t>(
        &info->count, size);
    end_count += size; // NOTE lintel::unsafe::atomic_fetch_add returns the value before the
    // addition. So, we need to add it here again. atomic_add_fetch desired..
    w_assert3(end_count <= ConsolidationArray::SLOT_FINISHED);
    if(end_count == ConsolidationArray::SLOT_FINISHED) {
        if(!info->error) {
            _update_epochs(info);
        }
    }

    if(info->error) {
        return RC(info->error);
    }

    return RCOK;
}

rc_t log_core::insert_raw(const char* src, size_t length, lsn_t* rlsn)
{
    CArraySlot* info = NULL;
    long pos = 0;
    W_DO(_join_carray(info, pos, length));
    w_assert1(info);

    // insert my value
    if(!info->error) {
        if (rlsn) { *rlsn = info->lsn + pos; }
        _copy_raw(info, pos, src, length);
    }

    W_DO(_leave_carray(info, length));

    INC_TSTAT(log_inserts);
    ADD_TSTAT(log_bytes_generated,length);
    return RCOK;
}

rc_t log_core::insert(logrec_t &rec, lsn_t* rlsn)
{
    w_assert1(rec.length() <= sizeof(logrec_t));
    int32_t size = rec.length();

    CArraySlot* info = NULL;
    long pos = 0;
    W_DO(_join_carray(info, pos, size));
    w_assert1(info);

    // insert my value
    lsn_t rec_lsn;
    if(!info->error) {
        rec_lsn = _copy_to_buffer(rec, pos, size, info);
    }

    W_DO(_leave_carray(info, size));

    if(rlsn) {
        *rlsn = rec_lsn;
    }
    DBGOUT3(<< " insert @ lsn: " << rec_lsn << " type " << rec.type() << " length " << rec.length() );

    INC_TSTAT(log_inserts);
    ADD_TSTAT(log_bytes_generated,size);
    return RCOK;
}

void log_core::_copy_raw(CArraySlot* info, long& pos, const char* data,
        size_t size)
{
    // are we the ones that actually wrap? (do this *after* computing the lsn!)
    pos += info->start_pos;
    if(pos >= _segsize)
        pos -= _segsize;

    long spillsize = pos + size - _segsize;
    if(spillsize <= 0) {
        // normal insert
        memcpy(_buf+pos, data, size);
    }
    else {
        // spillsize > 0 so we are wrapping.
        // The wrap is within a partition.
        // next_lsn is still valid but not new_end
        //
        // Old epoch becomes valid for the flush daemon to
        // flush and "close".  It contains the first part of
        // this log record that we're trying to insert.
        // New epoch holds the rest of the log record that we're trying
        // to insert.
        //
        // spillsize is the portion that wraps around
        // partsize is the portion that doesn't wrap.
        long partsize = size - spillsize;

        // Copy log record to buffer
        // memcpy : areas do not overlap
        memcpy(_buf+pos, data, partsize);
        memcpy(_buf, data+partsize, spillsize);
    }
}

/*
 * Inserts an arbitrary block of memory (a bulk of log records from plog) into
 * the log buffer, returning the LSN of the first byte in rlsn. This is used
 * by the atomic commit protocol (see plog_xct_t::_commit) (Caetano).
 */
/*rc_t log_core::insert_bulk(char* data, size_t size, lsn_t*& rlsn)
{
    CArraySlot* info = NULL;
    long pos = 0;
    W_DO(_join_carray(info, pos, size));
    w_assert1(info);

    // copy to buffer (similar logic of _copy_to_buffer, but for bulk use)
    if(!info->error) {
        *rlsn = info->lsn + pos;


    W_DO(_leave_carray(info, size));

    ADD_TSTAT(log_bytes_generated,size);
    return RCOK;
}
*/


// Return when we know that the given lsn is durable. Wait for the
// log flush daemon to ensure that it's durable.
rc_t log_core::flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
{
    DBGOUT3(<< " flush @ to_lsn: " << to_lsn);

    w_assert1(signal || !block); // signal=false can be used only when block=false
    ASSERT_FITS_IN_POINTER(lsn_t);
    // else our reads to _durable_lsn would be unsafe

    // don't try to flush past end of log -- we might wait forever...
    lsn_t lsn = std::min(to_lsn, (*&_curr_lsn)+ -1);

    // already durable?
    if(lsn >= *&_durable_lsn) {
        if (!block) {
            *&_waiting_for_flush = true;
            if (signal) {
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
            }
            if (ret_flushed) *ret_flushed = false; // not yet flushed
        }  else {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            while(lsn >= *&_durable_lsn) {
                *&_waiting_for_flush = true;
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                DO_PTHREAD(pthread_cond_wait(&_wait_cond, &_wait_flush_lock));
            }
            if (ret_flushed) *ret_flushed = true;// now flushed!
        }
    } else {
        INC_TSTAT(log_dup_sync_cnt);
        if (ret_flushed) *ret_flushed = true; // already flushed
    }
    return RCOK;
}

/**\brief Log-flush daemon driver.
 * \details
 * This method handles the wait/block of the daemon thread,
 * and when awake, calls its main-work method, flush_daemon_work.
 */
void log_core::flush_daemon()
{
    /* Algorithm: attempt to flush non-durable portion of the buffer.
     * If we empty out the buffer, block until either enough
       bytes get written or a thread specifically requests a flush.
     */
    lsn_t last_completed_flush_lsn;
    bool success = false;
    while(1) {

        // wait for a kick. Kicks come at regular intervals from
        // inserts, but also at arbitrary times when threads request a
        // flush.
        {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            // CS: commented out check for waiting_for_space -- don't know why it was here?
            //if(success && (*&_waiting_for_space || *&_waiting_for_flush)) {
            if(success && *&_waiting_for_flush) {
                //_waiting_for_flush = _waiting_for_space = false;
                _waiting_for_flush = false;
                DO_PTHREAD(pthread_cond_broadcast(&_wait_cond));
                // wake up anyone waiting on log flush
            }
            if(_shutting_down) {
                _shutting_down = false;
                break;
            }

            // NOTE: right now the thread waiting for a flush has woken up or will woke up, but...
            // this thread, as long as success is true (it just flushed something in the previous
            // flush_daemon_work), will keep calling flush_daemon_work until there is nothing to flush....
            // this happens in the background

            // sleep. We don't care if we get a spurious wakeup
            //if(!success && !*&_waiting_for_space && !*&_waiting_for_flush) {
            if(!success && !*&_waiting_for_flush) {
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                // CS FINELINE: log not waiting for signal anymore (higher tput)
                // DO_PTHREAD(pthread_cond_wait(&_flush_cond, &_wait_flush_lock));
            }
        }

        // flush all records later than last_completed_flush_lsn
        // and return the resulting last durable lsn
        lsn_t lsn = flush_daemon_work(last_completed_flush_lsn);

        // success=true if we wrote anything
        success = (lsn != last_completed_flush_lsn);
        last_completed_flush_lsn = lsn;
    }

    // make sure the buffer is completely empty before leaving...
    for(lsn_t lsn;
        (lsn=flush_daemon_work(last_completed_flush_lsn)) !=
                last_completed_flush_lsn;
        last_completed_flush_lsn=lsn) ;
}

bool log_core::_should_group_commit(long write_size)
{
    // Do not flush if write size is less than group commit size
    if (write_size < _group_commit_size) {
        // Only supress flush if timeout hasn't expired
        if (_group_commit_timeout > 0 &&
                _group_commit_timer.time_ms() > _group_commit_timeout)
        {
            return true;
        }
        return false;
    }

    return true;
}

/**\brief Flush unflushed-portion of log buffer.
 * @param[in] old_mark Durable lsn from last flush. Flush records later than this.
 * \details
 * This is the guts of the log daemon.
 *
 * Flush the log buffer of any log records later than \a old_mark. The
 * argument indicates what is already durable and these log records must
 * not be duplicated on the disk.
 *
 * Called by the log flush daemon.
 * Protection from duplicate flushing is handled by the fact that we have
 * only one log flush daemon.
 * \return Latest durable lsn resulting from this flush
 *
 */
lsn_t log_core::flush_daemon_work(lsn_t old_mark)
{
    lsn_t base_lsn_before, base_lsn_after;
    long base, start1, end1, start2, end2, write_size;
    {
        CRITICAL_SECTION(cs, _flush_lock);
        base_lsn_before = _old_epoch.base_lsn;
        base_lsn_after = _cur_epoch.base_lsn;
        base = _cur_epoch.base;

        // The old_epoch is valid (needs flushing) iff its end > start.
        // The old_epoch is valid id two cases, both when
        // insert wrapped thelog buffer
        // 1) by doing so reached the end of the partition,
        //     In this case, the old epoch might not be an entire
        //     even segment size
        // 2) still room in the partition
        //     In this case, the old epoch is exactly 1 segment in size.

        if(_old_epoch.start == _old_epoch.end) {
            // no wrap -- flush only the new
            w_assert1(_cur_epoch.end >= _cur_epoch.start);
            start2 = _cur_epoch.start;
            end2 = _cur_epoch.end;
            w_assert1(end2 >= start2);
            // false alarm?
            if(start2 == end2) {
                return old_mark;
            }

            start1 = start2; // fake start1 so the start_lsn calc below works
            end1 = start2;

            // CS TODO: what is the purpose of old_mark? Do we ever use it?
            write_size = (end2 - start2) + (end1 - start1);
            if (!_should_group_commit(write_size)) { return old_mark; }

            base_lsn_before = base_lsn_after;
            _cur_epoch.start = end2;
        }
        else if(base_lsn_before.file() == base_lsn_after.file()) {
            // wrapped within partition -- flush both
            start2 = _cur_epoch.start;
            // race here with insert setting _curr_epoch.end, but
            // it won't matter. Since insert already did the memcpy,
            // we are safe and can flush the entire amount.
            end2 = _cur_epoch.end;

            start1 = _old_epoch.start;
            end1 = _old_epoch.end;

            write_size = (end2 - start2) + (end1 - start1);
            if (!_should_group_commit(write_size)) { return old_mark; }

            _old_epoch.start = end1;
            _cur_epoch.start = end2;
            w_assert1(base_lsn_before + segsize() == base_lsn_after);
        }
        else {
            // new partition -- flushing only the old since the
            // two epochs target different files. Let the next
            // flush handle the new epoch.
            start2 = 0;
            end2 = 0; // don't fake end2 because end_lsn needs to see '0'

            start1 = _old_epoch.start;
            end1 = _old_epoch.end;

            // Mark the old epoch has no longer valid.
            _old_epoch.start = end1;

            w_assert1(base_lsn_before.file()+1 == base_lsn_after.file());
        }
    } // end critical section

    lsn_t start_lsn = base_lsn_before + start1;
    lsn_t end_lsn   = base_lsn_after + end2;
    long  new_start = base + end2;
    {
        // Avoid interference with compensations.
        CRITICAL_SECTION(cs, _comp_lock);
        _flush_lsn = end_lsn;
    }

    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    w_assert1(end_lsn == first_lsn(start_lsn.hi()+1)
          || end_lsn.lo() - start_lsn.lo() == (end1-start1) + (end2-start2));

    // start_lsn.file() determines partition # and whether code
    // will open a new partition into which to flush.
    // That, in turn, is determined by whether the _old_epoch.base_lsn.file()
    // matches the _cur_epoch.base_lsn.file()
    // CS: This code used to be on the method _flushX
    auto p = _storage->get_partition_for_flush(start_lsn, start1, end1,
            start2, end2);

    // Flush the log buffer
    W_COERCE(p->flush(start_lsn, _buf, start1, end1, start2, end2));
    write_size = (end2 - start2) + (end1 - start1);

    _durable_lsn = end_lsn;
    _start = new_start;
    _epoch_tracker.advance_epoch();
    // For eviction purposes, epoch associated with the log file must be the lowest active, and not current!
    _log_file_epochs[p->num()] = _epoch_tracker.get_lowest_active_epoch() - 1;

    _group_commit_timer.reset();

    return end_lsn;
}

lsn_t log_core::get_oldest_active_lsn()
{
    return _oldest_lsn_tracker->get_oldest_active_lsn(curr_lsn());
}
