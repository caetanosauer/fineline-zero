/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
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

 $Id: partition.cpp,v 1.11 2010/12/08 17:37:43 nhall Exp $

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
#define PARTITION_C

#include "sm_base.h"
#include "log_storage.h"
#include "xct_logger.h"

// files and stuff
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

partition_t::partition_t(log_storage *owner, partition_number_t num)
    : _num(num), _owner(owner),
      _fhdl(invalid_fhdl), _skip_logrec{skip_log}, _delete_after_close(false)
{
    // Add space for skip log record
    _max_partition_size = owner->get_partition_size() + sizeof(baseLogHeader);
    _readbuf = nullptr;
}

long floor2(long offset, long block_size)
{ return offset & -block_size; }
long ceil2(long offset, long block_size)
{ return floor2(offset + block_size - 1, block_size); }

// Block of zeroes : used in next function.
// Initialize on first access:
// block to be cleared upon first use.
class block_of_zeroes {
private:
    char _block[log_storage::BLOCK_SIZE];
public:
    NORET block_of_zeroes() {
        memset(&_block[0], 0, log_storage::BLOCK_SIZE);
    }
    char *block() { return _block; }
};

char *block_of_zeros() {

    static block_of_zeroes z;
    return z.block();
}

/*
 * partition::flush(int fd, bool force)
 * flush to disk whatever's been buffered.
 * Do this with a writev of 4 parts:
 * start->end1 where start is start1 rounded down to the beginning of a BLOCK
 * start2->end2
 * a skip record
 * enough zeroes to make the entire write become a multiple of BLOCK_SIZE
 */
rc_t partition_t::flush(
        lsn_t lsn,  // needed so that we can set the lsn in the skip_log record
        const char* const buf,
        long start1,
        long end1,
        long start2,
        long end2)
{
    w_assert0(end1 >= start1);
    w_assert0(end2 >= start2);
    long size = (end2 - start2) + (end1 - start1);
    long write_size = size;
    long file_offset;

    { // sync log: Seek the file to the right place.
        DBG5( << "Sync-ing log lsn " << lsn
                << " start1 " << start1
                << " end1 " << end1
                << " start2 " << start2
                << " end2 " << end2 );

        // works because BLOCK_SIZE is always a power of 2
        file_offset = floor2(lsn.lo(), log_storage::BLOCK_SIZE);
        // offset is rounded down to a block_size

        long delta = lsn.lo() - file_offset;

        // adjust down to the nearest full block
        w_assert1(start1 >= delta); // really offset - delta >= 0,
                                    // but works for unsigned...
        write_size += delta; // account for the extra (clean) bytes
        start1 -= delta;

        /* FRJ: This seek is safe (in theory) because only one thread
           can flush at a time and all other accesses to the file use
           pread/pwrite (which doesn't change the file pointer).
         */
        auto ret = lseek(_fhdl, file_offset, SEEK_SET);
        CHECK_ERRNO(ret);
    } // end sync log

    { // Copy a skip record to the end of the buffer.
        // CS TODO FINELINE: fix log priming
        // _skip_logrec.set_lsn_ck(lsn+size);

        // Hopefully the OS is smart enough to coalesce the writes
        // before sending them to disk. If not, and it's a problem
        // (e.g. for direct I/O), the alternative is to assemble the last
        // block by copying data out of the buffer so we can append the
        // skiplog without messing up concurrent inserts. However, that
        // could mean copying up to BLOCK_SIZE bytes.
        long total = write_size + _skip_logrec.length();

        // works because BLOCK_SIZE is always a power of 2
        long grand_total = ceil2(total, log_storage::BLOCK_SIZE);
        // take it up to multiple of block size
        w_assert2(grand_total % log_storage::BLOCK_SIZE == 0);

        if(grand_total == log_storage::BLOCK_SIZE) {
            // 1-block flush
            INC_TSTAT(log_short_flush);
        } else {
            // 2-or-more-block flush
            INC_TSTAT(log_long_flush);
        }

        // CS FINELINE TODO: this is a temporary solution for the log priming problem.
        // For now, we set the PID of the skip log record as the file offset and
        // look for that when initializing the log.
        _skip_logrec.set_pid(file_offset + write_size);

        struct iovec iov[] = {
            // iovec_t expects void* not const void *
            { (char*)buf+start1,                end1-start1 },
            // iovec_t expects void* not const void *
            { (char*)buf+start2,                end2-start2},
            { &_skip_logrec,                    _skip_logrec.length()},
            { block_of_zeros(),         grand_total-total},
        };

        auto ret = ::writev(_fhdl, iov, 4);
        CHECK_ERRNO(ret);

        ADD_TSTAT(log_bytes_written, grand_total);
    } // end copy skip record

    fsync_delayed(_fhdl); // fsync
    return RCOK;
}

void partition_t::read(logrec_t *&rp, lsn_t &ll)
{
    w_assert1(ll.hi() == num());
    w_assert1(is_open());

    size_t pos = ll.lo();
    rp = reinterpret_cast<logrec_t*>(_readbuf + pos);
}

size_t partition_t::read_block(void* buf, size_t count, off_t offset)
{
    w_assert0(is_open());
    auto bytesRead = ::pread(_fhdl, buf, count, offset);
    CHECK_ERRNO(bytesRead);

    return bytesRead;
}

void partition_t::open()
{
    unique_lock<mutex> lck(_mutex);
    if (is_open()) { return; }
    string fname = _owner->make_log_name(_num);
    int fd, flags = O_RDWR | O_CREAT;
    fd = ::open(fname.c_str(), flags, 0744 /*mode*/);
    CHECK_ERRNO(fd);
    auto res = ::ftruncate(fd, _max_partition_size);
    CHECK_ERRNO(res);
    w_assert3(_fhdl == invalid_fhdl);
    _fhdl = fd;
    _readbuf = reinterpret_cast<char*>(
            mmap(nullptr, _max_partition_size, PROT_READ, MAP_SHARED, _fhdl, 0));
    CHECK_ERRNO((long) _readbuf);
    DBG(<< "opened_log_file " << _num);
}

// CS TODO: why is this definition here?
int partition_t::_artificial_flush_delay = 0;

void partition_t::fsync_delayed(int fd)
{
    static int64_t attempt_flush_delay = 0;
    // We only cound the fsyncs called as
    // a result of flush(), not from peek
    // or start-up
    INC_TSTAT(log_fsync_cnt);

    auto ret = ::fsync(fd);
    CHECK_ERRNO(ret);

    if (_artificial_flush_delay > 0) {
        if (attempt_flush_delay==0) {
            w_assert1(_artificial_flush_delay < 99999999/1000);
            attempt_flush_delay = _artificial_flush_delay * 1000;
        }
        struct timespec req, rem;
        req.tv_sec = 0;
        req.tv_nsec = attempt_flush_delay;

        struct timeval start;
        gettimeofday(&start,0);

        while(nanosleep(&req, &rem) != 0) {
            if (errno != EINTR)  break;
            req = rem;
        }

        struct timeval stop;
        gettimeofday(&stop,0);
        int64_t diff = stop.tv_sec * 1000000 + stop.tv_usec;
        diff -= start.tv_sec *       1000000 + start.tv_usec;
        //diff is in micros.
        diff *= 1000; // now it is nanos
        attempt_flush_delay += ((_artificial_flush_delay * 1000) - diff)/8;

    }
}

void partition_t::close()
{
    unique_lock<mutex> lck(_mutex);

    if (is_open()) {
        // Caller must guarantee thread safety (log_storage::delete_old_partitions)
        auto ret = munmap(_readbuf, _max_partition_size);
        CHECK_ERRNO(ret);
        _readbuf = nullptr;
        ret = ::close(_fhdl);
        CHECK_ERRNO(ret);
        _fhdl = invalid_fhdl;

        Logger::log_sys<comment_log>("closed_log_file " + to_string(_num));
        DBG(<< "closed_log_file " << _num);
    }

    if (_delete_after_close) {
	fs::path f = _owner->make_log_name(_num);
	fs::remove(f);
        Logger::log_sys<comment_log>("deleted_log_file " + to_string(_num));
        DBG(<< "deleted_log_file " << _num);
    }
}
