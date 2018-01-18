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

/*<std-header orig-src='shore' incl-file-exclusion='SRV_LOG_H'>

 $Id: partition.h,v 1.6 2010/08/23 14:28:18 nhall Exp $

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

#ifndef PARTITION_H
#define PARTITION_H
#include "w_defines.h"

#include "sm_base.h" // for partition_number_t (CS TODO)
#include "logrec.h"
#include <mutex>
#include <atomic>

class log_storage; // forward

class partition_t {
public:
    typedef smlevel_0::partition_number_t partition_number_t;

    enum { XFERSIZE = 8192 };
    enum { invalid_fhdl = -1 };

    partition_t(log_storage*, partition_number_t);
    virtual ~partition_t() { close(); }

    partition_number_t num() const   { return _num; }

    void open();
    void close();

    void read(logrec_t *&r, lsn_t &ll);

    size_t read_block(void* buf, size_t count, off_t offset);

    rc_t flush(lsn_t lsn, const char* const buf, long start1, long end1,
            long start2, long end2);

    bool is_open() const
    {
        return (_fhdl != invalid_fhdl);
    }

    void mark_for_deletion() { _delete_after_close = true; }

private:
    partition_number_t    _num;
    log_storage*          _owner;
    int                   _fhdl;
    static int            _artificial_flush_delay;  // in microseconds
    char*                 _readbuf;
    bool _delete_after_close;

    size_t _max_partition_size;
    char* _mmap_buffer;

    void             fsync_delayed(int fd);

    // Serialize open and close calls
    mutex _mutex;

    logrec_t _skip_logrec;
};

#endif
