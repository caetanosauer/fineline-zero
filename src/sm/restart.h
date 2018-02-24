/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

     /*<std-header orig-src='shore' incl-file-exclusion='RESTART_H'>

        $Id: restart.h,v 1.27 2010/07/01 00:08:22 nhall Exp $

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

#ifndef RESTART_H
#define RESTART_H

#include "w_defines.h"
#include "sm_base.h"
#include "fixable_page_h.h"
#include "logarchive_scanner.h"

#include <map>

/*
 * A log-record iterator that encapsulates a log archive scan and a recovery
 * log scan. It reads from the former until it runs out, after which it reads
 * from the latter, which is collected by following the per-page chain in the
 * recovery log.
 */
class SprIterator
{
public:

    SprIterator();
    ~SprIterator();

    void open(PageID pid);
    void apply(fixable_page_h& page);
    void redo(fixable_page_h& p, logrec_t* lr);

    // Required for eviction of pages with updates not yet archived (FineLine)
    void reopen(PageID pid);
    run_number_t getLastProbedRun() const { return archive_scan.getLastProbedRun(); }

private:
    ArchiveScan archive_scan;
    bool img_consumed; // Workaround for page-img compression (see comments in cpp)
};

#endif
