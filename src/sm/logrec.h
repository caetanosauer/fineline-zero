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

/*<std-header orig-src='shore' incl-file-exclusion='LOGREC_H'>

 $Id: logrec.h,v 1.73 2010/12/08 17:37:42 nhall Exp $

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

#ifndef LOGREC_H
#define LOGREC_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class rangeset_t;
class RestoreBitmap;
class xct_t;

#include "lsn.h"
#include "tid_t.h"
#include "generic_page.h" // logrec size == 3 * page size
#include "allocator.h"

// 1/4 of typical cache-line size (64B)
constexpr size_t LogrecAlignment = 16;

struct alignas(LogrecAlignment) baseLogHeader
{
    PageID _pid;
    uint32_t _page_version;
    uint16_t _len;
    uint8_t _type;

    bool is_valid() const;
};

static_assert(sizeof(baseLogHeader) == 16, "Wrong logrec header size");

enum kind_t {
    comment_log = 0,
    // compensate_log = 1,
    skip_log = 2,
    chkpt_begin_log = 3,
    // t_chkpt_bf_tab = 4,
    // t_chkpt_xct_tab = 5,
    // t_chkpt_xct_lock = 6,
    warmup_done_log = 7,
    alloc_format_log = 8,
    evict_page_log = 9,
    add_backup_log = 10,
    // xct_abort_log = 11,
    fetch_page_log = 12,
    xct_end_log = 13,
    // t_xct_end_group = 14,
    xct_latency_dump_log = 15,
    alloc_page_log = 16,
    dealloc_page_log = 17,
    create_store_log = 18,
    append_extent_log = 19,
    loganalysis_begin_log = 20,
    loganalysis_end_log = 21,
    redo_done_log = 22,
    undo_done_log = 23,
    restore_begin_log = 24,
    restore_segment_log = 25,
    restore_end_log = 26,
    // t_page_set_to_be_deleted = 27,
    stnode_format_log = 27,
    page_img_format_log = 28,
    update_emlsn_log = 29,
    // t_btree_norec_alloc = 30,
    btree_insert_log = 31,
    btree_insert_nonghost_log = 32,
    btree_update_log = 33,
    btree_overwrite_log = 34,
    btree_ghost_mark_log = 35,
    btree_ghost_reclaim_log = 36,
    btree_ghost_reserve_log = 37,
    btree_foster_adopt_log = 38,
    btree_unset_foster_log = 39,
    // t_btree_foster_rebalance = 40,
    // t_btree_foster_rebalance_norec = 41,
    // t_btree_foster_deadopt = 42,
    btree_bulk_delete_log = 43,
    btree_compress_page_log = 44,
    tick_sec_log = 45,
    tick_msec_log = 46,
    benchmark_start_log = 47,
    page_write_log = 48,
    page_read_log = 49,
    t_max_logrec = 50
};

/**
 * \brief Represents a transactional log record.
 * \ingroup SSMLOG
 * \details
 * A log record's space is divided between a header and data.
 * All log records' headers include the information contained in baseLogHeader.
 * Log records pertaining to transactions that produce multiple log records
 * also persist a transaction id chain (_xid and _xid_prv).
 *
 * \section OPT Optimization for single-log system transaction
 * For single-log system transaction, header items in xidChainLogHeader are not stored.
 * instead, we use these area as data area to save 16 bytes.
 * we do need to keep these 8 bytes aligned. and this is a bit dirty trick.
 * however, we really need it to reduce the volume of log we output for system transactions.
 */
class logrec_t {
public:
    friend class XctLogger;
    friend class sysevent;
    friend class baseLogHeader;

    bool             is_page_update() const;
    bool             is_redo() const;
    bool             is_skip() const;
    bool             is_undo() const;
    bool             is_cpsn() const;
    bool             is_system() const;
    bool             valid_header() const;
    smsize_t         header_size() const;

    template <class PagePtr>
    void             redo(PagePtr);

    static constexpr u_char get_logrec_cat(kind_t type);

    void redo();

    static void undo(kind_t type, StoreID stid, const char* data);

    void init_header(kind_t, PageID = 0);

    void set_pid(PageID pid)
    {
        header._pid = pid;
    }

    void set_size(size_t l);

    enum {
        max_sz = 3 * sizeof(generic_page),
        hdr_sz = sizeof(baseLogHeader),
        max_data_sz = max_sz - hdr_sz
    };

    tid_t   tid() const;
    StoreID        stid() const;
    PageID         pid() const;

    // Gives an initialized skip log record
    static const logrec_t& get_skip_log();

    logrec_t() = default;
    // constructor used for data-less (i.e., system) log records
    logrec_t(kind_t kind);

public:
    smsize_t             length() const;
    kind_t               type() const;
    const char*          type_str() const
    {
        return get_type_str(type());
    }
    static const char*   get_type_str(kind_t);
    const char*          data() const;
    char*                data();

    uint32_t page_version() const
    {
        return header._page_version;
    }

    void set_page_version(uint32_t version)
    {
        header._page_version = version;
    }

    void                 corrupt();

    // Tells whether this log record restores a full page image, meaning
    // that the previous history is not needed during log replay.
    bool has_page_img()
    {
        return
             (type() == page_img_format_log)
            || (type() == stnode_format_log)
            || (type() == alloc_format_log)
            ;
    }

    friend ostream& operator<<(ostream&, logrec_t&);

    enum category_t {
        /** should not happen. */
        t_bad_cat   = 0x00,
        /** System log record: not transaction- or page-related; no undo/redo */
        t_system    = 0x01,
        /** log with UNDO action? */
        t_undo      = 0x02,
        /** log with REDO action? */
        t_redo      = 0x04,
    };

    u_char             cat() const;

protected:

    baseLogHeader header;

    char            _data[max_data_sz];


public:
    // overloaded new/delete operators for tailored memory management
    void* operator new(size_t);
    void operator delete(void*, size_t);

    // CS: apparently we have to define placement new as well if the standard
    // new is overloaded
    void* operator new(size_t, void* p) { return p; }
};

struct UndoEntry
{
    uint16_t offset;
    StoreID store;
    kind_t type;
};

class UndoBuffer
{
    static constexpr size_t UndoBufferSize = 64 * 1024;
    static constexpr size_t MaxUndoRecords = UndoBufferSize / 128;
    std::array<char, UndoBufferSize> _buffer;
    std::array<UndoEntry, MaxUndoRecords+1> _entries;
    size_t _count;
    bool _abortable;

public:
    UndoBuffer()
    {
        reset();
    }

    void reset()
    {
        _count = 0;
        _abortable = true;
        _entries[0].offset = 0;
    }

    size_t get_count() { return _count; }

    bool is_abortable() { return _abortable; }

    char* get_buffer_end()
    {
        return &_buffer[_entries[_count].offset];
    }

    size_t get_free_space()
    {
        return UndoBufferSize - (get_buffer_end() - &_buffer[0]);
    }

    char* acquire()
    {
        if (!is_abortable()) { return nullptr; }
        // Conservative approach: make sure we can fit maximum logrec size
        if (get_free_space() < sizeof(logrec_t) || _count >= MaxUndoRecords) {
            // W_FATAL_MSG(eINTERNAL, <<
            //         "Transaction too large -- undo buffer full!");
            _abortable = false;
            return nullptr;
        }

        return get_buffer_end();
    }

    void release(size_t length, StoreID store, kind_t type)
    {
        _entries[_count].store = store;
        _entries[_count].type = type;
        auto offset = _entries[_count].offset;
        _count++;
        _entries[_count].offset = offset + length;
    }

    char* get_data(size_t i)
    {
        if (i < _count) { return &_buffer[_entries[i].offset]; }
        return nullptr;
    }

    StoreID get_store_id(size_t i)
    {
        if (i < _count) { return _entries[i].store; }
        return StoreID{0};
    }

    kind_t get_type(size_t i)
    {
        if (i < _count) { return _entries[i].type; }
        return t_max_logrec;
    }
};

class RedoBuffer
{
    static constexpr size_t BufferSize = 1024 * 1024;
    std::array<char, BufferSize> _buffer;
    size_t _size;
    uint64_t _epoch;

public:
    RedoBuffer()
        : _size(0), _epoch(0)
    {
    }

    uint64_t get_epoch() const { return _epoch; }
    void set_epoch(uint64_t e) { _epoch = e; }

    char* get_buffer_end()
    {
        return &_buffer[_size];
    }

    char* get_buffer_begin()
    {
        return &_buffer[0];
    }

    size_t get_free_space()
    {
        return BufferSize - _size;
    }

    size_t get_size()
    {
        return _size;
    }

    void drop_suffix(size_t len)
    {
        _size -= len;
    }

    char* acquire()
    {
        // Conservative approach: make sure we can fit maximum logrec size
        if (get_free_space() < sizeof(logrec_t)) {
            return nullptr;
        }

        return get_buffer_end();
    }

    void release(size_t length)
    {
        _size += length;
    }

    void reset()
    {
        _size = 0;
    }
};

inline bool baseLogHeader::is_valid() const
{
    return (_len >= sizeof(baseLogHeader)
            && _type < t_max_logrec
            && _len <= sizeof(logrec_t));
}

// for single-log system transaction, we use tid/_xid_prev as data area!
inline const char*  logrec_t::data() const
{
    return _data;
}
inline char*  logrec_t::data()
{
    return _data;
}

inline PageID
logrec_t::pid() const
{
    return header._pid;
}

inline smsize_t
logrec_t::length() const
{
    return header._len;
}

inline kind_t
logrec_t::type() const
{
    return (kind_t) header._type;
}

inline u_char
logrec_t::cat() const
{
    return get_logrec_cat(static_cast<kind_t>(header._type));
}

inline bool
logrec_t::is_system() const
{
    return (cat() & t_system) != 0;
}

inline bool
logrec_t::is_redo() const
{
    return (cat() & t_redo) != 0;
}

inline bool
logrec_t::is_skip() const
{
    return type() == skip_log;
}

inline bool
logrec_t::is_undo() const
{
    return (cat() & t_undo) != 0;
}

inline bool
logrec_t::is_page_update() const
{
    // CS: I have no idea why a compensation log record is not considered a
    // page update. In fact every check of in_page_update() is or'ed with
    // is_cpsn()
    return is_redo() && !is_cpsn();
}

constexpr u_char logrec_t::get_logrec_cat(kind_t type)
{
    switch (type) {
	case comment_log : return t_system;
	case tick_sec_log : return t_system;
	case tick_msec_log : return t_system;
	case benchmark_start_log : return t_system;
	case page_write_log : return t_system;
	case page_read_log : return t_system;
	case skip_log : return t_system;
	case chkpt_begin_log : return t_system;
	case loganalysis_begin_log : return t_system;
	case loganalysis_end_log : return t_system;
	case redo_done_log : return t_system;
	case undo_done_log : return t_system;
        case warmup_done_log: return t_system;
	case restore_begin_log : return t_system;
	case restore_segment_log : return t_system;
	case restore_end_log : return t_system;
	case xct_latency_dump_log : return t_system;
	case add_backup_log : return t_system;
	case evict_page_log : return t_system;
	case fetch_page_log : return t_system;
	case xct_end_log : return t_system;

	case alloc_page_log : return t_redo;
	case stnode_format_log : return t_redo;
	case alloc_format_log : return t_redo;
	case dealloc_page_log : return t_redo;
	case create_store_log : return t_redo;
	case append_extent_log : return t_redo;
	case page_img_format_log : return t_redo;
	case update_emlsn_log : return t_redo;
	case btree_insert_log : return t_redo|t_undo;
	case btree_insert_nonghost_log : return t_redo|t_undo;
	case btree_update_log : return t_redo|t_undo;
	case btree_overwrite_log : return t_redo|t_undo;
	case btree_ghost_mark_log : return t_redo|t_undo;
	case btree_ghost_reclaim_log : return t_redo;
	case btree_ghost_reserve_log : return t_redo;
	case btree_foster_adopt_log : return t_redo;
	case btree_unset_foster_log : return t_redo;
	case btree_bulk_delete_log : return t_redo;
	case btree_compress_page_log : return t_redo;

        default: return t_bad_cat;
    }
}

#endif          /*</std-footer>*/
