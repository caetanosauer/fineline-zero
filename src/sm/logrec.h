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
struct multi_page_log_t;
class RestoreBitmap;
class xct_t;

#include "lsn.h"
#include "tid_t.h"
#include "generic_page.h" // logrec size == 3 * page size
#include "allocator.h"

struct baseLogHeader
{
    uint16_t _len;  // length of the log record
    uint8_t _type; // kind_t (included from logtype_gen.h)
    uint8_t _fill4;
    PageID             _pid; // 4 bytes

    bool is_valid() const;
};

static_assert(sizeof(baseLogHeader) == 8, "Wrong logrec header size");

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
    // t_btree_foster_merge = 39,
    // t_btree_foster_rebalance = 40,
    // t_btree_foster_rebalance_norec = 41,
    // t_btree_foster_deadopt = 42,
    btree_split_log = 43,
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
    bool             is_multi_page() const;
    bool             is_system() const;
    bool             is_single_sys_xct() const;
    bool             valid_header(const lsn_t & lsn_ck = lsn_t::null) const;
    smsize_t         header_size() const;

    template <class PagePtr>
    void             redo(PagePtr);

    static constexpr u_char get_logrec_cat(kind_t type);

    void redo();

    static void undo(kind_t type, StoreID stid, const char* data);

    void init_header(kind_t);

    template <class PagePtr>
    void init_page_info(const PagePtr p)
    {
        header._pid = p->pid();
    }

    void set_size(size_t l);

    enum {
        max_sz = 3 * sizeof(generic_page),
        hdr_sz = sizeof(baseLogHeader),
        max_data_sz = max_sz - hdr_sz - sizeof(lsn_t)
    };

       tid_t   tid() const;
       StoreID        stid() const;
       PageID         pid() const;
       PageID         pid2() const;

public:
    smsize_t             length() const;
    void                 set_pid(const PageID& p);
    kind_t               type() const;
    const char*          type_str() const
    {
        return get_type_str(type());
    }
    static const char*   get_type_str(kind_t);
    const char*          cat_str() const;
    const char*          data() const;
    char*                data();
    /** Returns the log record data as a multi-page SSX log. */
    multi_page_log_t*           data_multi();
    /** Const version */
    const multi_page_log_t*     data_multi() const;
    const lsn_t&         lsn_ck() const {  return *_lsn_ck(); }
    const lsn_t&         lsn() const {  return *_lsn_ck(); }
    const lsn_t          get_lsn_ck() const {
                                lsn_t    tmp = *_lsn_ck();
                                return tmp;
                            }
    void                 set_lsn_ck(const lsn_t &lsn_ck) {
                                // put lsn in last bytes of data
                                lsn_t& where = *_lsn_ck();
                                where = lsn_ck;
                            }
    void                 corrupt();

    void remove_info_for_pid(PageID pid);

    // Tells whether this log record restores a full page image, meaning
    // that the previous history is not needed during log replay.
    bool has_page_img(PageID page_id)
    {
        return
            (type() == btree_split_log && page_id == pid())
            || (type() == page_img_format_log)
            || (type() == stnode_format_log)
            || (type() == alloc_format_log)
            ;
    }

    friend ostream& operator<<(ostream&, logrec_t&);

protected:

    enum category_t {
        /** should not happen. */
        t_bad_cat   = 0x00,
        /** System log record: not transaction- or page-related; no undo/redo */
        t_system    = 0x01,
        /** log with UNDO action? */
        t_undo      = 0x02,
        /** log with REDO action? */
        t_redo      = 0x04,
        /** log for multi pages? */
        t_multi     = 0x08,

        /** log by system transaction which is fused with begin/commit record. */
        t_single_sys_xct    = 0x80
    };

    u_char             cat() const;

    baseLogHeader header;

    char            _data[max_data_sz];


    // The last sizeof(lsn_t) bytes of data are used for
    // recording the lsn.
    // Should always be aligned to 8 bytes.
    lsn_t*            _lsn_ck() {
        w_assert3(alignon(header._len, 8));
        char* this_ptr = reinterpret_cast<char*>(this);
        return reinterpret_cast<lsn_t*>(this_ptr + header._len - sizeof(lsn_t));
    }
    const lsn_t*            _lsn_ck() const {
        w_assert3(alignon(header._len, 8));
        const char* this_ptr = reinterpret_cast<const char*>(this);
        return reinterpret_cast<const lsn_t*>(this_ptr + header._len - sizeof(lsn_t));
    }

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
        : _count{0}, _abortable{true}
    {
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


inline bool baseLogHeader::is_valid() const
{
    return (_len >= sizeof(baseLogHeader)
            && _type < t_max_logrec
            && _len <= sizeof(logrec_t));
}

/**
 * \brief Base struct for log records that touch multi-pages.
 * \ingroup SSMLOG
 * \details
 * Such log records are so far _always_ single-log system transaction that touches 2 pages.
 * If possible, such log record should contain everything we physically need to recover
 * either page without the other page. This is an important property
 * because otherwise it imposes write-order-dependency and a careful recovery.
 * In such a case "page2" is the data source page while "page" is the data destination page.
 * \NOTE a REDO operation of multi-page log must expect _either_ of page/page2 are given.
 * It must first check if which page is requested to recover, then apply right changes
 * to the page.
 */
struct multi_page_log_t {

    /** Page ID of another page touched by the operation. */
    PageID     _page2_pid; // +4

    /** for alignment only. */
    uint32_t    _fill4;    // +4.

    multi_page_log_t(PageID page2_pid) : _page2_pid(page2_pid) {
    }
};

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

inline PageID logrec_t::pid2() const
{
    if (!is_multi_page()) { return 0; }

    const multi_page_log_t* multi_log = reinterpret_cast<const multi_page_log_t*> (data());
    return multi_log->_page2_pid;
}

inline void
logrec_t::set_pid(const PageID& p)
{
    header._pid = p;
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

inline bool logrec_t::is_multi_page() const {
    return (cat() & t_multi) != 0;
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

inline bool
logrec_t::is_single_sys_xct() const
{
    return (cat() & t_single_sys_xct) != 0;
}

inline multi_page_log_t* logrec_t::data_multi() {
    w_assert1(is_multi_page());
    return reinterpret_cast<multi_page_log_t*>(data());
}
inline const multi_page_log_t* logrec_t::data_multi() const {
    w_assert1(is_multi_page());
    return reinterpret_cast<const multi_page_log_t*>(data());
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

	case alloc_page_log : return t_redo|t_single_sys_xct;
	case stnode_format_log : return t_redo|t_single_sys_xct;
	case alloc_format_log : return t_redo|t_single_sys_xct;
	case dealloc_page_log : return t_redo|t_single_sys_xct;
	case create_store_log : return t_redo|t_single_sys_xct;
	case append_extent_log : return t_redo|t_single_sys_xct;
	case page_img_format_log : return t_redo;
	case update_emlsn_log : return t_redo|t_single_sys_xct;
	case btree_insert_log : return t_redo|t_undo;
	case btree_insert_nonghost_log : return t_redo|t_undo;
	case btree_update_log : return t_redo|t_undo;
	case btree_overwrite_log : return t_redo|t_undo;
	case btree_ghost_mark_log : return t_redo|t_undo;
	case btree_ghost_reclaim_log : return t_redo|t_single_sys_xct;
	case btree_ghost_reserve_log : return t_redo|t_single_sys_xct;
	case btree_foster_adopt_log : return t_redo|t_multi|t_single_sys_xct;
	case btree_split_log : return t_redo|t_multi|t_single_sys_xct;
	case btree_compress_page_log : return t_redo|t_single_sys_xct;

        default: return t_bad_cat;
    }
}

#endif          /*</std-footer>*/
