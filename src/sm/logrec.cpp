/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOGREC_C

#include "eventlog.h"

#include "sm_base.h"
#include "vec_t.h"
#include "alloc_cache.h"
#include "restore.h"
#include <sstream>
#include "logrec_handler.h"
#include "logrec_support.h"
#include "btree_page_h.h"

#include <iomanip>
typedef        ios::fmtflags        ios_fmtflags;

#include <new>

#include "allocator.h"
DECLARE_TLS(block_pool<logrec_t>, logrec_pool);
template<>
logrec_t* sm_tls_allocator::allocate(size_t)
{
    return (logrec_t*) logrec_pool->acquire();
}

template<>
void sm_tls_allocator::release(logrec_t* p, size_t)
{
    logrec_pool->release(p);
}

DEFINE_SM_ALLOC(logrec_t);

const logrec_t& logrec_t::get_skip_log()
{
    static logrec_t skip{skip_log};
    return skip;
}

/*********************************************************************
 *
 *  logrec_t::type_str()
 *
 *  Return a string describing the type of the log record.
 *
 *********************************************************************/
const char*
logrec_t::get_type_str(kind_t type)
{
    switch (type)  {
	case comment_log :
		return "comment";
	case skip_log :
		return "skip";
	case chkpt_begin_log :
		return "chkpt_begin";
	case add_backup_log :
		return "add_backup";
	case evict_page_log :
		return "evict_page";
	case fetch_page_log :
		return "fetch_page";
	case xct_end_log :
		return "xct_end";
	case xct_latency_dump_log :
		return "xct_latency_dump";
	case alloc_page_log :
		return "alloc_page";
	case dealloc_page_log :
		return "dealloc_page";
	case create_store_log :
		return "create_store";
	case alloc_format_log :
		return "alloc_format";
	case stnode_format_log :
		return "stnode_format";
	case append_extent_log :
		return "append_extent";
	case loganalysis_begin_log :
		return "loganalysis_begin";
	case loganalysis_end_log :
		return "loganalysis_end";
	case redo_done_log :
		return "redo_done";
	case undo_done_log :
		return "undo_done";
	case restore_begin_log :
		return "restore_begin";
	case restore_segment_log :
		return "restore_segment";
	case restore_end_log :
		return "restore_end";
	case warmup_done_log :
		return "warmup_done";
	case page_img_format_log :
		return "page_img_format";
	case update_emlsn_log :
		return "update_emlsn";
	case btree_insert_log :
		return "btree_insert";
	case btree_insert_nonghost_log :
		return "btree_insert_nonghost";
	case btree_update_log :
		return "btree_update";
	case btree_overwrite_log :
		return "btree_overwrite";
	case btree_ghost_mark_log :
		return "btree_ghost_mark";
	case btree_ghost_reclaim_log :
		return "btree_ghost_reclaim";
	case btree_ghost_reserve_log :
		return "btree_ghost_reserve";
	case btree_foster_adopt_log :
		return "btree_foster_adopt";
	case btree_unset_foster_log :
		return "btree_unset_foster";
	case btree_bulk_delete_log :
		return "btree_bulk_delete";
	case btree_compress_page_log :
		return "btree_compress_page";
	case tick_sec_log :
		return "tick_sec";
	case tick_msec_log :
		return "tick_msec";
	case benchmark_start_log :
		return "benchmark_start";
	case page_write_log :
		return "page_write";
	case page_read_log :
		return "page_read";
    default:
      return "UNKNOWN";
    }

    /*
     *  Not reached.
     */
    W_FATAL(eINTERNAL);
    return 0;
}

logrec_t::logrec_t(kind_t kind)
{
    header._type = kind;
    header._pid = 0;
    header._page_version = 0;
    set_size(0);
}

void logrec_t::init_header(kind_t type, PageID pid)
{
    header._type = type;
    header._pid = pid;
    header._page_version = 0;
    // CS TODO: for most logrecs, set_size is called twice
    set_size(0);
}

void logrec_t::set_size(size_t l)
{
    char *dat = data();
    if (l != ALIGN_BYTE(l)) {
        // zero out extra space to keep purify happy
        memset(dat+l, 0, ALIGN_BYTE(l)-l);
    }
    unsigned int tmp = ALIGN_BYTE(l) + (hdr_sz);
    tmp = (tmp + 7) & unsigned(-8); // force 8-byte alignment
    w_assert1(tmp <= sizeof(*this));
    header._len = tmp;
}

/*
 * Determine whether the log record header looks valid
 */
bool
logrec_t::valid_header() const
{
    return header.is_valid();
}


/*********************************************************************
 *  Invoke the redo method of the log record.
 *********************************************************************/
template <class PagePtr>
void logrec_t::redo(PagePtr page)
{
    DBG( << "Redo  log rec: " << *this << " size: " << header._len);

    switch (header._type)  {
	case alloc_page_log :
                LogrecHandler<alloc_page_log, PagePtr>::redo(this, page);
		break;
	case dealloc_page_log :
                LogrecHandler<dealloc_page_log, PagePtr>::redo(this, page);
		break;
	case alloc_format_log :
                LogrecHandler<alloc_format_log, PagePtr>::redo(this, page);
		break;
	case stnode_format_log :
                LogrecHandler<stnode_format_log, PagePtr>::redo(this, page);
		break;
	case create_store_log :
                LogrecHandler<create_store_log, PagePtr>::redo(this, page);
		break;
	case append_extent_log :
                LogrecHandler<append_extent_log, PagePtr>::redo(this, page);
		break;
	case page_img_format_log :
                LogrecHandler<page_img_format_log, PagePtr>::redo(this, page);
		break;
	case update_emlsn_log :
                LogrecHandler<update_emlsn_log, PagePtr>::redo(this, page);
		break;
	case btree_insert_log :
                LogrecHandler<btree_insert_log, PagePtr>::redo(this, page);
		break;
	case btree_insert_nonghost_log :
                LogrecHandler<btree_insert_nonghost_log, PagePtr>::redo(this, page);
		break;
	case btree_update_log :
                LogrecHandler<btree_update_log, PagePtr>::redo(this, page);
		break;
	case btree_overwrite_log :
                LogrecHandler<btree_overwrite_log, PagePtr>::redo(this, page);
		break;
	case btree_ghost_mark_log :
                LogrecHandler<btree_ghost_mark_log, PagePtr>::redo(this, page);
		break;
	case btree_ghost_reclaim_log :
                LogrecHandler<btree_ghost_reclaim_log, PagePtr>::redo(this, page);
		break;
	case btree_ghost_reserve_log :
                LogrecHandler<btree_ghost_reserve_log, PagePtr>::redo(this, page);
		break;
	case btree_foster_adopt_log :
                LogrecHandler<btree_foster_adopt_log, PagePtr>::redo(this, page);
		break;
	case btree_unset_foster_log :
                LogrecHandler<btree_unset_foster_log, PagePtr>::redo(this, page);
		break;
	case btree_bulk_delete_log :
                LogrecHandler<btree_bulk_delete_log, PagePtr>::redo(this, page);
		break;
	case btree_compress_page_log :
                LogrecHandler<btree_compress_page_log, PagePtr>::redo(this, page);
		break;
	default :
		W_FATAL(eINTERNAL);
		break;
    }

    page->set_version(page_version());
}

void logrec_t::redo()
{
    redo<btree_page_h*>(nullptr);
}


/*********************************************************************
 *
 *  logrec_t::undo(page)
 *
 *  Invoke the undo method of the log record. Automatically tag
 *  a compensation lsn to the last log record generated for the
 *  undo operation.
 *
 *********************************************************************/
void logrec_t::undo(kind_t type, StoreID stid, const char* data)
{
    using PagePtr = fixable_page_h*;
    switch (type) {
	case btree_insert_log :
                LogrecHandler<btree_insert_log, PagePtr>::undo(stid, data);
		break;
	case btree_insert_nonghost_log :
                LogrecHandler<btree_insert_nonghost_log, PagePtr>::undo(stid, data);
		break;
	case btree_update_log :
                LogrecHandler<btree_update_log, PagePtr>::undo(stid, data);
		break;
	case btree_overwrite_log :
                LogrecHandler<btree_overwrite_log, PagePtr>::undo(stid, data);
		break;
	case btree_ghost_mark_log :
                LogrecHandler<btree_ghost_mark_log, PagePtr>::undo(stid, data);
		break;
	default :
		W_FATAL(eINTERNAL);
		break;
    }
}

/*********************************************************************
 *
 *  logrec_t::corrupt()
 *
 *  Zero out most of log record to make it look corrupt.
 *  This is for recovery testing.
 *
 *********************************************************************/
void
logrec_t::corrupt()
{
    char* end_of_corruption = ((char*)this)+length();
    char* start_of_corruption = (char*)&header._type;
    size_t bytes_to_corrupt = end_of_corruption - start_of_corruption;
    memset(start_of_corruption, 0, bytes_to_corrupt);
}

/*********************************************************************
 *
 *  operator<<(ostream, logrec)
 *
 *  Pretty print a log record to ostream.
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, logrec_t& l)
{
    ios_fmtflags        f = o.flags();
    o.setf(ios::left, ios::left);

    o << l.type_str();
    o << " len=" << l.length();
    o << " pid=" << l.pid();
    o << " pversion=" << l.page_version();

    switch(l.type()) {
        case comment_log :
            {
                o << " " << (const char *)l._data;
                break;
            }
        case update_emlsn_log:
            {
                general_recordid_t slot;
                lsn_t lsn;
                deserialize_log_fields(&l, slot, lsn);
                o << " slot: " << slot << " emlsn: " << lsn;
                break;
            }
        case evict_page_log:
            {
                PageID pid;
                uint32_t version;
                deserialize_log_fields(&l, pid, version);
                o << " pid: " << pid << " version: " << version;
                break;
            }
        case fetch_page_log:
            {
                PageID pid;
                uint32_t version;
                StoreID store;
                deserialize_log_fields(&l, pid, version, store);
                o << " pid: " << pid << " version: " << version << " store: " << store;
                break;
            }
        case alloc_page_log:
        case dealloc_page_log:
            {
                PageID pid;
                deserialize_log_fields(&l, pid);
                o << " page: " << pid;
                break;
            }
        case create_store_log:
            {
                StoreID stid;
                PageID root_pid;
                deserialize_log_fields(&l, stid, root_pid);
                o << " stid: " <<  stid;
                o << " root_pid: " << root_pid;
                break;
            }
        case page_read_log:
            {
                PageID pid;
                uint32_t count;
                PageID end = pid + count - 1;
                deserialize_log_fields(&l, pid, count);
                o << " pids: " << pid << "-" << end;
                break;
            }
        case page_write_log:
            {
                PageID pid;
                lsn_t clean_lsn;
                uint32_t count;
                deserialize_log_fields(&l, pid, clean_lsn, count);
                PageID end = pid + count - 1;
                o << " pids: " << pid << "-" << end << " clean_lsn: " << clean_lsn;
                break;
            }
        case restore_segment_log:
            {
                uint32_t segment;
                deserialize_log_fields(&l, segment);
                o << " segment: " << segment;
                break;
            }
        case append_extent_log:
            {
                extent_id_t ext;
                StoreID snum;
                deserialize_log_fields(&l, snum, ext);
                o << " extent: " << ext << " store: " << snum;
                break;
            }


        default: /* nothing */
                break;
    }

    o.flags(f);
    return o;
}

template void logrec_t::template redo<btree_page_h*>(btree_page_h*);
template void logrec_t::template redo<fixable_page_h*>(fixable_page_h*);
