#ifndef LOG_INTERFACE_H
#define LOG_INTERFACE_H

#include <log.h>

#include "logrec_types.h"
#include "logrec_handler.h"

class ZeroLogInterface
{
public:

   using Type = LogRecordType;

   static constexpr uint8_t typeCount = static_cast<uint8_t>(Type::t_max_logrec);

   static constexpr uint8_t getFlags(Type type)
   {
       switch (type) {
           case Type::comment_log : return logrec_t::t_system;
           case Type::tick_sec_log : return logrec_t::t_system;
           case Type::tick_msec_log : return logrec_t::t_system;
           case Type::benchmark_start_log : return logrec_t::t_system;
           case Type::page_write_log : return logrec_t::t_system;
           case Type::page_read_log : return logrec_t::t_system;
           case Type::chkpt_begin_log : return logrec_t::t_system;
           case Type::loganalysis_begin_log : return logrec_t::t_system;
           case Type::loganalysis_end_log : return logrec_t::t_system;
           case Type::redo_done_log : return logrec_t::t_system;
           case Type::undo_done_log : return logrec_t::t_system;
           case Type::warmup_done_log: return logrec_t::t_system;
           case Type::restore_begin_log : return logrec_t::t_system;
           case Type::restore_segment_log : return logrec_t::t_system;
           case Type::restore_end_log : return logrec_t::t_system;
           case Type::xct_latency_dump_log : return logrec_t::t_system;
           case Type::add_backup_log : return logrec_t::t_system;
           case Type::evict_page_log : return logrec_t::t_system;
           case Type::fetch_page_log : return logrec_t::t_system;
           case Type::xct_end_log : return logrec_t::t_system;

           case Type::alloc_page_log : return logrec_t::t_redo;
           case Type::stnode_format_log : return logrec_t::t_redo|logrec_t::t_page_img;
           case Type::alloc_format_log : return logrec_t::t_redo|logrec_t::t_page_img;
           case Type::dealloc_page_log : return logrec_t::t_redo;
           case Type::create_store_log : return logrec_t::t_redo;
           case Type::append_extent_log : return logrec_t::t_redo;
           case Type::page_img_format_log : return logrec_t::t_redo|logrec_t::t_page_img;
           case Type::update_emlsn_log : return logrec_t::t_redo;
           case Type::btree_insert_log : return logrec_t::t_redo|logrec_t::t_undo;
           case Type::btree_insert_nonghost_log : return logrec_t::t_redo|logrec_t::t_undo;
           case Type::btree_update_log : return logrec_t::t_redo|logrec_t::t_undo;
           case Type::btree_overwrite_log : return logrec_t::t_redo|logrec_t::t_undo;
           case Type::btree_ghost_mark_log : return logrec_t::t_redo|logrec_t::t_undo;
           case Type::btree_ghost_reclaim_log : return logrec_t::t_redo;
           case Type::btree_ghost_reserve_log : return logrec_t::t_redo;
           case Type::btree_foster_adopt_log : return logrec_t::t_redo;
           case Type::btree_unset_foster_log : return logrec_t::t_redo;
           case Type::btree_bulk_delete_log : return logrec_t::t_redo;
           case Type::btree_compress_page_log : return logrec_t::t_redo;

           default: return logrec_t::t_bad;
       }
   }

   static constexpr uint8_t getFlags(uint8_t type)
   {
      return getFlags(base_to_enum<Type>(type));
   }

   static void initialize()
   {
      std::vector<uint8_t> flags{typeCount};
      for (uint8_t i = 0; i < typeCount; i++) {
         flags[i] = getFlags(base_to_enum<Type>(i));
      }
      logrec_t::initialize(flags.begin(), flags.end());
   }

   static const char* getTypeString(Type type)
   {
       switch (type)  {
           case Type::comment_log : return "comment";
           case Type::chkpt_begin_log : return "chkpt_begin";
           case Type::add_backup_log : return "add_backup";
           case Type::evict_page_log : return "evict_page";
           case Type::fetch_page_log : return "fetch_page";
           case Type::xct_end_log : return "xct_end";
           case Type::xct_latency_dump_log : return "xct_latency_dump";
           case Type::alloc_page_log : return "alloc_page";
           case Type::dealloc_page_log : return "dealloc_page";
           case Type::create_store_log : return "create_store";
           case Type::alloc_format_log : return "alloc_format";
           case Type::stnode_format_log : return "stnode_format";
           case Type::append_extent_log : return "append_extent";
           case Type::loganalysis_begin_log : return "loganalysis_begin";
           case Type::loganalysis_end_log : return "loganalysis_end";
           case Type::redo_done_log : return "redo_done";
           case Type::undo_done_log : return "undo_done";
           case Type::restore_begin_log : return "restore_begin";
           case Type::restore_segment_log : return "restore_segment";
           case Type::restore_end_log : return "restore_end";
           case Type::warmup_done_log : return "warmup_done";
           case Type::page_img_format_log : return "page_img_format";
           case Type::update_emlsn_log : return "update_emlsn";
           case Type::btree_insert_log : return "btree_insert";
           case Type::btree_insert_nonghost_log : return "btree_insert_nonghost";
           case Type::btree_update_log : return "btree_update";
           case Type::btree_overwrite_log : return "btree_overwrite";
           case Type::btree_ghost_mark_log : return "btree_ghost_mark";
           case Type::btree_ghost_reclaim_log : return "btree_ghost_reclaim";
           case Type::btree_ghost_reserve_log : return "btree_ghost_reserve";
           case Type::btree_foster_adopt_log : return "btree_foster_adopt";
           case Type::btree_unset_foster_log : return "btree_unset_foster";
           case Type::btree_bulk_delete_log : return "btree_bulk_delete";
           case Type::btree_compress_page_log : return "btree_compress_page";
           case Type::tick_sec_log : return "tick_sec";
           case Type::tick_msec_log : return "tick_msec";
           case Type::benchmark_start_log : return "benchmark_start";
           case Type::page_write_log : return "page_write";
           case Type::page_read_log : return "page_read";
           case Type::t_max_logrec : return "max_logrec";
       }

       /*
        *  Not reached.
        */
       w_assert0(false);
       return nullptr;
   }

   static const char* getTypeString(uint8_t type)
   {
      return getTypeString(base_to_enum<Type>(type));
   }

   /*********************************************************************
    *  Invoke the redo method of the log record.
    *********************************************************************/
   template <class PagePtr>
   static void redo(const logrec_t* lr, PagePtr page)
   {
       // DBG( << "Redo  log rec: " << *this << " size: " << header._len);

       switch (base_to_enum<Type>(lr->type()))  {
           case Type::alloc_page_log : LogrecHandler<Type::alloc_page_log, PagePtr>::redo(lr, page); break;
           case Type::dealloc_page_log : LogrecHandler<Type::dealloc_page_log, PagePtr>::redo(lr, page); break;
           case Type::alloc_format_log : LogrecHandler<Type::alloc_format_log, PagePtr>::redo(lr, page); break;
           case Type::stnode_format_log : LogrecHandler<Type::stnode_format_log, PagePtr>::redo(lr, page); break;
           case Type::create_store_log : LogrecHandler<Type::create_store_log, PagePtr>::redo(lr, page); break;
           case Type::append_extent_log : LogrecHandler<Type::append_extent_log, PagePtr>::redo(lr, page); break;
           case Type::page_img_format_log : LogrecHandler<Type::page_img_format_log, PagePtr>::redo(lr, page); break;
           case Type::update_emlsn_log : LogrecHandler<Type::update_emlsn_log, PagePtr>::redo(lr, page); break;
           case Type::btree_insert_log : LogrecHandler<Type::btree_insert_log, PagePtr>::redo(lr, page); break;
           case Type::btree_insert_nonghost_log : LogrecHandler<Type::btree_insert_nonghost_log, PagePtr>::redo(lr, page); break;
           case Type::btree_update_log : LogrecHandler<Type::btree_update_log, PagePtr>::redo(lr, page); break;
           case Type::btree_overwrite_log : LogrecHandler<Type::btree_overwrite_log, PagePtr>::redo(lr, page); break;
           case Type::btree_ghost_mark_log : LogrecHandler<Type::btree_ghost_mark_log, PagePtr>::redo(lr, page); break;
           case Type::btree_ghost_reclaim_log : LogrecHandler<Type::btree_ghost_reclaim_log, PagePtr>::redo(lr, page); break;
           case Type::btree_ghost_reserve_log : LogrecHandler<Type::btree_ghost_reserve_log, PagePtr>::redo(lr, page); break;
           case Type::btree_foster_adopt_log : LogrecHandler<Type::btree_foster_adopt_log, PagePtr>::redo(lr, page); break;
           case Type::btree_unset_foster_log : LogrecHandler<Type::btree_unset_foster_log, PagePtr>::redo(lr, page); break;
           case Type::btree_bulk_delete_log : LogrecHandler<Type::btree_bulk_delete_log, PagePtr>::redo(lr, page); break;
           case Type::btree_compress_page_log : LogrecHandler<Type::btree_compress_page_log, PagePtr>::redo(lr, page); break;
           default:
               w_fatal("Log record type does not support redo");
               break;
       }

       page->set_version(lr->page_version());
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
   static void undo(Type type, StoreID stid, const char* data)
   {
       using PagePtr = fixable_page_h*;
       switch (type) {
           case Type::btree_insert_log : LogrecHandler<Type::btree_insert_log, PagePtr>::undo(stid, data); break;
           case Type::btree_insert_nonghost_log : LogrecHandler<Type::btree_insert_nonghost_log, PagePtr>::undo(stid, data); break;
           case Type::btree_update_log : LogrecHandler<Type::btree_update_log, PagePtr>::undo(stid, data); break;
           case Type::btree_overwrite_log : LogrecHandler<Type::btree_overwrite_log, PagePtr>::undo(stid, data); break;
           case Type::btree_ghost_mark_log : LogrecHandler<Type::btree_ghost_mark_log, PagePtr>::undo(stid, data); break;
           default :
               w_fatal("Log record type does not support undo");
               break;
       }
   }

   static void undo(uint8_t type, StoreID stid, const char* data)
   {
      return undo(base_to_enum<Type>(type), stid, data);
   }

   static std::ostream& printContents(const logrec_t& lr, std::ostream& o)
   {
       switch(base_to_enum<Type>(lr.type())) {
          case Type::comment_log :
               {
                   o << " " << lr.data();
                   break;
               }
          case Type::update_emlsn_log:
               {
                   general_recordid_t slot;
                   lsn_t lsn;
                   deserialize_log_fields(&lr, slot, lsn);
                   o << " slot: " << slot << " emlsn: " << lsn;
                   break;
               }
          case Type::evict_page_log:
               {
                   PageID pid;
                   uint32_t version;
                   deserialize_log_fields(&lr, pid, version);
                   o << " pid: " << pid << " version: " << version;
                   break;
               }
          case Type::fetch_page_log:
               {
                   PageID pid;
                   uint32_t version;
                   StoreID store;
                   deserialize_log_fields(&lr, pid, version, store);
                   o << " pid: " << pid << " version: " << version << " store: " << store;
                   break;
               }
          case Type::alloc_page_log:
          case Type::dealloc_page_log:
               {
                   PageID pid;
                   deserialize_log_fields(&lr, pid);
                   o << " page: " << pid;
                   break;
               }
          case Type::create_store_log:
               {
                   StoreID stid;
                   PageID root_pid;
                   deserialize_log_fields(&lr, stid, root_pid);
                   o << " stid: " <<  stid;
                   o << " root_pid: " << root_pid;
                   break;
               }
          case Type::page_read_log:
               {
                   PageID pid;
                   uint32_t count;
                   PageID end = pid + count - 1;
                   deserialize_log_fields(&lr, pid, count);
                   o << " pids: " << pid << "-" << end;
                   break;
               }
          case Type::page_write_log:
               {
                   PageID pid;
                   lsn_t clean_lsn;
                   uint32_t count;
                   deserialize_log_fields(&lr, pid, clean_lsn, count);
                   PageID end = pid + count - 1;
                   o << " pids: " << pid << "-" << end << " clean_lsn: " << clean_lsn;
                   break;
               }
          case Type::restore_segment_log:
               {
                   uint32_t segment;
                   deserialize_log_fields(&lr, segment);
                   o << " segment: " << segment;
                   break;
               }
          case Type::append_extent_log:
               {
                   extent_id_t ext;
                   StoreID snum;
                   deserialize_log_fields(&lr, snum, ext);
                   o << " extent: " << ext << " store: " << snum;
                   break;
               }
           default: /* nothing */
                   break;
       }

       return o;
   }


};

inline ostream& operator<<(ostream& o, const logrec_t& lr)
{
    auto f = o.flags();
    o.setf(ios::left, ios::left);

    o << ZeroLogInterface::getTypeString(lr.type());
    o << " len=" << lr.length();
    o << " pid=" << lr.pid();
    o << " pversion=" << lr.page_version();
    ZeroLogInterface::printContents(lr, o);
    o.flags(f);
    return o;
}

#endif
