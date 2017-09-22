#ifndef XCT_LOGGER_H
#define XCT_LOGGER_H

#include "sm.h"
#include "xct.h"
#include "btree_page_h.h"
#include "log_core.h"
#include "logrec_support.h"
#include "logrec_serialize.h"

class XctLogger
{
public:

    template <kind_t LR, class... Args>
    static void log(const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        bool should_log = smlevel_0::log && smlevel_0::logging_enabled && xd;
        if (!should_log)  { return; }

        auto redobuf = smthread_t::get_redo_buf();
        char* dest = redobuf->acquire();
        w_assert0(dest);
        auto logrec = reinterpret_cast<logrec_t*>(dest);
        logrec->init_header(LR);
        LogrecSerializer<LR>::serialize(nullptr, logrec, args...);
        w_assert1(logrec->valid_header());

        // REDO log records always pertain to a page and must therefore use log_p
        w_assert1(!logrec->is_redo());
        // This method is only used for xct_end right now
        w_assert1(logrec->type() == xct_end_log);
        w_assert1(!xd->is_piggy_backed_single_log_sys_xct());
        w_assert1(!logrec->is_undo());

        redobuf->release(logrec->length());
    }

    template <kind_t LR, class PagePtr, class... Args>
    static void log_p(PagePtr p, const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        bool should_log = smlevel_0::log && smlevel_0::logging_enabled && xd;
        if (!should_log)  { return; }

        if (_should_apply_img_compression(LR, p)) {
            // log this page image as an SX to keep it out of the xct undo chain
            sys_xct_section_t sx;
            log_p<page_img_format_log>(p);
            sx.end_sys_xct(RCOK);

            // Keep track of additional space created by page images on log
            auto extra_space = p->get_log_volume();
            w_assert3(extra_space > 0);
            ADD_TSTAT(log_img_format_bytes, extra_space);
            p->reset_log_volume();
        }

        auto redobuf = smthread_t::get_redo_buf();
        char* dest = redobuf->acquire();
        w_assert0(dest);
        auto logrec = reinterpret_cast<logrec_t*>(dest);
        logrec->init_header(LR);
        logrec->init_page_info(p);
        LogrecSerializer<LR>::serialize(p, logrec, args...);
        w_assert1(logrec->valid_header());

        _update_page_version(p, logrec);

        if (xd->is_single_log_sys_xct()) {
            // w_assert1(logrec->is_single_sys_xct());
            // SSX goes directly into log
            // CS FINELINE TODO: log insertion should be performed
            // by commit protocol, using redobuf like in user txns
            W_COERCE(ss_m::log->insert_raw(dest, logrec->length()));
            // argument 0 deletes log record
            redobuf->release(0);
            return;
        }

        redobuf->release(logrec->length());

        auto undobuf = smthread_t::get_undo_buf();
        dest = undobuf->acquire();
        if (logrec->is_undo() && dest) {
            auto len = UndoLogrecSerializer<LR>::serialize(dest, args...);
            StoreID stid = p->store();
            undobuf->release(len, stid, LR);
        }
    }

    template <kind_t LR, class PagePtr, class... Args>
    static void log_p(PagePtr p, PagePtr p2, const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        bool should_log = smlevel_0::log && smlevel_0::logging_enabled && xd;
        if (!should_log)  { return; }

        auto redobuf = smthread_t::get_redo_buf();
        char* dest = redobuf->acquire();
        w_assert0(dest);
        auto logrec = reinterpret_cast<logrec_t*>(dest);
        logrec->init_header(LR);
        logrec->init_page_info(p);
        LogrecSerializer<LR>::serialize(p, p2, logrec, args...);
        w_assert1(logrec->valid_header());

        // For multi-page log, also set LSN chain with a branch.
        w_assert1(logrec->is_multi_page());
        w_assert1(logrec->is_single_sys_xct());
        multi_page_log_t *multi = logrec->data_multi();
        w_assert1(multi->_page2_pid != 0);


        if (xd->is_single_log_sys_xct()) {
            w_assert1(logrec->is_single_sys_xct());
            _update_page_version(p, logrec);
            _update_page_version(p2, logrec);
            // SSX goes directly into log
            // CS FINELINE TODO: log insertion should be performed
            // by commit protocol, using redobuf like in user txns
            W_COERCE(ss_m::log->insert_raw(dest, logrec->length()));
            redobuf->release(0);
            return;
        }

        // CS TODO: so far, all multi-page logrecs are SSXs
        w_assert0(false);
        return;
    }

    /*
     * log_sys is used for system log records (e.g., checkpoints, clock
     * ticks, reads & writes, recovery events, debug stuff, stats, etc.)
     *
     * The difference to the other logging methods is that no xct or page
     * is involved and the logrec buffer is obtained with the 'new' operator.
     */
    template <kind_t LR, class... Args>
    static lsn_t log_sys(const Args&... args)
    {
        // this should use TLS allocator, so it's fast
        // (see macro DEFINE_SM_ALLOC in allocator.h and logrec.cpp)
        logrec_t* logrec = new logrec_t;

        logrec->init_header(LR);
        LogrecSerializer<LR>::serialize(nullptr, logrec, args...);
        w_assert1(logrec->valid_header());
        w_assert1(logrec_t::get_logrec_cat(LR) == logrec_t::t_system);

        lsn_t lsn;
        W_COERCE(ss_m::log->insert(*logrec, &lsn));
        logrec->set_lsn(lsn);

        delete logrec;
        return lsn;
    }

     template <class PagePtr>
     static void _update_page_version(PagePtr page, logrec_t* lr)
     {
         lsn_t lsn = page->get_page_lsn() + 1;
         page->update_page_lsn(lsn);
         page->increment_log_volume(lr->length());
         if (lr->pid() == page->pid()) {
             lr->set_lsn_ck(lsn);
         }
         else { // multi-page logrec
             lr->set_lsn2(lsn);
         }
     }

    template <class PagePtr>
    static bool _should_apply_img_compression(kind_t type, PagePtr page)
    {
        if (type == page_img_format_log) { return false; }

        auto comp = ss_m::log->get_page_img_compression();
        if (comp == 0) { return false; }
        auto vol = page->get_log_volume();
        if (vol >= comp) {
            page->reset_log_volume();
            return true;
        }
        return false;
    }

    static logrec_t* _get_logbuf(xct_t* xd)
    {
        if (xd->is_piggy_backed_single_log_sys_xct()) {
            return smthread_t::get_logbuf2();
        }
        return smthread_t::get_logbuf();
    }
};

// CS TODO this is a temporary alias -- at some point the SM should have its
// own generic Logger template argument
using Logger = XctLogger;

#endif
