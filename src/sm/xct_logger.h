#ifndef XCT_LOGGER_H
#define XCT_LOGGER_H

#include "sm.h"
#include "xct.h"
#include "btree_page_h.h"
#include "log_core.h"
#include "logrec_support.h"
#include "logrec_serialize.h"

class UndoOnlyLogger
{
public:

    template <kind_t LR, class... Args>
    static void log(const Args&... args)
    {
        log_sys<LR>(args...);
    }

    template <kind_t LR, class PagePtr, class... Args>
    static void log_p(PagePtr p, const Args&... args)
    {
        p->incr_version();

        if (logrec_t::get_logrec_cat(LR) & logrec_t::t_undo) {
            auto undobuf = smthread_t::get_undo_buf();
            char* dest = undobuf->acquire();
            if (dest) {
                auto len = UndoLogrecSerializer<LR>::serialize(dest, args...);
                StoreID stid = p->store();
                undobuf->release(len, stid, LR);
            }
        }
    }

    /// This Logger still generates system log records
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
        // logrec->set_lsn(lsn);

        delete logrec;
        return lsn;
    }
};

class XctLogger
{
public:

    template <kind_t LR, class... Args>
    static void log(const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        auto redobuf = _get_redo_buffer();
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
        w_assert1(!xd->is_sys_xct());
        w_assert1(!logrec->is_undo());

        redobuf->release(logrec->length());
    }

    template <kind_t LR, class PagePtr, class... Args>
    static void log_p(PagePtr p, const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        if (_should_apply_img_compression(LR, p)) {
            // RULE: Always log before applying update, otherwise compression won't work!
            // log this page image as an SX to keep it out of the xct undo chain
            sys_xct_section_t sx;
            log_p<page_img_format_log>(p);
            W_COERCE(sx.end_sys_xct(RCOK));

            // Keep track of additional space created by page images on log
            auto extra_space = p->get_log_volume();
            w_assert3(extra_space > 0);
            ADD_TSTAT(log_img_format_bytes, extra_space);
            p->reset_log_volume();
        }

        auto redobuf = _get_redo_buffer();
        char* dest = redobuf->acquire();
        w_assert0(dest);
        auto logrec = reinterpret_cast<logrec_t*>(dest);
        logrec->init_header(LR, p->pid());
        LogrecSerializer<LR>::serialize(p, logrec, args...);
        w_assert1(logrec->valid_header());

        p->increment_log_volume(logrec->length());
        p->incr_version();
        w_assert1 (logrec->pid() == p->pid());
        logrec->set_page_version(p->version());
        p->set_epoch(redobuf->get_epoch());
        redobuf->release(logrec->length());

        if (logrec->is_undo()) {
            auto undobuf = smthread_t::get_undo_buf();
            dest = undobuf->acquire();
            if (dest) {
                auto len = UndoLogrecSerializer<LR>::serialize(dest, args...);
                StoreID stid = p->store();
                undobuf->release(len, stid, LR);
            }
        }
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
        if (!ss_m::log) { return lsn_t(0,0); }

        // this should use TLS allocator, so it's fast
        // (see macro DEFINE_SM_ALLOC in allocator.h and logrec.cpp)
        logrec_t* logrec = new logrec_t;

        logrec->init_header(LR);
        LogrecSerializer<LR>::serialize(nullptr, logrec, args...);
        w_assert1(logrec->valid_header());
        w_assert1(logrec_t::get_logrec_cat(LR) == logrec_t::t_system);

        lsn_t lsn;
        W_COERCE(ss_m::log->insert(*logrec, &lsn));
        // logrec->set_lsn(lsn);

        delete logrec;
        return lsn;
    }

    static RedoBuffer* _get_redo_buffer()
    {
        auto redobuf = smthread_t::get_redo_buf();
        // Initialize epoch, if not done yet
        if (redobuf->get_size() == 0) {
            auto epoch = smlevel_0::log->get_epoch_tracker().acquire();
            redobuf->set_epoch(epoch);
        }
        return redobuf;
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
        if (xd->is_sys_xct()) {
            return smthread_t::get_logbuf2();
        }
        return smthread_t::get_logbuf();
    }
};

// CS TODO this is a temporary alias -- at some point the SM should have its
// own generic Logger template argument
// using Logger = UndoOnlyLogger;
using Logger = XctLogger;

#endif
