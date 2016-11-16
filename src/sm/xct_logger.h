#ifndef XCT_LOGGER_H
#define XCT_LOGGER_H

#include "sm.h"
#include "btree_page_h.h"
#include "logdef_gen.h"
#include "log_core.h"

/*
 * Ideally, PagePtr would be a template argument of the log method, but that would mess
 * up template argument deduction in all the calls to thw three different versions.
 */
template <class PagePtr>
class XctLogger
{
public:

    /*
     * This method replaces the old log "stubs" that were generated by a Perl
     * script logdef.pl. Two overloads are required because of the cumbersome
     * way in which PageLSNs are managed in Zero (see xct_t::give_logbuf).
     */
    // CS TODO we need a new page-lsn update mechanism!
    template <class Logrec, class... Args>
    static rc_t log(const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        bool should_log = smlevel_0::log && smlevel_0::logging_enabled && xd;
        if (!should_log)  { return RCOK; }

        logrec_t* logrec = _get_logbuf(xd);
        new (logrec) Logrec;
        reinterpret_cast<Logrec*>(logrec)->construct(args...);

        // If it's a log for piggy-backed SSX, we call log->insert without updating _last_log
        // because this is a single log independent from other logs in outer transaction.
        if (xd->is_piggy_backed_single_log_sys_xct()) {
            w_assert1(logrec->is_single_sys_xct());
            lsn_t lsn;
            W_DO( ss_m::log->insert(*logrec, &lsn) );
            w_assert1(lsn != lsn_t::null);
            DBGOUT3(<< " SSX logged: " << logrec->type() << "\n new_lsn= " << lsn);
            return RCOK;
        }

        lsn_t lsn;
        W_DO(ss_m::log->insert(*logrec, &lsn));

        if (!logrec->is_single_sys_xct()) {
            logrec->fill_xct_attr(xd->tid(), lsn);
        }
        W_DO(xd->update_last_logrec(logrec, lsn));

        return RCOK;
    }

    template <class Logrec, class... Args>
    static rc_t log(PagePtr p, const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        bool should_log = smlevel_0::log && smlevel_0::logging_enabled && xd;
        if (!should_log)  { return RCOK; }

        logrec_t* logrec = _get_logbuf(xd);
        new (logrec) Logrec;
        reinterpret_cast<Logrec*>(logrec)->construct(p, args...);

        // set page LSN chain
        logrec->set_page_prev_lsn(p->get_page_lsn());

        // If it's a log for piggy-backed SSX, we call log->insert without updating _last_log
        // because this is a single log independent from other logs in outer transaction.
        if (xd->is_piggy_backed_single_log_sys_xct()) {
            w_assert1(logrec->is_single_sys_xct());
            lsn_t lsn;
            W_DO( ss_m::log->insert(*logrec, &lsn) );
            w_assert1(lsn != lsn_t::null);
            _update_page_lsns(p, lsn);
            DBGOUT3(<< " SSX logged: " << logrec->type() << "\n new_lsn= " << lsn);
            return RCOK;
        }

        lsn_t lsn;
        W_DO(ss_m::log->insert(*logrec, &lsn));

        if (!logrec->is_single_sys_xct()) {
            logrec->fill_xct_attr(xd->tid(), lsn);
        }
        W_DO(xd->update_last_logrec(logrec, lsn));
        _update_page_lsns(p, lsn);

        return RCOK;
    }

    template <class Logrec, class... Args>
    static rc_t log(PagePtr p, PagePtr p2, const Args&... args)
    {
        xct_t* xd = smthread_t::xct();
        bool should_log = smlevel_0::log && smlevel_0::logging_enabled && xd;
        if (!should_log)  { return RCOK; }

        logrec_t* logrec = _get_logbuf(xd);
        new (logrec) Logrec;
        reinterpret_cast<Logrec*>(logrec)->construct(p, p2, args...);

        // set page LSN chain
        logrec->set_page_prev_lsn(p->get_page_lsn());
        // For multi-page log, also set LSN chain with a branch.
        w_assert1(logrec->is_multi_page());
        w_assert1(logrec->is_single_sys_xct());
        multi_page_log_t *multi = logrec->data_ssx_multi();
        w_assert1(multi->_page2_pid != 0);
        multi->_page2_prv = p2->get_page_lsn();

        // If it's a log for piggy-backed SSX, we call log->insert without updating _last_log
        // because this is a single log independent from other logs in outer transaction.
        if (xd->is_piggy_backed_single_log_sys_xct()) {
            w_assert1(logrec->is_single_sys_xct());
            lsn_t lsn;
            W_DO( ss_m::log->insert(*logrec, &lsn) );
            w_assert1(lsn != lsn_t::null);
            _update_page_lsns(p, lsn);
            _update_page_lsns(p2, lsn);
            DBGOUT3(<< " SSX logged: " << logrec->type() << "\n new_lsn= " << lsn);
            return RCOK;
        }

        lsn_t lsn;
        W_DO(ss_m::log->insert(*logrec, &lsn));

        if (!logrec->is_single_sys_xct()) {
            logrec->fill_xct_attr(xd->tid(), lsn);
        }
        W_DO(xd->update_last_logrec(logrec, lsn));
        _update_page_lsns(p, lsn);
        _update_page_lsns(p2, lsn);

        return RCOK;
    }

    static void _update_page_lsns(PagePtr page, lsn_t new_lsn)
    {
        page->update_page_lsn(new_lsn);
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
#include "btree_page_h.h"
using Logger = XctLogger<btree_page_h*>;

#endif
