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

/*<std-header orig-src='shore' incl-file-exclusion='XCT_H'>

 $Id: xct.h,v 1.161 2010/12/08 17:37:43 nhall Exp $

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

#ifndef XCT_H
#define XCT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if W_DEBUG_LEVEL > 2
// You can rebuild with this turned on
// if you want comment log records inserted into the log
// to help with deciphering the log when recovery bugs
// are nasty.
#define  X_LOG_COMMENT_ON 1
#define  ADD_LOG_COMMENT_SIG ,const char *debugmsg
#define  ADD_LOG_COMMENT_USE ,debugmsg
#define  X_LOG_COMMENT_USE(x)  ,x

#else

#define  X_LOG_COMMENT_ON 0
#define  ADD_LOG_COMMENT_SIG
#define  ADD_LOG_COMMENT_USE
#define  X_LOG_COMMENT_USE(x)
#endif

#include <chrono>
#include <set>
#include <atomic>
#include <AtomicCounter.hpp>
#include "w_key.h"
#include "lsn.h"
#include "allocator.h"
#include "latch.h"

struct okvl_mode;
struct RawXct;
class lockid_t; // forward
class xct_i; // forward
class restart_thread_t; // forward
class lock_m; // forward
class lock_core_m; // forward
class lock_request_t; // forward
class xct_lock_info_t; // forward
class smthread_t; // forward
class lil_private_table;
class logrec_t; // forward
class fixable_page_h; // forward

/**
 * Results of in-query (not batch) BTree verification.
 * In-query verification is on when xct_t::set_inquery_verify(true).
 * Use In-query verification as follows:
 * \verbatim
xct()->set_inquery_verify(true); // verification mode on
xct()->set_inquery_verify_keyorder(true); // detailed check for sortedness/uniqueness
xct()->set_inquery_verify_space(true); // detailed check for space overlap
ss_m::create_assoc(...);
ss_m::find_assoc(...);
...
const inquery_verify_context_t &result = xct()->inquery_verify_context();
cout << "checked " << result.pages_checked << "pages"
 << " and found " << result.pids_inconsistent.size() << " inconsistencies.";
if (result.pids_inconsistent.size() > 0) {
  // output IDs of inconsistent pages etc..
}
}\endverbatim
 */
class inquery_verify_context_t {
public:
    inquery_verify_context_t() : pages_checked(0), next_pid(0), next_level(0) {
    }

    /** total count of pages checked (includes checks of same page). */
    int32_t pages_checked;
    /** ID of pages that had some inconsistency. */
    std::set<PageID> pids_inconsistent;

    /** expected next page id. */
    PageID next_pid;
    /** expected next page level. -1 means "don't check" (only for root page). */
    int16_t next_level;
    /** expected next fence-low key. */
    w_keystr_t next_low_key;
    /** expected next fence-high key. */
    w_keystr_t next_high_key;
};

/**\cond skip
 * \brief Class used to keep track of stores to be
 * freed or changed from tmp to regular at the end of
 * a transaction
 */
class stid_list_elem_t  {
    public:
    StoreID        stid;
    w_link_t    _link;

    stid_list_elem_t(const StoreID& theStid)
        : stid(theStid)
        {};
    ~stid_list_elem_t()
    {
        if (_link.member_of() != NULL)
            _link.detach();
    }
    static uint32_t    link_offset()
    {
        return W_LIST_ARG(stid_list_elem_t, _link);
    }
};
/**\endcond skip */




/**
 * \brief A transaction. Internal to the storage manager.
 * \ingroup SSMXCT
 * This class may be used in a limited way for the handling of
 * out-of-log-space conditions.  See \ref SSMLOG.
 */
class xct_t : public smlevel_0 {
/**\cond skip */
    friend class xct_i;
    friend class smthread_t;
    friend class lock_m;
    friend class lock_core_m;
    friend class lock_request_t;

public:
    typedef xct_state_t           state_t;

    /* A nearly-POD struct whose only job is to enable a N:1
       relationship between the log streams of a transaction (xct_t)
       and its core functionality such as locking and 2PC (xct_core).

       Any transaction state which should not eventually be replicated
       per-thread goes here. Usually such state is protected by the
       1-thread-xct-mutex.

       Static data members can stay in xct_t, since they're not even
       duplicated per-xct, let alone per-thread.
     */
    struct xct_core
    {
        xct_core(tid_t const &t, state_t s, int timeout);
        ~xct_core();

        //-- from xct.h ----------------------------------------------------
        tid_t                  _tid;
        int          _timeout; // default timeout value for lock reqs
        xct_lock_info_t*       _lock_info;
        lil_private_table*     _lil_lock_info;

        /** RAW-style lock manager's shadow transaction object. Garbage collected. */
        RawXct*                _raw_lock_xct;

        state_t                   _state;

        // CS: Using these instead of the old new_xct and destroy_xct methods
        void* operator new(size_t s);
        void operator delete(void* p, size_t s);
    };

protected:
    xct_core* _core;

protected:
    enum commit_t { t_normal = 0, t_lazy = 1 };


/**\endcond skip */

/**\cond skip */
public:

    rc_t                      commit_free_locks(bool read_lock_only = false, lsn_t commit_lsn = lsn_t::null);
    rc_t                      early_lock_release(lsn_t last_lsn);

    bool has_logs();

    // CS: Using these instead of the old new_xct and destroy_xct methods
    void* operator new(size_t s);
    void operator delete(void* p, size_t s);

public:
    NORET                       xct_t(
            sm_stats_t*    stats = NULL,
            int       timeout = timeout_t::WAIT_SPECIFIED_BY_THREAD,
            const tid_t&        tid = 0,
            const lsn_t&        last_lsn = lsn_t::null,
            bool                loser_xct = false
            );
    ~xct_t();

public:

    friend ostream&             operator<<(ostream&, const xct_t&);

    state_t                     state() const;
    void                        set_timeout(int t) ;

    int               timeout_c() const;

    /*
     * basic tx commands:
     */
public:
    static void                 dump(ostream &o);
    static void                  cleanup(bool shutdown_clean = true);


    bool                        is_instrumented() {
                                   return (__stats != 0);
                                }
    void                        give_stats(sm_stats_t* s) {
                                    w_assert1(__stats == 0);
                                    __stats = s;
                                }
    void                        clear_stats() {
                                    memset(__stats,0, sizeof(*__stats));
                                }
    sm_stats_t*            steal_stats() {
                                    sm_stats_t*s = __stats;
                                    __stats = 0;
                                    return         s;
                                }
    const sm_stats_t&      const_stats_ref() { return *__stats; }
    rc_t                        commit(bool sync_log = true);
    rc_t                        rollback();
    rc_t                        abort(bool save_stats = false);

    // used by restart.cpp, some logrecs
protected:
    sm_stats_t&            stats_ref() { return *__stats; }
    rc_t                        dispose();
    void                        change_state(state_t new_state);
/**\endcond skip */

public:

    // used by restart, chkpt among others
    static xct_t*               look_up(const tid_t& tid);
    static tid_t                oldest_tid();        // with min tid value
    static tid_t                youngest_tid();        // with max tid value
/**\cond skip */
    static void                 update_youngest_tid(const tid_t &);
/**\endcond skip */

    // used by sm.cpp:
    static uint32_t    num_active_xcts();

public: // not quite public thing.. but convenient for experiments
    xct_lock_info_t*             lock_info() const;
    lil_private_table*           lil_lock_info() const;
    RawXct*                      raw_lock_xct() const;

public:
    // XXX this is only for chkpt::take().  This problem needs to
    // be fixed correctly.  DO NOT USE THIS.  Really want a
    // friend that is just a friend on some methods, not the entire class.
    static w_rc_t                acquire_xlist_mutex();
    static void                  release_xlist_mutex();
    static void                  assert_xlist_mutex_not_mine();
    static void                  assert_xlist_mutex_is_mine();
    static bool                  xlist_mutex_is_mine();



/////////////////////////////////////////////////////////////////
// DATA
/////////////////////////////////////////////////////////////////
protected:
    // list of all transactions instances
    w_link_t                      _xlink;
    static w_descend_list_t<xct_t, queue_based_lock_t, tid_t> _xlist;
    void                         put_in_order();

    // CS TODO: TID assignment could be thread-local
    static std::atomic<tid_t>                 _nxt_tid;// only safe for pre-emptive
                                        // threads on 64-bit platforms
    static tid_t                 _oldest_tid;
private:
    static queue_based_lock_t    _xlist_mutex;

    sm_stats_t*             __stats; // allocated by user
    lockid_t*                    __saved_lockid_t;

    tid_t                        _tid;

    /**
     * \brief The count of consecutive SSXs conveyed by this transaction object.
     * \details
     * SSX can't nest SSX.
     * However, as SSX doesn't care what the transaction object is, SSX can \e chain
     * an arbitraly number of SSXs as far as they are consecutive SSXs, no multi-log
     * system transactions or user-transactions in-between.
     * In that case, we simply increment/decrement this counter when we
     * start/end SSX. Chaining is useful when SSX operation might cause another SSX
     * operation, eg ghost-reservation causes page-split which causes page-evict etc etc.
     */
    uint32_t                     _ssx_chain_len;

    static constexpr size_t MaxDepth = 8;
    std::array<size_t, MaxDepth> _ssx_positions;

    /** concurrency mode of this transaction. */
    concurrency_t                _query_concurrency;
    /** whether to take X lock for lookup/cursor. */
    bool                         _query_exlock_for_select;
// hey, these could be one integer with OR-ed flags

    /**
     * whether to defer the logging and applying of the change made
     * by single-log system transaxction (SSX). Experimental.
     */
    bool                         _deferred_ssx;

    /** whether in-query verification is on. */
    bool                         _inquery_verify;
    /** whether to additionally check the sortedness and uniqueness of keys. */
    bool                         _inquery_verify_keyorder;
    /** whether to check any overlaps of records and integrity of space offset. */
    bool                         _inquery_verify_space;

    /** result and context of in-query verification. */
    inquery_verify_context_t     _inquery_verify_context;

    // Latch object mainly for checkpoint to access information in txn object
    latch_t                      _latch;

protected:
    void flush_redo_buffer(bool sys_xct = false, bool sync = true);
    rc_t                _abort();
    rc_t _commit_read_only(lsn_t& inherited_read_watermark);

private:
    bool                        one_thread_attached() const;   // assertion

public:

    bool                        is_sys_xct () const { return _ssx_chain_len > 0; }
    void push_ssx();
    void pop_ssx();

    void                        set_inquery_verify(bool enabled) { _inquery_verify = enabled; }
    bool                        is_inquery_verify() const { return _inquery_verify; }
    void                        set_inquery_verify_keyorder(bool enabled) { _inquery_verify_keyorder = enabled; }
    bool                        is_inquery_verify_keyorder() const { return _inquery_verify_keyorder; }
    void                        set_inquery_verify_space(bool enabled) { _inquery_verify_space = enabled; }
    bool                        is_inquery_verify_space() const { return _inquery_verify_space; }

    const inquery_verify_context_t& inquery_verify_context() const { return _inquery_verify_context;}
    inquery_verify_context_t& inquery_verify_context() { return _inquery_verify_context;}


    concurrency_t                get_query_concurrency() const { return _query_concurrency; }
    void                         set_query_concurrency(concurrency_t mode) { _query_concurrency = mode; }
    bool                         get_query_exlock_for_select() const {return _query_exlock_for_select;}
    void                         set_query_exlock_for_select(bool mode) {_query_exlock_for_select = mode;}

    ostream &                   dump_locks(ostream &) const;

    /////////////////////////////////////////////////////////////////
private:
    /////////////////////////////////////////////////////////////////
    // non-const because it acquires mutex:
    // removed, now that the lock mgrs use the const,INLINE-d form
    // int        timeout();

    static void                 xct_stats(
                                    u_long&             begins,
                                    u_long&             commits,
                                    u_long&             aborts,
                                    bool                 reset);

    w_rc_t                     _sync_logbuf(bool block=true, bool signal=true);
    void                       _teardown();

public:
    /**
     * Early Lock Release mode.
     * This is a totally separated implementation from Quarks.
     * @see _read_watermark
     */
    enum elr_mode_t {
        /** ELR is disabled. */
        elr_none,

        /** ELR releases only S, U, and intent locks (same as Quarks?). */
        elr_s,

        /**
         * ELR releases all locks. When this mode is on, even read-only transactions
         * do an additional check to maintain serializability. So, do NOT forget to
         * set this mode to ALL transactions if you are using it for any of
         * your transactions.
         */
        elr_sx,

        /**
         * ELR releases no locks but gives permissions for its locks
         * to be violated.  When this mode is on, even read-only
         * transactions do an additional check to maintain
         * serializability.  So, do NOT forget to set this mode to ALL
         * transactions if you are using it for any of your
         * transactions.
         */
        elr_clv
    };

protected: // all data members protected
    /**
     * Whenever a transaction acquires some lock,
     * this value is updated as _read_watermark=max(_read_watermark, lock_bucket.tag)
     * so that we maintain a maximum commit LSN of transactions it depends on.
     * This value is used to commit a read-only transaction with Safe SX-ELR to block
     * until the log manager flushed the log buffer at least to this value.
     * Assuming this protocol, we can do ELR for x-locks.
     * See jira ticket:99 "ELR for X-lock" (originally trac ticket:101).
     */
    lsn_t                        _read_watermark;

    elr_mode_t                   _elr_mode;

    // timestamp for calculating latency
    std::chrono::high_resolution_clock::time_point _begin_tstamp;

public:
#if W_DEBUG_LEVEL > 2
private:
    bool                        _had_error;
public:
    // Tells if we ever did a partial rollback.
    // This state is only needed for certain assertions.
    void                        set_error_encountered() { _had_error = true; }
    bool                        error_encountered() const {
                                               return _had_error; }
#else
    void                        set_error_encountered() {}
    bool                        error_encountered() const {  return false; }
#endif
    tid_t                       tid() const {
                                    w_assert1(_core == NULL || _tid == _core->_tid);
                                    return _tid; }
    uint32_t&                   ssx_chain_len() { return _ssx_chain_len;}

    const lsn_t&                get_read_watermark() const { return _read_watermark; }
    void                        update_read_watermark(const lsn_t &tag) {
        if (_read_watermark < tag) {
            _read_watermark = tag;
        }
    }
    elr_mode_t                  get_elr_mode() const { return _elr_mode; }
    void                        set_elr_mode(elr_mode_t mode) { _elr_mode = mode; }

    // Latch the xct object in order to access internal data
    // it is to prevent data changing while reading them
    // Mainly for checkpoint logging purpose
    latch_t* latchp() const
    {
        // If _core is gone (txn is being destroyed), return NULL
        // CS TODO: this is incorrect. Threads waiting on the latch after
        // core is destructed will encounter a segmentation fault. The real
        // problem here is that an object should not be destroyed while some
        // thread may still try to access it. We need a different design or
        // some higher form of concurrency control.
        // if ( NULL == _core)
        //     return (latch_t *)NULL;

        return const_cast<latch_t*>(&(_latch));
    }
    latch_t &latch()
    {
        return *latchp();
    }

};

/* XXXX This is somewhat hacky becuase I am working on cleaning
   up the xct_i xct iterator to provide various levels of consistency.
   Until then, the "locking option" provides enough variance so
   code need not be duplicated or have deep call graphs. */

/**\brief Iterator over transaction list.
 *
 * This is exposed for the purpose of coping with out-of-log-space
 * conditions. See \ref SSMLOG.
 */
class xct_i  {
public:
    // NB: still not safe, since this does not
    // lock down the list for the entire iteration.

    // FRJ: Making it safe -- all non-debug users lock it down
    // manually right now anyway; the rest *should* to avoid bugs.

    /// True if this thread holds the transaction list mutex.
    bool locked_by_me() const {
        if(xct_t::xlist_mutex_is_mine()) {
            W_IFDEBUG1(if(_may_check) w_assert1(_locked);)
            return true;
        }
        return false;
    }

    /// Release transaction list mutex if this thread holds it.
    void never_mind() {
        // Be careful here: must leave in the
        // state it was when we constructed this.
        if(_locked && locked_by_me()) {
            *(const_cast<bool *>(&_locked)) = false; // grot
            xct_t::release_xlist_mutex();
        }
    }
    /// Get transaction at cursor.
    xct_t* curr() const { return unsafe_iterator.curr(); }
    /// Advance cursor.
    xct_t* next() { return unsafe_iterator.next(); }

    /**\cond skip */
    // Note that this is called to INIT the attribute "locked"
    static bool init_locked(bool lockit)
    {
        if(lockit) {
            W_COERCE(xct_t::acquire_xlist_mutex());
        }
        return lockit;
    }
    /**\endcond skip */

    /**\brief Constructor.
    *
    * @param[in] locked_accesses Set to true if you want this
    * iterator to be safe, false if you don't care or if you already
    * hold the transaction-list mutex.
    */
    NORET xct_i(bool locked_accesses)
        : _locked(init_locked(locked_accesses)),
        _may_check(locked_accesses),
        unsafe_iterator(xct_t::_xlist)
    {
        w_assert1(_locked == locked_accesses);
        _check(_locked);
    }

    /// Desctructor. Calls never_mind() if necessary.
    NORET ~xct_i() {
        if(locked_by_me()) {
          _check(true);
          never_mind();
          _check(false);
        }
    }

private:
    void _check(bool b) const  {
          if(!_may_check) return;
          if(b) xct_t::assert_xlist_mutex_is_mine();
          else  xct_t::assert_xlist_mutex_not_mine();
    }
    // FRJ: make sure init_locked runs before we actually create the iterator
    const bool            _locked;
    const bool            _may_check;
    w_list_i<xct_t,queue_based_lock_t> unsafe_iterator;

    // disabled
    xct_i(const xct_i&);
    xct_i& operator=(const xct_i&);
};


/**\cond skip */
inline
xct_t::state_t
xct_t::state() const
{
    if (NULL == _core)
        return xct_ended;
    return _core->_state;
}

// For use in sm functions that don't allow
// active xct when entered.  These are functions that
// apply to local volumes only.
class xct_auto_abort_t : public smlevel_0 {
public:
    xct_auto_abort_t() : _xct(new xct_t()) {
    }
    ~xct_auto_abort_t() {
        switch(_xct->state()) {
        case smlevel_0::xct_ended:
            // do nothing
            break;
        case smlevel_0::xct_active:
        case smlevel_0::xct_freeing_space: // we got an error in commit
        case smlevel_0::xct_committing: // we got an error in commit
            W_COERCE(_xct->abort());
            break;
        default:
            cerr << "unexpected xct state: " << _xct->state() << endl;
            W_FATAL(eINTERNAL);
        }
        delete _xct;
    }
    rc_t commit() {
        W_DO(_xct->commit());
        return RCOK;
    }
    rc_t abort() {W_DO(_xct->abort()); return RCOK;}

private:
    xct_t*        _xct;
};


inline
bool
operator>(const xct_t& x1, const xct_t& x2)
{
    return (x1.tid() > x2.tid());
}


/**\endcond skip */

// TODO. this should accept store id/volume id.
// it should say 'does not need' if we have absolute locks in LIL.
// same thing can be manually achieved by user code, though.
inline bool
g_xct_does_need_lock()
{
    xct_t* x = xct();
    if (x == NULL)  return false;
    if (x->is_sys_xct()) return false; // system transaction never needs locks
    return x->get_query_concurrency() == smlevel_0::t_cc_keyrange;
}

inline bool
g_xct_does_ex_lock_for_select()
{
    xct_t* x = xct();
    return x && x->get_query_exlock_for_select();
}

/**
 * \brief Used to automatically begin/commit/abort a system transaction.
 * \details
 * Use this class as follows:
 * \verbatim
rc_t some_function ()
{
  // the function to use system transaction
  sys_xct_section_t sxs;
  W_DO (sxs.check_error_on_start()); // optional: check the system xct successfully started
  rc_t result = do_some_thing();
  W_DO (sxs.end_sys_xct (result)); //commit or abort, depending on the result code
  // if we exit this function without calling end_sys_xct(), the system transaction
  // automatically aborts.
  return result;
}\endverbatim
 */
class sys_xct_section_t {
public:
    /**
     * starts a nested system transaction.
     */
    sys_xct_section_t();
    /** This destructor makes sure the system transaction ended. */
    ~sys_xct_section_t();

    /** Commits or aborts the system transaction, depending on the given result code.*/
    rc_t end_sys_xct (rc_t result);

    int _depth;
};

/** Used to tentatively set t_cc_none to _query_concurrency. */
class no_lock_section_t {
public:
    no_lock_section_t () {
        xct_t *x = xct();
        if (x) {
            DBGOUT3( << "!!!! no_lock_section_t() - lock has been disabled");

            org_cc = x->get_query_concurrency();
            x->set_query_concurrency(smlevel_0::t_cc_none);
        } else {
            DBGOUT3( << "!!!! no_lock_section_t() - set original lock mode to t_cc_none");

            org_cc = smlevel_0::t_cc_none;
        }
    }
    ~no_lock_section_t () {
        xct_t *x = xct();
        if (x) {
            DBGOUT3( << "!!!! ~no_lock_section_t() - restored original lock mode: " << org_cc);
            x->set_query_concurrency(org_cc);
        }
    }
private:
    smlevel_0::concurrency_t org_cc;
};

// microseconds to "give up" watermark waits and flushes its own log in readonly xct
const int ELR_READONLY_WAIT_MAX_COUNT = 10;
const int ELR_READONLY_WAIT_USEC = 2000;


/*<std-footer incl-file-exclusion='XCT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
