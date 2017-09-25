/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define XCT_C

#include <new>
#include "sm_base.h"

#include "tls.h"

#include "lock.h"
#include <sm_base.h>
#include "xct.h"
#include "lock_x.h"
#include "lock_lil.h"

#include <sm.h>
#include "tls.h"
#include <sstream>
#include "logrec.h"
#include "bf_tree.h"
#include "lock_raw.h"
#include "log_lsn_tracker.h"
#include "log_core.h"
#include "xct_logger.h"

#include "allocator.h"

// Template specializations for sm_tls_allocator
DECLARE_TLS(block_pool<xct_t>, xct_pool);
DECLARE_TLS(block_pool<xct_t::xct_core>, xct_core_pool);

template<>
xct_t* sm_tls_allocator::allocate<xct_t>(size_t)
{
    return (xct_t*) xct_pool->acquire();
}

template<>
void sm_tls_allocator::release(xct_t* p, size_t)
{
    xct_pool->release(p);
}

template<>
xct_t::xct_core* sm_tls_allocator::allocate<xct_t::xct_core>(size_t)
{
    return (xct_t::xct_core*) xct_core_pool->acquire();
}

template<>
void sm_tls_allocator::release(xct_t::xct_core* p, size_t)
{
    xct_core_pool->release(p);
}

#define DBGX(arg) DBG(<< "tid." << _tid  arg)

// If we run into btree shrinking activity, we'll bump up the
// fudge factor, b/c to undo a lot of btree removes (incremental
// tree removes) takes about 4X the logging...
extern double logfudge_factors[t_max_logrec]; // in logstub.cpp
#define UNDO_FUDGE_FACTOR(t, nbytes) int((logfudge_factors[t])*(nbytes))

#ifdef W_TRACE
extern "C" void debugflags(const char *);
void
debugflags(const char *a)
{
   _w_debug.setflags(a);
}
#endif /* W_TRACE */

/*********************************************************************
 *
 *  The xct list is sorted for easy access to the oldest and
 *  youngest transaction. All instantiated xct_t objects are
 *  in the list.
 *
 *  Here are the transaction list and the mutex that protects it.
 *
 *********************************************************************/

queue_based_lock_t        xct_t::_xlist_mutex;

w_descend_list_t<xct_t, queue_based_lock_t, tid_t>
        xct_t::_xlist(W_KEYED_ARG(xct_t, _tid,_xlink), &_xlist_mutex);

bool xct_t::xlist_mutex_is_mine()
{
     bool is =
        smthread_t::get_xlist_mutex_node()._held
        &&
        (smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node()));
     return is;
}
void xct_t::assert_xlist_mutex_not_mine()
{
    w_assert1(
            (smthread_t::get_xlist_mutex_node()._held == 0)
           ||
           (smthread_t::get_xlist_mutex_node()._held->
               is_mine(&smthread_t::get_xlist_mutex_node())==false));
}
void xct_t::assert_xlist_mutex_is_mine()
{
#if W_DEBUG_LEVEL > 1
    bool res =
     smthread_t::get_xlist_mutex_node()._held
        && (smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node()));
    if(!res) {
        fprintf(stderr, "held: %p\n",
             smthread_t::get_xlist_mutex_node()._held );
        if ( smthread_t::get_xlist_mutex_node()._held  )
        {
        fprintf(stderr, "ismine: %d\n",
            smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node()));
        }
        w_assert1(0);
    }
#else
     w_assert1(smthread_t::get_xlist_mutex_node()._held
        && (smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node())));
#endif
}

w_rc_t  xct_t::acquire_xlist_mutex()
{
     assert_xlist_mutex_not_mine();
     _xlist_mutex.acquire(&smthread_t::get_xlist_mutex_node());
     assert_xlist_mutex_is_mine();
     return RCOK;
}

void  xct_t::release_xlist_mutex()
{
     assert_xlist_mutex_is_mine();
     _xlist_mutex.release(smthread_t::get_xlist_mutex_node());
     assert_xlist_mutex_not_mine();
}

/*********************************************************************
 *
 *  _nxt_tid is used to generate unique transaction id
 *
 *********************************************************************/
std::atomic<tid_t> xct_t::_nxt_tid {0};

/*********************************************************************
 *
 *  _oldest_tid is the oldest currently-running tx (well, could be
 *  committed by now - the xct destructor updates this)
 *  This corresponds to the Shore-MT paper section 7.3, top of
 *  2nd column, page 10.
 *
 *********************************************************************/
tid_t                                xct_t::_oldest_tid = 0;

struct lock_info_ptr {
    xct_lock_info_t* _ptr;

    lock_info_ptr() : _ptr(0) { }

    xct_lock_info_t* take() {
        if(xct_lock_info_t* rval = _ptr) {
            _ptr = 0;
            return rval;
        }
        return new xct_lock_info_t;
    }
    void put(xct_lock_info_t* ptr) {
        if(_ptr)
            delete _ptr;
        _ptr = ptr? ptr->reset_for_reuse() : 0;
    }

    ~lock_info_ptr() { put(0); }
};

DECLARE_TLS(lock_info_ptr, agent_lock_info);

struct lil_lock_info_ptr {
    lil_private_table* _ptr;

    lil_lock_info_ptr() : _ptr(0) { }

    lil_private_table* take() {
        if(lil_private_table* rval = _ptr) {
            _ptr = 0;
            return rval;
        }
        return new lil_private_table;
    }
    void put(lil_private_table* ptr) {
        if(_ptr)
            delete _ptr;
        if (ptr) {
            ptr->clear();
        }
        _ptr = ptr;
    }

    ~lil_lock_info_ptr() { put(0); }
};

DECLARE_TLS(lil_lock_info_ptr, agent_lil_lock_info);

// Define customized new and delete operators for sm allocation
DEFINE_SM_ALLOC(xct_t);
DEFINE_SM_ALLOC(xct_t::xct_core);

xct_t::xct_core::xct_core(tid_t const &t, state_t s, int timeout)
    :
    _tid(t),
    _timeout(timeout),
    _lock_info(agent_lock_info->take()),
    _lil_lock_info(agent_lil_lock_info->take()),
    _raw_lock_xct(NULL),
    _state(s)
{
    _lock_info->set_tid(_tid);
    w_assert1(_tid == _lock_info->tid());

    w_assert1(_lil_lock_info);
    if (smlevel_0::lm) {
        _raw_lock_xct = smlevel_0::lm->allocate_xct();
    }

    INC_TSTAT(begin_xct_cnt);

}

/*********************************************************************
 *
 *  xct_t::xct_t(that, type)
 *
 *  Begin a transaction. The transaction id is assigned automatically,
 *  and the xct record is inserted into _xlist.
 *
 *********************************************************************/
xct_t::xct_t(sm_stats_t* stats, int timeout,
           const tid_t& given_tid, const lsn_t& last_lsn,
           bool loser_xct
            )
    :
    _core(new xct_core(
                given_tid == 0 ? ++_nxt_tid : given_tid,
                xct_active, timeout)),
    __stats(stats),
    __saved_lockid_t(0),
    _tid(_core->_tid),
    _ssx_chain_len(0),
    _query_concurrency (smlevel_0::t_cc_none),
    _query_exlock_for_select(false),
    _inquery_verify(false),
    _inquery_verify_keyorder(false),
    _inquery_verify_space(false),
    _read_watermark(lsn_t::null),
    _elr_mode (elr_none)
#if W_DEBUG_LEVEL > 2
    ,
    _had_error(false)
#endif
{
    w_assert3(state() == xct_active);
    if (given_tid != 0 && _nxt_tid < given_tid) {
        // CS: this should only happen during restart log analysis, which
        // creates one transaction at a time (i.e., no concurrency)
        _nxt_tid = given_tid;
    }

    w_assert1(tid() == _core->_tid);
    w_assert3(tid() <= _nxt_tid);
    w_assert2(tid() <= _nxt_tid);
    w_assert1(tid() == _core->_lock_info->tid());

    _ssx_positions[0] = 0;

    if (timeout_c() == timeout_t::WAIT_SPECIFIED_BY_THREAD) {
        // override in this case
        set_timeout(smthread_t::lock_timeout());
    }
    w_assert9(timeout_c() >= 0 || timeout_c() == timeout_t::WAIT_FOREVER);

    // CS: acquires xlist mutex and adds thix xct to the list
    // CS FINELINE TODO: no need to keep a list of txns with no-steal
    put_in_order();

    w_assert3(state() == xct_active);

    if (given_tid == 0) {
        smthread_t::attach_xct(this);
    }
    else {
        w_assert1(smthread_t::xct() == 0);
    }

    w_assert3(state() == xct_active);
    _begin_tstamp = std::chrono::high_resolution_clock::now();
}


xct_t::xct_core::~xct_core()
{
    w_assert3(_state == xct_ended);
    if(_lock_info) {
        agent_lock_info->put(_lock_info);
    }
    if (_lil_lock_info) {
        agent_lil_lock_info->put(_lil_lock_info);
    }
    if (_raw_lock_xct) {
        smlevel_0::lm->deallocate_xct(_raw_lock_xct);
    }
}
/*********************************************************************
 *
 *  xct_t::~xct_t()
 *
 *  Clean up and free up memory used by the transaction. The
 *  transaction has normally ended (committed or aborted)
 *  when this routine is called.
 *
 *********************************************************************/
xct_t::~xct_t()
{
    w_assert9(__stats == 0);

    if (!is_sys_xct() && smlevel_0::log) {
        smlevel_0::log->get_oldest_lsn_tracker()->leave(
                reinterpret_cast<uintptr_t>(this));
    }

    _teardown();

    if (shutdown_clean)  {
        // if this transaction is system transaction,
        // the thread might be still conveying another thread
        w_assert1(is_sys_xct() || smthread_t::xct() == 0);
    }

    // clean up what's stored in the thread
    smthread_t::no_xct(this);

    if(__saved_lockid_t)  {
        delete[] __saved_lockid_t;
        __saved_lockid_t=0;
    }

        if(_core)
            delete _core;
        _core = NULL;
    // if (LATCH_NL != latch().mode())
    // {
    //     // Someone is accessing this txn, wait until it finished
    //     w_rc_t latch_rc = latch().latch_acquire(LATCH_EX, timeout_t::WAIT_FOREVER);

    //     // Now we can delete the core, no one can acquire latch on this txn after this point
    //     // since transaction is being destroyed

    //     if (false == latch_rc.is_error())
    //     {
    //         // CS TODO if _core is nullified above, latch() causes segfault!
    //         if (latch().held_by_me())
    //             latch().latch_release();
    //     }
    // }

        w_assert1(state() == xct_ended);
}

/*
 * Clean up existing transactions at ssm shutdown.
 * -- called from ~ss_m, so this should never be
 * subject to multiple threads using the xct list.
 */
void xct_t::cleanup(bool allow_abort)
{
    bool        changed_list;
    xct_t*      xd;
    W_COERCE(acquire_xlist_mutex());
    do {
        /*
         *  We cannot delete an xct while iterating. Use a loop
         *  to iterate and delete one xct for each iteration.
         */
        xct_i i(false); // do acquire the list mutex. Noone
        // else should be iterating over the xcts at this point.
        changed_list = false;
        xd = i.next();
        if (xd) {
            // Release the mutex so we can delete the xd if need be...
            release_xlist_mutex();
            switch(xd->state()) {
            case xct_active: {
                    smthread_t::attach_xct(xd);
                    if (allow_abort) {
                        W_COERCE( xd->abort() );
                    } else {
                        W_COERCE( xd->dispose() );
                    }
                    delete xd;
                    changed_list = true;
                }
                break;

            case xct_freeing_space:
            case xct_ended: {
                    DBG(<< xd->tid() <<"deleting "
                            << " w/ state=" << xd->state() );
                    delete xd;
                    changed_list = true;
                }
                break;

            default: {
                    DBG(<< xd->tid() <<"skipping "
                            << " w/ state=" << xd->state() );
                }
                break;

            } // switch on xct state
            W_COERCE(acquire_xlist_mutex());
        } // xd not null
    } while (xd && changed_list);

    release_xlist_mutex();
}

void xct_t::push_ssx()
{
    _ssx_chain_len++;
    _ssx_positions[_ssx_chain_len] = smthread_t::get_redo_buf()->get_size();
}

void xct_t::pop_ssx()
{
    _ssx_chain_len--;
}

/*********************************************************************
 *
 *  xct_t::num_active_xcts()
 *
 *  Return the number of active transactions (equivalent to the
 *  size of _xlist.
 *
 *********************************************************************/
uint32_t
xct_t::num_active_xcts()
{
    uint32_t num;
    W_COERCE(acquire_xlist_mutex());
    num = _xlist.num_members();
    release_xlist_mutex();
    return  num;
}



/*********************************************************************
 *
 *  xct_t::look_up(tid)
 *
 *  Find the record for tid and return it. If not found, return 0.
 *
 *********************************************************************/
xct_t*
xct_t::look_up(const tid_t& tid)
{
    xct_t* xd;
    xct_i iter(true);

    while ((xd = iter.next())) {
        if (xd->tid() == tid) {
            return xd;
        }
    }
    return 0;
}

xct_lock_info_t*
xct_t::lock_info() const {
    return _core->_lock_info;
}

lil_private_table* xct_t::lil_lock_info() const
{
    return _core->_lil_lock_info;
}
RawXct* xct_t::raw_lock_xct() const {
    return _core->_raw_lock_xct;
}

int
xct_t::timeout_c() const {
    return _core->_timeout;
}

/*********************************************************************
 *
 *  xct_t::oldest_tid()
 *
 *  Return the tid of the oldest active xct.
 *
 *********************************************************************/
tid_t
xct_t::oldest_tid()
{
    return _oldest_tid;
}


rc_t
xct_t::abort(bool save_stats_structure /* = false */)
{
    if(is_instrumented() && !save_stats_structure) {
        delete __stats;
        __stats = 0;
    }
    return _abort();
}

rc_t
xct_t::commit(bool lazy,lsn_t* plastlsn)
{
    // removed because a checkpoint could
    // be going on right now.... see comments
    // in log_prepared and chkpt.cpp

    return _commit(t_normal | (lazy ? t_lazy : t_normal), plastlsn);
}

tid_t
xct_t::youngest_tid()
{
    ASSERT_FITS_IN_LONGLONG(tid_t);
    return _nxt_tid;
}

void
xct_t::update_youngest_tid(const tid_t &t)
{
    if (_nxt_tid < t) _nxt_tid = t;
}


void
xct_t::put_in_order() {
    W_COERCE(acquire_xlist_mutex());
    _xlist.put_in_order(this);
    _oldest_tid = _xlist.last()->_tid;
    release_xlist_mutex();

// TODO(Restart)... enable the checking in retail build, also generate error in retail
//                           this is to prevent missing something in retail and weird error
//                           shows up in retail build much later

// #if W_DEBUG_LEVEL > 2
    W_COERCE(acquire_xlist_mutex());
    {
        // make sure that _xlist is in order
        w_list_i<xct_t, queue_based_lock_t> i(_xlist);
        tid_t t = 0;
        xct_t* xd;
        while ((xd = i.next()))  {
            if (t >= xd->_tid)
                ERROUT(<<"put_in_order: failed to satisfy t < xd->_tid, t: " << t << ", xd->tid: " << xd->_tid);
            w_assert1(t < xd->_tid);
        }
        if (t > _nxt_tid)
            ERROUT(<<"put_in_order: failed to satisfy t <= _nxt_tid, t: " << t << ", _nxt_tid: " << _nxt_tid);
        w_assert1(t <= _nxt_tid);
    }
    release_xlist_mutex();
// #endif
}

void
xct_t::dump(ostream &out)
{
    W_COERCE(acquire_xlist_mutex());
    out << "xct_t: "
            << _xlist.num_members() << " transactions"
        << endl;
    w_list_i<xct_t, queue_based_lock_t> i(_xlist);
    xct_t* xd;
    while ((xd = i.next()))  {
        out << "********************" << "\n";
        out << *xd << endl;
    }
    release_xlist_mutex();
}

void
xct_t::set_timeout(int t)
{
    _core->_timeout = t;
}



/*********************************************************************
 *
 *  Print out tid and status
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, const xct_t& x)
{
    o << "tid="<< x.tid();
    o << "\n" << " state=" << x.state() << "\n" << "   ";

    if(x.raw_lock_xct()) {
         x.raw_lock_xct()->dump_lockinfo(o);
    }

    return o;
}

// common code needed by _commit(t_chain) and ~xct_t()
void
xct_t::_teardown() {
    W_COERCE(acquire_xlist_mutex());

    _xlink.detach();

    // find the new oldest xct
    xct_t* xd = _xlist.last();
    _oldest_tid = xd ? xd->_tid : _nxt_tid.load();
    release_xlist_mutex();
}

/*********************************************************************
 *
 *  xct_t::change_state(new_state)
 *
 *  Change the status of the transaction to new_state.
 *
 *********************************************************************/
void
xct_t::change_state(state_t new_state)
{
    // Acquire a write latch, the traditional read latch is used by checkpoint
    w_rc_t latch_rc = latch().latch_acquire(LATCH_EX, timeout_t::WAIT_FOREVER);
    if (latch_rc.is_error())
    {
        // Unable to the read acquire latch, cannot continue, raise an internal error
        DBGOUT2 (<< "Unable to acquire LATCH_EX for transaction object. tid = "
                 << tid() << ", rc = " << latch_rc);
        W_FATAL_MSG(fcINTERNAL, << "unable to write latch a transaction object to change state");
        return;
    }

    w_assert2(_core->_state != new_state);
    w_assert2((new_state > _core->_state) ||
            (_core->_state == xct_chaining && new_state == xct_active));

    _core->_state = new_state;

    // Release the write latch
    latch().latch_release();

}


bool xct_t::has_logs()
{
    auto begin = _ssx_positions[_ssx_chain_len];
    auto end = smthread_t::get_redo_buf()->get_size();
    return end > begin;
}

void xct_t::flush_redo_buffer(bool sys_xct)
{
    // CS FINELINE TODO: simplify this, by unifying all tcb, redobuf, xct,
    // and ss_m code
    if (!sys_xct) { Logger::log<xct_end_log>(); }
    // FINELINE log insert of whole redo buffer
    auto redobuf = smthread_t::get_redo_buf();
    auto offset = _ssx_positions[_ssx_chain_len];
    char* begin = redobuf->get_buffer_begin() + offset;
    auto commit_size = redobuf->get_size() - offset;
    w_assert1(commit_size > 0);
    smlevel_0::log->insert_raw(begin, commit_size);
    redobuf->drop_suffix(commit_size);
}

/*********************************************************************
 *
 *  xct_t::commit(flags)
 *
 *  Commit the transaction. If flag t_lazy, log is not synced.
 *  If flag t_chain, a new transaction is instantiated inside
 *  this one, and inherits all its locks.
 *
 *  In *plastlsn it returns the lsn of the last log record for this
 *  xct.
 *
 *********************************************************************/
rc_t
xct_t::_commit(uint32_t flags, lsn_t* plastlsn /* default NULL*/)
{
    // when chaining, we inherit the read_watermark from the previous xct
    // in case the next transaction are read-only.
    lsn_t inherited_read_watermark;

    // Static thread-local variables used to measure transaction latency
    static thread_local unsigned long _accum_latency = 0;
    static thread_local unsigned int _latency_count = 0;

    bool sys_xct = is_sys_xct();
    if (!sys_xct) {
        w_assert1(_core->_state == xct_active);

        change_state(xct_committing);

        if (has_logs())  {
            // CS FINELINE TODO: early lock release won't work like this, because commit_lsn is
            // not known until log is actually flushed. I just replaced it with the current
            // durable_lsn, but I'm not entirely sure if that is correct.
            lsn_t commit_lsn = smlevel_0::log->durable_lsn();
            W_DO(early_lock_release(commit_lsn));
        }
    }

    if (has_logs())  {
        flush_redo_buffer(sys_xct);

        // If system txn, we're done here
        if (sys_xct) { return RCOK; }
        if(_elr_mode != elr_sx)  { W_DO(commit_free_locks()); }
    }
    else if (!sys_xct) {
        W_DO(_commit_read_only(flags, inherited_read_watermark));
    }

    if (sys_xct) { return RCOK; }

    INC_TSTAT(commit_xct_cnt);

    change_state(xct_ended);
    smthread_t::detach_xct(this);        // no transaction for this thread

    /*
     *  Xct is now committed
     */

    auto end_tstamp = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tstamp - _begin_tstamp);
    _accum_latency += elapsed.count();
    _latency_count++;
    // dump average latency every 100 commits
    if (_latency_count % 100 == 0) {
        Logger::log_sys<xct_latency_dump_log>(_accum_latency / _latency_count);
        _accum_latency = 0;
        _latency_count = 0;
    }

    return RCOK;
}

rc_t
xct_t::_commit_read_only(uint32_t flags, lsn_t& inherited_read_watermark)
{
    if(!is_sys_xct()) {
        W_DO(commit_free_locks());

        // however, to make sure the ELR for X-lock and CLV is
        // okay (ELR for S-lock is anyway okay) we need to make
        // sure this read-only xct (no-log=read-only) didn't read
        // anything not yet durable. Thus,
        if ((_elr_mode==elr_sx || _elr_mode==elr_clv) &&
                _query_concurrency != t_cc_none && _query_concurrency != t_cc_bad && _read_watermark.valid()) {
            // to avoid infinite sleep because of dirty pages changed by aborted xct,
            // we really output a log and flush it
            bool flushed = false;
            timeval start, now, result;
            ::gettimeofday(&start,NULL);
            while (true) {
                W_DO(log->flush(_read_watermark, false, true, &flushed));
                if (flushed) {
                    break;
                }

                // in some OS, usleep() has a very low accuracy.
                // So, we check the current time rather than assuming
                // elapsed time = ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC.
                ::gettimeofday(&now,NULL);
                timersub(&now, &start, &result);
                int elapsed = (result.tv_sec * 1000000 + result.tv_usec);
                if (elapsed > ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC) {
#if W_DEBUG_LEVEL>0
                    // this is NOT an error. it's fine.
                    cout << "ELR timeout happened in readonly xct's watermark check. outputting xct_end log..." << endl;
#endif // W_DEBUG_LEVEL>0
                    break; // timeout
                }
                ::usleep(ELR_READONLY_WAIT_USEC);
            }

            if (!flushed) {
                // now we suspect that we might saw a bogus tag for some reason.
                // so, let's output a real xct_end log and flush it.
                // See jira ticket:99 "ELR for X-lock" (originally trac ticket:101).
                // NOTE this should not be needed now that our algorithm is based
                // on lock bucket tag, which is always exact, not too conservative.
                // should consider removing this later, but for now keep it.
                //W_COERCE(log_xct_end());
                //_sync_logbuf();
                W_FATAL_MSG(fcINTERNAL,
                        << "Reached part of the code that was supposed to be dead."
                        << " Please uncomment the lines and remove this error");
            }
            _read_watermark = lsn_t::null;
        }
    } else {
        // even if chaining or grouped xct, we can do ELR
        // CS removed _last_lsn from xct_t
        // W_DO(early_lock_release());
        W_DO(early_lock_release(lsn_t::null));
    }

    return RCOK;
}

rc_t
xct_t::commit_free_locks(bool read_lock_only, lsn_t commit_lsn)
{
    // system transaction doesn't acquire locks
    if (!is_sys_xct()) {
        W_COERCE( lm->unlock_duration(read_lock_only, commit_lsn) );
    }
    return RCOK;
}

rc_t xct_t::early_lock_release(lsn_t last_lsn) {
    if (!is_sys_xct()) { // system transaction anyway doesn't have locks
        switch (_elr_mode) {
            case elr_none: break;
            case elr_s:
                // release only S and U locks
                W_DO(commit_free_locks(true));
                break;
            case elr_sx:
            case elr_clv: // TODO see below
                // simply release all locks
                // update tag for safe SX-ELR with _last_lsn which should be the commit lsn
                // (we should have called log_xct_end right before this)
                W_DO(commit_free_locks(false, last_lsn));
                break;
                // TODO Controlled Lock Violation is tentatively replaced with SX-ELR.
                // In RAW-style lock manager, reading the permitted LSN needs another barrier.
                // In reality (not concept but dirty impl), they are doing the same anyways.
                /*
            case elr_clv:
                // release no locks, but give permission to violate ours:
                lm->give_permission_to_violate(_last_lsn);
                break;
                */
            default:
                w_assert1(false); // wtf??
        }
    }
    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::abort()
 *
 *  Abort the transaction by calling rollback().
 *
 *********************************************************************/
rc_t
xct_t::_abort()
{
    w_assert1(_ssx_chain_len == 0);
    w_assert1(_core->_state == xct_active
            || _core->_state == xct_committing /* if it got an error in commit*/
            || _core->_state == xct_freeing_space /* if it got an error in commit*/
            );

    change_state(xct_aborting);
    W_DO(rollback());
    W_COERCE(commit_free_locks());
    change_state(xct_ended);

    smthread_t::detach_xct(this);        // no transaction for this thread
    INC_TSTAT(abort_xct_cnt);
    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::dispose()
 *
 *  Make the transaction disappear.
 *  This is only for simulating crashes.  It violates
 *  all tx semantics.
 *
 *********************************************************************/
rc_t
xct_t::dispose()
{
    delete __stats;
    __stats = 0;

    W_COERCE( commit_free_locks());
    // ClearAllStoresToFree();
    // ClearAllLoadStores();
    _core->_state = xct_ended; // unclean!
    smthread_t::detach_xct(this);
    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::_sync_logbuf()
 *
 *  Force log entries up to the most recently written to disk.
 *
 *  block: If not set it does not block, but kicks the flusher. The
 *         default is to block, the no block option is used by AsynchCommit
 * signal: Whether we even fire the log buffer
 *********************************************************************/
w_rc_t
xct_t::_sync_logbuf(bool block, bool signal)
{
    if(log) {
        INC_TSTAT(xct_log_flush);
        // CS TODO Fineline -- code above is non-durable
        // return log->flush(_last_lsn,block,signal);
        return log->flush(lsn_t::null,block,signal);
    }
    return RCOK;
}

// rc_t xct_t::get_logbuf(logrec_t*& ret)
// {
//     // then , use tentative log buffer.
//     // CS: system transactions should also go through log reservation,
//     // since they are consuming space which user transactions think
//     // is available for rollback. This is probably a bug.
//     if (is_piggy_backed_single_log_sys_xct()) {
//         ret = _log_buf_for_piggybacked_ssx;
//         return RCOK;
//     }


//     // Instead of flushing here, we'll flush at the end of give_logbuf()
//     // and assert here that we've got nothing buffered:
//     w_assert1(!_last_log);
//     ret = _last_log = _log_buf;

//     return RCOK;
// }


/*********************************************************************
 *
 *  xct_t::rollback(savept)
 *
 *  Rollback transaction up to "savept".
 *
 *********************************************************************/
rc_t
xct_t::rollback()
{
    auto undo_buf = smthread_t::get_undo_buf();
    if (!undo_buf->is_abortable()) {
        W_FATAL_MSG(eINTERNAL, <<
                "Transaction too large -- cannot rollback! " <<
                "Increase UndoBufferSize and recompile");
    }

    /*
     * CS FineLine txn rollback.
     */
    for (int i = undo_buf->get_count() - 1; i >= 0; i--) {
        char* data = undo_buf->get_data(i);
        StoreID stid = undo_buf->get_store_id(i);
        kind_t type = undo_buf->get_type(i);
        logrec_t::undo(type, stid, data);
    }

    _read_watermark = lsn_t::null;
    return RCOK;
}

ostream &
xct_t::dump_locks(ostream &out) const
{
    raw_lock_xct()->dump_lockinfo(out);
    return out;
}

sys_xct_section_t::sys_xct_section_t()
{
    W_IFDEBUG1(_depth = xct()->ssx_chain_len());
    W_COERCE(ss_m::begin_sys_xct());
}
sys_xct_section_t::~sys_xct_section_t()
{
}
rc_t sys_xct_section_t::end_sys_xct (rc_t result)
{
    if (result.is_error()) {
        W_COERCE (ss_m::abort_xct());
    } else {
        W_COERCE (ss_m::commit_sys_xct());
    }
    w_assert1(_depth == xct()->ssx_chain_len());
    return RCOK;
}
