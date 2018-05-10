/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT

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

/** @file:   shore_helper_loader.h
 *
 *  @brief:  Definition of helper loader thread classes
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __SHORE_HELPER_LOADER_H
#define __SHORE_HELPER_LOADER_H

#include "sm_vas.h"

#include "table_man.h"
#include "shore_env.h"
#include "kits_thread.h"

class ShoreEnv;


/******************************************************************
 *
 *  @class: db_init_smt_t
 *
 *  @brief: An smthread inherited class that it is used for initiating
 *          the Shore environment
 *
 ******************************************************************/

class db_init_smt_t : public thread_t
{
private:
    ShoreEnv* _env;
    int       _rv;

public:

    db_init_smt_t(std::string tname, ShoreEnv* db);
    ~db_init_smt_t();
    void work();
    int rv();

}; // EOF: db_init_smt_t


/******************************************************************
 *
 *  @class: checkpointer_t
 *
 *  @brief: An smthread inherited class that it is used for taking
 *          periodic checkpoints during loading and measurements.
 *          It is currently also used to activate the archiver and
 *          merger daemons. (TODO) In the future, we should use a
 *          generic "timer" service to control all system daemons:
 *          - Checkpointing
 *          - Page cleaner (or disk-to-disk propagation)
 *          - Log archiver
 *          - Archive merger
 *          - Backup
 *          - Space reclamation, etc.
 *
 ******************************************************************/


class checkpointer_t : public thread_t
{
private:
    ShoreEnv* _env;
    bool _active;
public:
    checkpointer_t(ShoreEnv* env)
        : thread_t("checkpointer"), _env(env), _active(true)
    { }

    void set_active(bool active) {
        _active = active;
    }

    void work();
};


/******************************************************************
 *
 *  @class: crasher_t
 *
 *  @brief: An smthread inherited class that it is used for simulating
 *          a crash for recovery testing purposes. The thread simply
 *          calls abort after a certain number of seconds.
 *
 ******************************************************************/


class crasher_t : public thread_t
{
private:
    int _timeout;
public:
    crasher_t(int t)
        : thread_t("crasher"), _timeout(t)
    { }

    void work();
};

/******************************************************************
 *
 *  @class: table_loading_smt_t
 *
 *  @brief: An smthread inherited class that it is used for spawning
 *          multiple table loading threads.
 *
 ******************************************************************/

class table_loading_smt_t : public thread_t
{
protected:

    ss_m*         _pssm;
    table_desc_t* _ptable;
    const int     _sf;
    const char*   _datadir;
    int           _rv;

public:

    table_loading_smt_t(std::string tname, ss_m* assm,
                        table_desc_t* atable,
                        const int asf, const char* adatadir)
	: thread_t(tname), _pssm(assm), _ptable(atable),
          _sf(asf), _datadir(adatadir)
    {
        assert (_pssm);
        assert (_ptable);
        assert (_sf);
        assert (_datadir);
    }

    virtual ~table_loading_smt_t() { }

    // thread entrance
    virtual void work()=0;
    inline int rv() { return (_rv); }
    inline table_desc_t* table() { return (_ptable); }

}; // EOF: table_loading_smt_t


/******************************************************************
 *
 *  @class: index_loading_smt_t
 *
 *  @brief: An smthread inherited class that it is used for helping
 *          the index loading.
 *
 *  @note:  Thread for helping the index loading. In order to do the
 *          index loading we need to open an iterator on the main table.
 *          Unfortunately, we cannot commit while loading, cause the
 *          iterator closes automatically.
 *
 ******************************************************************/

#if 0 // CS TODO

template <class TableDesc>
class index_loading_smt_t : public thread_t
{
    typedef table_row_t table_tuple;
    typedef table_man_t<TableDesc> table_manager;

private:

    ss_m*          _pssm;
    table_manager* _pmanager;
    index_desc_t*  _pindex;
    int            _t_count;
    int            _rv;


public:

    table_tuple* _ptuple;
    mcs_lock     _cs_mutex; /* (?) */

    bool         _has_to_consume;
    bool         _start;
    bool         _finish;


    index_loading_smt_t(std::string tname, ss_m* assm, table_manager* aptable_manager,
                        index_desc_t* apindex, table_tuple* aptuple)
	: thread_t(tname), _pssm(assm), _pmanager(aptable_manager),
          _pindex(apindex), _t_count(0), _ptuple(aptuple),
          _has_to_consume(false), _start(false), _finish(false)
    {
        assert (_pssm);
        assert (_pmanager);
        assert (_pindex);
        assert (_ptuple);
    }

    ~index_loading_smt_t()
    {
    }

    inline int rv() { return (_rv); }

    w_rc_t do_help()
    {
        assert (_pmanager);
        assert (_pindex);

        char* pdest  = NULL;
        int   bufsz  = 0;
        int   key_sz = 0;
        int   mark   = COMMIT_ACTION_COUNT;
        bool  cons_happened = false;
        int   ispin  = 0;

        {
            spinlock_write_critical_section cs(&_cs_mutex);
            // CS: used to be CRITICAL_SECTION followed by pause()
            // Since I don't understant the code completely, I just
            // use the equivalent Zero idiom. Note that pause() and
            // release() do nothing other than release and aquire,
            // respectively
        }

        while(!_start) {
            ispin++;
        }

        W_DO(_pssm->begin_xct());

        while (true) {

            {
                spinlock_write_critical_section cs(&_cs_mutex);

                if (_has_to_consume) {
                    // if new row waiting

                    // if signalled to finish
                    if (_finish)
                        break;

                    //*** CONSUME ***//

                    key_sz = _pmanager->format_key(_pindex, _ptuple, *_ptuple->_rep);
                    assert (pdest); // if NULL invalid key

                    W_DO(_pssm->create_assoc(_pindex->stid(),
                                vec_t(pdest, key_sz),
                                vec_t(&(_ptuple->_rid), sizeof(rid_t))));

                    _has_to_consume = false;
                    cons_happened = true; // a consumption just happened
                }

            }
            //*** EOF: CS ***//

            if (cons_happened) {
                // It just consumed a row, increase the counters
                _t_count++;

                if (_t_count >= mark) {
                    W_DO(_pssm->commit_xct());

                    if ((_t_count % 100000) == 0) { // every 100K
                        TRACE( TRACE_ALWAYS, "index(%s): %d\n",
                               _pindex->name(), _t_count);
                    }
                    else {
                        TRACE( TRACE_TRX_FLOW, "index(%s): %d\n",
                               _pindex->name(), _t_count);
                    }

                    W_DO(_pssm->begin_xct());
                    mark += COMMIT_ACTION_COUNT;
                }
                cons_happened = false;
            }
        }
        // final commit
        W_DO(_pssm->commit_xct());

        // if we reached this point everything went ok
        return (RCOK);
    }


    // thread entrance
    void work() {
        w_rc_t e = do_help();
        if (e.is_error()) {
            TRACE( TRACE_ALWAYS, "Index (%s) loading aborted [0x%x]\n",
                   _pindex->name(), e.err_num());

            int iretries = 0;
            w_rc_t abrt_rc = _pssm->abort_xct();

            while (!abrt_rc.is_error()) {
                iretries++;
                abrt_rc = _pssm->abort_xct();
                if (iretries > SHORE_NUM_OF_RETRIES)
                    break;
            }

            _rv = 1;
            return;
        }

        // the do_help() function exits _finish should be set to true
        assert (_finish);

        // if reached this point everything was ok
        _rv = 0;
    }

    int    count() { return (_t_count); }

}; // EOF: index_loading_smt_t



/******************************************************************
 *
 *  @class table_checking_smt_t
 *
 *  @brief An smthread inherited class that it is used for spawning
 *         multiple table checking consistency threads.
 *
 ******************************************************************/

class table_checking_smt_t : public thread_t
{
protected:

    ss_m*         _pssm;
    table_desc_t* _ptable;

public:

    table_checking_smt_t(std::string tname, ss_m* pssm,
                        table_desc_t* atable)
	: thread_t(tname), _pssm(pssm), _ptable(atable)
    {
        assert (_pssm);
        assert (_ptable);
    }

    virtual ~table_checking_smt_t() { }

    // thread entrance
    virtual void work()=0;

}; // EOF: table_checking_smt_t


template <class TableDesc>
class table_checking_smt_impl : public table_checking_smt_t
{
private:
    table_man_t<TableDesc>* _pmanager;

public:

    table_checking_smt_impl(std::string tname, ss_m* pssm,
                            table_man_t<TableDesc>* amanager,
                            TableDesc* atable)
	: table_checking_smt_t(tname, pssm, atable), _pmanager(amanager)
    {
        assert (_pmanager);
    }

    ~table_checking_smt_impl() { }

    // thread entrance
    void work() {
        TRACE( TRACE_DEBUG, "Checking (%s)\n", _ptable->name());

        //if (!_pmanager->check_all_indexes(_pssm)) {
        // w_rc_t e = _pmanager->check_all_indexes_together(_pssm);
        // if (e.is_error()) {
        //     TRACE( TRACE_DEBUG, "Inconsistency in (%s)\n", _ptable->name());
        // }
        // else {
        //     TRACE( TRACE_DEBUG, "(%s) OK...\n", _ptable->name());
        // }
    }

}; // EOF: table_checking_smt_impl



/******************************************************************
 *
 *  @class close_smt_t
 *
 *  @brief An smthread inherited class that it is used just for
 *         closing the database.
 *
 ******************************************************************/

class close_smt_t : public thread_t {
private:
    ShoreEnv* _env;

public:
    int	_rv;

    close_smt_t(ShoreEnv* env, std::string tname)
	: thread_t(tname),
          _env(env), _rv(0)
    {
    }

    ~close_smt_t() {
    }

    void work();

    /** @note Those two functions should be implemented by every
     *        smthread-inherited class that runs using run_smthread()
     */
    inline int retval() { return (_rv); }

}; // EOF: close_smt_t



/******************************************************************
 *
 *  @class dump_smt_t
 *
 *  @brief An smthread inherited class that it is used just for
 *         dumping the database.
 *
 ******************************************************************/

class dump_smt_t : public thread_t
{
private:
    ShoreEnv* _env;

public:
    int	_rv;

    dump_smt_t(ShoreEnv* env, std::string tname)
	: thread_t(tname),
          _env(env), _rv(0)
    {
    }

    ~dump_smt_t() {
    }

    void work();

    /** @note Those two functions should be implemented by every
     *        smthread-inherited class that runs using run_smthread()
     */
    inline int retval() { return (_rv); }

}; // EOF: dump_smt_t

#endif


/******************************************************************
 *
 *  @class: abort_smt_t
 *
 *  @brief: An smthread inherited class that it is used just for
 *          aborting a list of transactions
 *
 ******************************************************************/

class abort_smt_t : public thread_t
{
private:
    ShoreEnv* _env;

public:

    vector<xct_t*>* _toabort;
    uint _aborted;

    abort_smt_t(std::string tname, ShoreEnv* env, vector<xct_t*>& toabort);
    ~abort_smt_t();
    void work();

}; // EOF: abort_smt_t

#endif /* __SHORE_HELPER_LOADER_H */

