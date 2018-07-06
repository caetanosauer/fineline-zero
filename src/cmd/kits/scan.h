#ifndef KITS_SCAN_H
#define KITS_SCAN_H

#include "table_man.h"
#include "leveldb_scan.h"

#ifdef USE_LEVELDB
typedef leveldb_cursor_t cursor_t;
#else
typedef bt_cursor_t cursor_t;
#endif

class base_scan_t
{
protected:
    index_desc_t* _pindex;
    cursor_t* cursor;
    Database* _db;

public:
    base_scan_t(index_desc_t* pindex)
        : _pindex(pindex), cursor(nullptr)
    {
        w_assert1(_pindex);
        _db = _pindex->table()->db();
        w_assert1(_db);
    }

    virtual ~base_scan_t() {
        if (cursor) delete cursor;
    };

    w_rc_t open_scan(bool forward = true) {
        if (!cursor) {
#ifdef USE_LEVELDB
            cursor = new cursor_t(_db, _pindex->stid(), forward);
#else
            cursor = new cursor_t(_pindex->stid(), forward);
#endif
        }

        return (RCOK);
    }

    w_rc_t open_scan(char* bound, int bsz, bool incl, bool forward = true)
    {
        if (!cursor) {
#ifdef USE_LEVELDB
            cursor = new cursor_t(_db, _pindex->stid(), bound, bsz, incl, forward);
#else
            w_keystr_t kstr;
            kstr.construct_regularkey(bound, bsz);
            cursor = new cursor_t(_pindex->stid(), kstr, incl, forward);
#endif
        }

        return (RCOK);
    }

    w_rc_t open_scan(char* lower, int lowsz, bool lower_incl,
                     char* upper, int upsz, bool upper_incl,
                     bool forward = true)
    {
        if (!cursor) {
#ifdef USE_LEVELDB
            cursor = new cursor_t(_db, _pindex->stid(), lower, lowsz, lower_incl,
                  upper, upsz, upper_incl, forward);
#else
            w_keystr_t kup, klow;
            kup.construct_regularkey(upper, upsz);
            klow.construct_regularkey(lower, lowsz);
            cursor = new bt_cursor_t(_pindex->stid(), klow, lower_incl, kup, upper_incl, forward);
#endif
        }

        return (RCOK);
    }

    virtual w_rc_t next(bool& eof, table_row_t& tuple) = 0;

};

template <class T>
class table_scan_iter_impl : public base_scan_t
{
public:

    table_scan_iter_impl(table_man_t<T>* pmanager)
        : base_scan_t(pmanager->table()->primary_idx())
    {}

    virtual ~table_scan_iter_impl() {}

    virtual w_rc_t next(bool& eof, table_row_t& tuple)
    {
        if (!cursor) open_scan();

        W_DO(cursor->next());
        eof = cursor->eof();
        if (eof) { return RCOK; }

        // Load key
        cursor->key().serialize_as_nonkeystr(tuple._rep_key->_dest);
        tuple.load_key(tuple._rep_key->_dest, _pindex);

        // Load element
        char* elem = cursor->elem();
        tuple.load_value(elem, _pindex);

        return (RCOK);
    }

};

template <class T>
class index_scan_iter_impl : public base_scan_t
{
private:
    index_desc_t* _primary_idx;
    bool          _need_tuple;

public:
    index_scan_iter_impl(index_desc_t* pindex,
                         table_man_t<T>* pmanager,
                         bool need_tuple = false)
          : base_scan_t(pindex), _need_tuple(need_tuple)
    {
        assert (_pindex);
        assert (pmanager);
        _primary_idx = pmanager->table()->primary_idx();
    }

    virtual ~index_scan_iter_impl() { };

    virtual w_rc_t next(bool& eof, table_row_t& tuple)
    {
        if (!cursor) open_scan();
        assert (cursor);

        W_DO(cursor->next());
        eof = cursor->eof();
        if (eof) { return RCOK; }

        bool loaded = false;

        if (!_need_tuple) {
            // Load only fields of secondary key (index key)
            cursor->key().serialize_as_nonkeystr(tuple._rep_key->_dest);
            tuple.load_key(tuple._rep_key->_dest, _pindex);
        }
        else {
            // Fetch complete tuple from primary index
            char* pkey = cursor->elem();
            smsize_t elen = cursor->elen();

            // load primary key fields
            tuple.load_key(pkey, _primary_idx);

            // fetch and load other fields
            w_keystr_t pkeystr;
            pkeystr.construct_regularkey(pkey, elen);
#ifdef USE_LEVELDB
            loaded = LevelDBInterface::levelDBProbe(_primary_idx->table()->db(), pkeystr, tuple._rep->_dest);
#else
            ss_m::find_assoc(_primary_idx->stid(), pkeystr, tuple._rep->_dest, elen, loaded);
#endif
            w_assert1(loaded);

            tuple.load_value(tuple._rep->_dest, _primary_idx);
        }
        return (RCOK);
    }
};

#endif
