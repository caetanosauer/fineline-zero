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

/** @file:   shore_row.h
 *
 *  @brief:  Base class for records (rows) of tables in Shore
 *
 *  @note:   table_row_t - row of a table
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */


/* shore_row.h contains the (abstract) base class (table_row_t) for the
 * tuple representation.
 *
 *
 * FUNCTIONALITY
 *
 * There are methods for formatting a tuple to its disk representation,
 * and loading it to memory, as well as, methods for accessing
 * the various fields of the tuple.
 *
 *
 * BUGS:
 *
 * Timestamp field is not fully implemented: no set function.
 *
 *
 * EXTENSIONS:
 *
 * The mapping between SQL types and C++ types are defined in
 * (field_desc_t).  Modify the class to support more SQL types or
 * change the mapping.  The NUMERIC type is currently stored as string;
 * no further understanding is provided yet.
 *
 */

/* The disk format of the record looks like this:
 *
 *  +--+-----+------------+-+-+-----+-------------+
 *  |NF| a   | d          | | | b   | c           |
 *  +--+-----+------------+-+-+-----+-------------+
 *                         | |      ^             ^
 *                         | +------+-------------+
 *                         |        |
 *                         +--------+
 *
 * The first part of tuple (NF) is dedicated to the null flags.  There
 * is a bit for each nullable field in null flags to tell whether the
 * data is presented. The space for the null flag is rounded to bytes.
 *
 * All the fixed size fields go first (a and d) and variable length
 * fields are appended after that. (b and c).  For the variable length
 * fields, we don't reserve the space for the full length of the value
 * but allocate as much space as needed.  Therefore, we need offsets
 * to tell the length of the actual values.  So here comes the two
 * additional slots in the middle (between d and b).  In our
 * implementation, we store the offset of the end of b relative to the
 * beginning of the tuple (address of a).
 *
 */

#ifndef __SHORE_ROW_H
#define __SHORE_ROW_H


//#include "k_defines.h"

#include "field.h"
#include "block_alloc.h"

class index_desc_t;


/* ---------------------------------------------------------------
 *
 * @struct: rep_row_t
 *
 * @brief:  A simple structure with a pointer to a buffer and its
 *          corresponding size.
 *
 * @note:   Not thread-safe, the caller should regulate access.
 *
 * --------------------------------------------------------------- */

typedef intptr_t offset_t;


/* ---------------------------------------------------------------
 *
 * @struct: rep_row_t
 *
 * @brief:  A scratchpad for writing the disk format of a tuple
 *
 * --------------------------------------------------------------- */

struct rep_row_t
{
    char* _dest;       /* pointer to a buffer */
    unsigned   _bufsz;     /* buffer size */
    blob_pool* _pts;  /* pointer to a trash stack */


    rep_row_t();
    rep_row_t(blob_pool* apts);
    ~rep_row_t();

    void set(const unsigned nsz);

    void set_ts(blob_pool* apts, const unsigned nsz);

}; // EOF: rep_row_t



/* ---------------------------------------------------------------
 *
 * @abstract struct: table_row_t
 *
 * @brief:  Abstract base class for the representation a row (record)
 *          of a table.
 *
 * --------------------------------------------------------------- */

class table_desc_t;

class table_row_t
{
public:
    table_desc_t*  _ptable;       /* pointer back to the table description */

    unsigned       _field_cnt;    /* number of fields */
    bool           _is_setup;     /* flag if already setup */

    field_value_t* _pvalues;      /* set of values */

    // pre-calculated offsets
    offset_t _fixed_offset;
    offset_t _var_slot_offset;
    offset_t _var_offset;
    unsigned     _null_count;

    // CS TODO -- get rid of these
    // But think about whether we should maintain the general principle,
    // which is to maintain a buffer for the serialized format of the tuple.
    // This could be useful to avoid repeated conversions, but it relies on
    // the caller to know whether the tuple changed since the last conversion
    // or not.
    rep_row_t*     _rep;          /* a pointer to a row representation struct */
    rep_row_t*     _rep_key;

    /*
     * CS: New methods for serialization and deserialization
     * (a.k.a. conversion between disk and memory format)
     */
    void load_key(char* data, index_desc_t* pindex = NULL);
    void load_value(char* data, index_desc_t* pindex = NULL);
    void store_key(char* data, size_t& length, index_desc_t* pindex = NULL);
    void store_value(char* data, size_t& length, index_desc_t* pindex = NULL);


    /* -------------------- */
    /* --- construction --- */
    /* -------------------- */

    table_row_t();
    table_row_t(table_desc_t* ptd)
	: _ptable(NULL),
	  _field_cnt(0), _is_setup(false),
	  _pvalues(NULL),
	  _fixed_offset(0),_var_slot_offset(0),_var_offset(0),_null_count(0),
	  _rep(NULL), _rep_key(NULL)
    {
        assert (ptd);
        setup(ptd);
    }

    virtual ~table_row_t();



    /* ----------------------------------------------------------------- */
    /* --- setup row according to table description, asserts if NULL --- */
    /* --- this setup is done only once, at the initialization of    --- */
    /* --- the record in the cache                                   --- */
    /* ----------------------------------------------------------------- */

    int setup(table_desc_t* ptd);


    /* ---------------------- */
    /* --- access methods --- */
    /* ---------------------- */

    inline offset_t get_fixed_offset() const { return (_fixed_offset); }
    inline offset_t get_var_slot_offset() const { return (_var_slot_offset); }
    inline offset_t get_var_offset() const { return (_var_offset); }
    inline unsigned get_null_count() const { return (_null_count); }

    unsigned size() const;


    /* ------------------------ */
    /* --- set field values --- */
    /* ------------------------ */

    void set_null(const unsigned idx);
    void set_value(const unsigned idx, const int v);
    void set_value(const unsigned idx, const bool v);
    void set_value(const unsigned idx, const short v);
    void set_value(const unsigned idx, const double v);
    void set_value(const unsigned idx, const long long v);
    void set_value(const unsigned idx, const uint64_t v);
    void set_value(const unsigned idx, const decimal v);
    void set_value(const unsigned idx, const time_t v);
    void set_value(const unsigned idx, const char v);
    void set_value(const unsigned idx, const char* string);
    void set_value(const unsigned idx, const timestamp_t& time);


    /* ------------------------ */
    /* --- get field values --- */
    /* ------------------------ */

    bool get_value(const unsigned idx, int& dest) const;
    bool get_value(const unsigned idx, bool& dest) const;
    bool get_value(const unsigned idx, short& dest) const;
    bool get_value(const unsigned idx, char& dest) const;
    bool get_value(const unsigned idx, char* destbuf, const unsigned bufsize) const;
    bool get_value(const unsigned idx, double& dest) const;
    bool get_value(const unsigned idx, long long& dest) const;
    bool get_value(const unsigned idx, uint64_t& dest) const;
    bool get_value(const unsigned idx, decimal& dest) const;
    bool get_value(const unsigned idx, time_t& dest) const;
    bool get_value(const unsigned idx, timestamp_t& dest) const;


    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void print_values(ostream& os = cout); /* print the tuple values */
    void print_tuple();                    /* print the whole tuple */
    void print_tuple_no_tracing();         /* print the whole tuple without trace msg */


    /* ------------------------------ */
    /* --- required functionality --- */
    /* ------------------------------ */

    //    virtual void reset()=0; /* clear the tuple and prepare it for re-use */

    /* clear the tuple and prepare it for re-use */
    void reset() {
        assert (_is_setup);
        for (unsigned i=0; i<_field_cnt; i++)
            _pvalues[i].reset();
    }

    void freevalues()
    {
        if (_pvalues) {
            delete [] _pvalues;
            _pvalues = NULL;
        }
    }

}; // EOF: table_row_t


/******************************************************************
 *
 * class tuple_guard
 *
 * @brief: guard object to manage table_row_t operations more easily
 *         reduces code complexity when using table_row_t in xcts
 *
 ******************************************************************/
template<class M, class T=table_row_t>
struct tuple_guard {
    T* ptr;
    M* manager;
    tuple_guard(M* m)
	: ptr(m->get_tuple()), manager(m) { assert(ptr); }
    ~tuple_guard() { manager->give_tuple(ptr); }
    T* operator->() { return ptr; }
    operator T*() { return ptr; }
private:
    // no you copy!
    tuple_guard(tuple_guard&);
    void operator=(tuple_guard&);
};


/******************************************************************
 *
 * class table_row_t methods
 *
 * @brief: The {set,get}_value() functions are very frequently called.
 *         Therefore, they have been inlined here.
 *
 ******************************************************************/


/******************************************************************
 *
 * SET value functions
 *
 ******************************************************************/

inline void table_row_t::set_null(const unsigned idx)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_null();
}

inline void table_row_t::set_value(const unsigned idx, const int v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_int_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const bool v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_bit_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const short v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_smallint_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const double v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_float_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const long long v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_long_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const uint64_t v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_long_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const decimal v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_decimal_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const time_t v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_time_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const char v)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_char_value(v);
}

inline void table_row_t::set_value(const unsigned idx, const char* string)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());

    sqltype_t sqlt = _pvalues[idx].field_desc()->type();
    assert (sqlt == SQL_VARCHAR || sqlt == SQL_FIXCHAR );

    int len = strlen(string);
    if ( sqlt == SQL_VARCHAR ) {
        // if variable length
        _pvalues[idx].set_var_string_value(string, len);
    }
    else {
        // if fixed length
        _pvalues[idx].set_fixed_string_value(string, len);
    }
}

inline void table_row_t::set_value(const unsigned idx, const timestamp_t& time)
{
    assert (_is_setup);
    assert (idx < _field_cnt);
    assert (_pvalues[idx].is_setup());
    _pvalues[idx].set_value(&time, 0);
}



/******************************************************************
 *
 * GET value functions
 *
 ******************************************************************/

inline bool table_row_t::get_value(const unsigned idx,
                                   int& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = 0;
        return false;
    }
    dest = _pvalues[idx].get_int_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   bool& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = false;
        return false;
    }
    dest = _pvalues[idx].get_bit_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   short& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = 0;
        return false;
    }
    dest = _pvalues[idx].get_smallint_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   char& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = 0;
        return false;
    }
    dest = _pvalues[idx].get_char_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   char* destbuf,
                                   const unsigned bufsize) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        destbuf[0] = '\0';
        return (false);
    }
    // if variable length
    unsigned sz = MIN(bufsize-1, _pvalues[idx]._max_size);
    _pvalues[idx].get_string_value(destbuf, sz);
    destbuf[sz] ='\0';
    return (true);
}

inline bool table_row_t::get_value(const unsigned idx,
                                   double& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = 0;
        return false;
    }
    dest = _pvalues[idx].get_float_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   uint64_t& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = 0;
        return false;
    }
    dest = _pvalues[idx].get_long_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   long long& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = 0;
        return false;
    }
    dest = _pvalues[idx].get_long_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   decimal& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        dest = decimal(0);
        return false;
    }
    dest = _pvalues[idx].get_decimal_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   time_t& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        return false;
    }
    dest = _pvalues[idx].get_time_value();
    return true;
}

inline bool table_row_t::get_value(const unsigned idx,
                                   timestamp_t& dest) const
{
    assert (_is_setup);
    assert(idx < _field_cnt);
    if (_pvalues[idx].is_null()) {
        return false;
    }
    dest = _pvalues[idx].get_tstamp_value();
    return true;
}


#endif /* __SHORE_ROW_H */
