/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "log_lsn_tracker.h"
#include "finelog_basics.h"
#include "AtomicCounter.hpp"

#include <cstring>

using namespace std;

// use this to compute highest prime #
// less that requested hash table size.
// Actually, it uses the highest prime less
// than the next power of 2 larger than the
// number requested.  Lowest allowable
// hash table option size is 64.

static const uint32_t primes[] = {
    /* 0x40, 64, 2**6 */ 61,
    /* 0x80, 128, 2**7  */ 127,
    /* 0x100, 256, 2**8 */ 251,
    /* 0x200, 512, 2**9 */ 509,
    /* 0x400, 1024, 2**10 */ 1021,
    /* 0x800, 2048, 2**11 */ 2039,
    /* 0x1000, 4096, 2**12 */ 4093,
    /* 0x2000, 8192, 2**13 */ 8191,
    /* 0x4000, 16384, 2**14 */ 16381,
    /* 0x8000, 32768, 2**15 */ 32749,
    /* 0x10000, 65536, 2**16 */ 65521,
    /* 0x20000, 131072, 2**17 */ 131071,
    /* 0x40000, 262144, 2**18 */ 262139,
    /* 0x80000, 524288, 2**19 */ 524287,
    /* 0x100000, 1048576, 2**20 */ 1048573,
    /* 0x200000, 2097152, 2**21 */ 2097143,
    /* 0x400000, 4194304, 2**22 */ 4194301,
    /* 0x800000, 8388608, 2**23   */ 8388593
};

oldest_lsn_tracker_t::oldest_lsn_tracker_t(uint32_t buckets) {
    // same logic as in lock_core(). yes, stupid prime hashing. but see the name of this class.
    int b = 0; // count bits shifted
    for (_buckets = 1; _buckets < buckets; _buckets <<= 1) {
        b++;
    }
    w_assert1(b >= 6 && b <= 23);
    b -= 6;
    _buckets = primes[b];

    _low_water_marks = new lsndata_t[_buckets];
    w_assert1(_low_water_marks);
    ::memset(_low_water_marks, 0, sizeof(lsndata_t) * _buckets);
}
oldest_lsn_tracker_t::~oldest_lsn_tracker_t() {
#if W_DEBUG_LEVEL > 0
    for (uint32_t i = 0; i < _buckets; ++i) {
        if (_low_water_marks[i] != 0) {
            ERROUT(<<"Non-zero _low_water_marks! i=" << i << ", val=" << _low_water_marks[i]);
            w_assert1(_low_water_marks[i] == 0);
        }
    }
#endif // W_DEBUG_LEVEL > 0
    delete[] _low_water_marks;
}

void oldest_lsn_tracker_t::enter(uint64_t xct_id, const lsn_t& curr_lsn) {
    lsndata_t data = curr_lsn.data();
    lsndata_t cas_tmp = 0;
    uint32_t index = xct_id % _buckets;
    DBGOUT4(<<"oldest_lsn_tracker_t::enter. xct_id=" << xct_id
        << ", index=" << index <<", data=" << data);
    lsndata_t *address = _low_water_marks + index;
    int counter = 0;
    while (!lintel::unsafe::atomic_compare_exchange_strong<lsndata_t>(
        address, &cas_tmp, data)) {
        cas_tmp = 0;
        ++counter;
        if ((counter & 0xFFFF) == 0) {
            DBGOUT1(<<"WARNING: spinning on oldest_lsn_tracker_t::enter..");
        } else if ((counter & 0xFFFFFF) == 0) {
            ERROUT(<<"WARNING: spinning on oldest_lsn_tracker_t::enter for LONG time..");
        }
    }
    w_assert1(_low_water_marks[index] == data);
}

void oldest_lsn_tracker_t::leave(uint64_t xct_id) {
    uint32_t index = xct_id % _buckets;
    DBGOUT4(<<"oldest_lsn_tracker_t::leave. xct_id=" << xct_id
        << ", index =" << index <<", current value=" << _low_water_marks[index]);
    _low_water_marks[index] = 0;
}


lsn_t oldest_lsn_tracker_t::get_oldest_active_lsn(lsn_t curr_lsn) {
    lsndata_t smallest = lsn_t::max.data();
    for (uint32_t i = 0; i < _buckets; ++i) {
        if (_low_water_marks[i] != 0 && _low_water_marks[i] < smallest) {
            smallest = _low_water_marks[i];
        }
    }
    _cache = lsn_t(smallest == lsn_t::max.data() ? curr_lsn.data() : smallest);
    return _cache;
}
