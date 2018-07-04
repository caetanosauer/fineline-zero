/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef ALLOC_CACHE_H
#define ALLOC_CACHE_H

#include "w_defines.h"
#include "alloc_page.h"
#include "latches.h"
#include "stnode_page.h"
#include <map>
#include <vector>
#include <unordered_set>

class bf_fixed_m;

/**
 * \brief Free-Page allocation/deallocation interface.
 *
 * \details
 * This object handles allocation/deallocation requests for one volume.
 * All allocation/deallocation are logged and done in a critical section.
 * To make it scalable, this object is designed to be as fast as possible.
 * @See alloc_page_h
 */
class alloc_cache_t {
public:
    alloc_cache_t(stnode_cache_t& stcache, bool virgin, bool clustered);

    /**
     * Allocates one page. (System transaction)
     * @param[out] pid allocated page ID.
     * @param[in] stid StoreID to which this page will belong -- this is used for
     *              clustering pages of the same store in the same extents
     */
    rc_t sx_allocate_page(PageID &pid, StoreID stid = 0);

    /**
     * Deallocates one page. (System transaction)
     * @param[in] pid page ID to deallocate.
     */
    rc_t sx_deallocate_page(PageID pid);

    /**
     * Formats an alloc page for a new extent. Called internally in
     * sx_allocate_page.
     */
    rc_t sx_format_alloc_page(PageID alloc_pid);

    bool is_allocated (PageID pid);

    /// Returns last allocated PID of a given store
    PageID get_last_allocated_pid(StoreID s) const;

    /// Returns last allocated PID of ALL stores
    PageID get_last_allocated_pid() const;
    PageID _get_last_allocated_pid_internal() const;

    lsn_t get_page_lsn(PageID pid);

    static constexpr size_t extent_size = alloc_page::bits_held;

    static bool is_alloc_pid(PageID pid) { return pid % extent_size == 0; }

    PageID num_pages() const
    {
        return get_last_allocated_pid() + 1;
    }

private:

    /**
     * Keep track of free pages using the ID of the last allocated page and
     * a list of free pages whose IDs are lower than that.
     *
     * Pages which are freed end up in the list of freed pages, again
     * one for each store. Currently, these lists are only used to determine
     * whether a certain page is allocated or not. To avoid fragmentation in
     * a workload with many deletions, items should be removed from these lists
     * when allocating a page to avoid fragmentation. One extreme policy would
     * be to only use the contiguous space when the corresponding list is
     * empty, i.e., when the non-contiguous space has been fully utilized.
     * Policies that trade off allocation performance for fragmentation by
     * managing allocations from both contiguous and non-contiguous space would
     * be the more flexible and robust option.
     *
     * In Feb 2017, this was extended to support clustering pages by store ID,
     * which requires assigning extents to stores exclusively, which means
     * that we must keep track of page allocation on a per-store basis.
     */
    std::vector<PageID> last_alloc_page;

    stnode_cache_t& stcache;

    /** This lath protects access to last_alloc_page */
    mutable srwlock_t _latch;

    bool _cluster_stores;

    /** Reads the alloc page of given extent to update last_alloc_page */
    rc_t load_alloc_page(StoreID stid, extent_id_t ext);
};

#endif // ALLOC_CACHE_H
