#include "page_evictioner.h"

#include "bf_tree.h"
#include "xct_logger.h"
#include "btree_page_h.h"

// Template definitions
#include "bf_hashtable.cpp"

page_evictioner_base::page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options)
    :
    worker_thread_t(options.get_int_option("sm_eviction_interval", 100)),
    _bufferpool(bufferpool),
    _rnd_distr(1, _bufferpool->get_block_cnt() - 1)
{
    _swizzling_enabled = options.get_bool_option("sm_bufferpool_swizzle", false);
    _maintain_emlsn = options.get_bool_option("sm_bf_maintain_emlsn", false);
    _random_pick = options.get_bool_option("sm_evict_random", false);
    _use_clock = options.get_bool_option("sm_evict_use_clock", true);
    _log_evictions = options.get_bool_option("sm_log_page_evictions", false);
    _write_elision = options.get_bool_option("sm_write_elision", false);
    // _evict_unarchived is the FineLine-equivalent of write elision
    _evict_unarchived = options.get_bool_option("sm_evict_unarchived", false);
    // Generate page image log record on eviction if log volume of page is above this threshold
    _page_img_on_evict_threshold = options.get_int_option("sm_page_img_on_evict_threshold", 0);

    if (_use_clock) { _clock_ref_bits.resize(_bufferpool->get_block_cnt(), false); }

    _current_frame = 0;

    constexpr unsigned max_rounds = 1000;
    _max_attempts = max_rounds * _bufferpool->get_block_cnt();
}

page_evictioner_base::~page_evictioner_base()
{
}

void page_evictioner_base::do_work()
{
    /**
     * When eviction is triggered, _about_ this number of frames will be evicted at once.
     * Given as a ratio of the buffer size (currently 1%)
     */
    constexpr float EVICT_BATCH_RATIO = 0.01;

    uint32_t preferred_count = EVICT_BATCH_RATIO * _bufferpool->_block_cnt + 1;

    // In principle, _freelist_len requires a fence, but here it should be OK
    // because we don't need to read a consistent value every time.
    while(_bufferpool->_freelist_len < preferred_count)
    {
        bf_idx victim = pick_victim();

        if (evict_one(victim)) {
            _bufferpool->_add_free_block(victim);
        }

        /* Rather than waiting for all the pages to be evicted, we notify
         * waiting threads every time a page is evicted. One of them is going to
         * be able to re-use the freed slot, the others will go back to waiting.
         */
        notify_one();

        if (should_exit()) { break; }
    }

    // cerr << "Eviction done; free frames: " << _bufferpool->_freelist_len << endl;
}

bool page_evictioner_base::evict_one(bf_idx victim)
{
    bf_tree_cb_t& cb = _bufferpool->get_cb(victim);
    w_assert1(cb.latch().is_mine());

    if(!unswizzle_and_update_emlsn(victim)) {
        /* We were not able to unswizzle/update parent, therefore we cannot
         * proceed with this victim. We just jump to the next iteration and
         * hope for better luck next time. */
        cb.latch().latch_release();
        INC_TSTAT(bf_evict_failed_unswizzle);
        return false;
    }

    // Try to atomically set pin from 0 to -1; give up if it fails
    if (!cb.prepare_for_eviction()) {
        cb.latch().latch_release();
        INC_TSTAT(bf_evict_failed_pinned);
        return false;
    }

    // We're passed the point of no return: eviction must happen no mather what

    generic_page* p = &_bufferpool->_buffer[victim];
    auto pid = cb._pid;

    // Generate page-image log record to optimize future fetches of this page and
    // keep log archive compact
    bool img_generated = false;
    if (_page_img_on_evict_threshold > 0) {
        if (cb.get_log_volume() >= _page_img_on_evict_threshold) {
            sys_xct_section_t sx;
            fixable_page_h fpage;
            fpage.fix_nonbufferpool_page(p);
            XctLogger::log_p<LogRecordType::page_img_format_log>(&fpage);
            W_COERCE(sx.end_sys_xct(RCOK));
            img_generated = true;
        }
    }

    // Log page eviction event
    w_assert1(cb.latch().is_mine());
    if (_log_evictions) {
        Logger::log_sys<LogRecordType::evict_page_log>(pid, p->version);
    }
    // Add to table of evicted pages (FineLine)
    if (_evict_unarchived) {
        _bufferpool->add_evicted_page(pid, p->version, img_generated);
    }

    // Remove pid from the buffer pool hash table
    w_assert1(cb._pin_cnt < 0);
    w_assert1(!cb._used);
    bool removed = _bufferpool->_hashtable->remove(pid);
    w_assert1(removed);

//     cerr << "EVICTED " << victim << " pid " << pid << " epoch " << p->epoch << " lowest_active " <<
//         ss_m::log->get_epoch_tracker().get_lowest_active_epoch() << endl;

    // Latch can finally be released and frame can be occupied by another page
    cb.latch().latch_release();

    INC_TSTAT(bf_evict);
    return true;
}

void page_evictioner_base::ref(bf_idx idx)
{
    if (_use_clock && !_clock_ref_bits[idx]) { _clock_ref_bits[idx] = true; }
}

bf_idx page_evictioner_base::pick_victim()
{
     auto next_idx = [this]
     {
         if (_current_frame > _bufferpool->_block_cnt) {
             // race condition here, but it's not a big deal
             _current_frame = 1;
         }
         return _random_pick ? get_random_idx() : _current_frame++;
     };

     unsigned attempts = 0;
     while(true) {

        if (should_exit()) return 0; // in bf_tree.h, 0 is never used, means null

        bf_idx idx = next_idx();
        if (idx >= _bufferpool->_block_cnt || idx == 0) { idx = 1; }

        attempts++;
        if (attempts >= _max_attempts) {
            W_FATAL_MSG(fcINTERNAL, << "Eviction got stuck!");
        }

        auto& cb = _bufferpool->get_cb(idx);

        if (!cb._used) {
            INC_TSTAT(bf_evict_failed_unused);
            continue;
        }

        // If I already hold the latch on this page (e.g., with latch
        // coupling), then the latch acquisition below will succeed, but the
        // page is obvisouly not available for eviction. This would not happen
        // if every fix would also pin the page, which I didn't wan't to do
        // because it seems like a waste.  Note that this is only a problem
        // with threads perform their own eviction (i.e., with the option
        // _async_eviction set to false in bf_tree_m), because otherwise the
        // evictioner thread never holds any latches other than when trying to
        // evict a page.  This bug cost me 2 days of work. Anyway, it should
        // work with the check below for now.
        if (cb.latch().held_by_me()) {
            // I (this thread) currently have the latch on this frame, so
            // obviously I should not evict it
            INC_TSTAT(bf_evict_failed_latch);
            continue;
        }

        // Step 1: latch page in EX mode and check if eligible for eviction
        auto latch_rc = cb.latch().latch_acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE);
        if (latch_rc != AcquireResult::OK) {
            INC_TSTAT(bf_evict_failed_latch);
            DBG3(<< "Eviction failed on latch for " << idx);
            continue;
        }
        w_assert1(cb.latch().is_mine());

        // Only evict if clock refbit is not set
        if (_use_clock && _clock_ref_bits[idx]) {
            _clock_ref_bits[idx] = false;
            cb.latch().latch_release();
            INC_TSTAT(bf_evict_failed_clock);
            continue;
        }

        // FineLine eviction: only evict pages whose last modification is older than
        // the archived epoch ...
        auto archived_epoch = ArchiveIndex::archived_epoch.load();
        // ... unless _evict_unarchived is set and we tried one round already
        constexpr unsigned max_archived_attempts = 100;
        bool ignore_epoch = _evict_unarchived && (attempts > max_archived_attempts);

        // now we hold an EX latch -- check if page qualifies for eviction
        btree_page_h p;
        p.fix_nonbufferpool_page(_bufferpool->_buffer + idx);
        // We do not consider for eviction...
        if (
                // ... the stnode page
                p.tag() == t_stnode_p
                // ... B-tree inner (non-leaf) pages
                // (requires unswizzling, which is not supported)
                // || (p.tag() == t_btree_p && !p.is_leaf())
                // ... B-tree root pages
                // (note, single-node B-tree is both root and leaf)
                || (p.tag() == t_btree_p && p.pid() == p.root())
                // ... B-tree pages that have a foster child
                // (requires unswizzling, which is not supported)
                // || (p.tag() == t_btree_p && p.get_foster() != 0)
                // ... unused frames, which don't hold a valid page
                || !cb._used
                // ... frames prefetched by restore but not yet restored
                || cb.is_pinned_for_restore()
                // ... pages that might contain uncommitted updates (FineLine)
                || (p.get_epoch() >= ss_m::log->get_epoch_tracker().get_lowest_active_epoch())
                // ... pages whose last update hasn't made it to the archive yet (FineLine)
                || (!ignore_epoch && p.get_epoch() >= archived_epoch)
        )
        {
            cb.latch().latch_release();
            INC_TSTAT(bf_evict_failed_flags);
            DBG5(<< "Eviction failed on flags for " << idx);
            continue;
        }

        // ... pinned frames, i.e., someone required it not be evicted
        if(cb._pin_cnt != 0) {
            cb.latch().latch_release();
            INC_TSTAT(bf_evict_failed_pinned);
            DBG5(<< "Eviction failed on pin_cnt for " << idx);
            continue;
        }

        // If we got here, we passed all tests and have a victim!
        w_assert1(_bufferpool->_is_active_idx(idx));
        w_assert0(idx != 0);
        ADD_TSTAT(bf_eviction_attempts, attempts);
        return idx;
    }
}

bool page_evictioner_base::unswizzle_and_update_emlsn(bf_idx idx)
{
    if (!_maintain_emlsn && !_swizzling_enabled) { return true; }

    bf_tree_cb_t& cb = _bufferpool->get_cb(idx);
    w_assert1(cb.latch().is_mine());

    //==========================================================================
    // STEP 1: Look for parent.
    //==========================================================================
    PageID pid = _bufferpool->_buffer[idx].pid;
    bf_idx_pair idx_pair;
    bool found = _bufferpool->_hashtable->lookup(pid, idx_pair);
    w_assert1(found);

    bf_idx parent_idx = idx_pair.second;
    w_assert1(!found || idx == idx_pair.first);

    // If there is no parent, but write elision is off and the frame is not swizzled,
    // then it's OK to evict
    if (parent_idx == 0) {
        return !_bufferpool->_write_elision && !cb._swizzled;
    }

    bf_tree_cb_t& parent_cb = _bufferpool->get_cb(parent_idx);
    auto r = parent_cb.latch().latch_acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE);
    if (r != AcquireResult::OK) {
        /* Just give up. If we try to latch it unconditionally, we may deadlock,
         * because other threads are also waiting on the eviction mutex. */
        return false;
    }
    w_assert1(parent_cb.latch().is_mine());

    /* Look for emlsn slot on parent (must be found because parent pointer is
     * kept consistent at all times). */
    w_assert1(_bufferpool->_is_active_idx(parent_idx));
    generic_page *parent = &_bufferpool->_buffer[parent_idx];
    btree_page_h parent_h;
    parent_h.fix_nonbufferpool_page(parent);

    general_recordid_t child_slotid;
    if (_swizzling_enabled && cb._swizzled) {
        // Search for swizzled address
        PageID swizzled_pid = idx | SWIZZLED_PID_BIT;
        child_slotid = _bufferpool->find_page_id_slot(parent, swizzled_pid);
    }
    else {
        child_slotid = _bufferpool->find_page_id_slot(parent, pid);
    }
    w_assert1 (child_slotid != GeneralRecordIds::INVALID);

    //==========================================================================
    // STEP 2: Unswizzle pointer on parent before evicting.
    //==========================================================================
    if (_swizzling_enabled && cb._swizzled) {
        bool ret = _bufferpool->unswizzle(parent, child_slotid);
        w_assert0(ret);
        w_assert1(!cb._swizzled);
    }

    //==========================================================================
    // STEP 3: Page will be evicted -- update EMLSN on parent.
    //==========================================================================
    lsn_t old = parent_h.get_emlsn_general(child_slotid);
    // CS TODO FINELINE: no more page_lsn on CB!
    // _bufferpool->_buffer[idx].lsn = cb.get_page_lsn();
    // CS TODO FINELINE: get rid of emlsn!
    // if (_maintain_emlsn && old < _bufferpool->_buffer[idx].lsn) {
    //     DBG3(<< "Updated EMLSN on page " << parent_h.pid()
    //             << " slot=" << child_slotid
    //             << " (child pid=" << pid << ")"
    //             << ", OldEMLSN=" << old << " NewEMLSN=" <<
    //             _bufferpool->_buffer[idx].lsn);

    //     w_assert1(parent_cb.latch().is_mine());

    //     _bufferpool->_sx_update_child_emlsn(parent_h, child_slotid,
    //                                         _bufferpool->_buffer[idx].lsn);

    //     w_assert1(parent_h.get_emlsn_general(child_slotid)
    //                 == _bufferpool->_buffer[idx].lsn);
    // }

    parent_cb.latch().latch_release();
    return true;
}

