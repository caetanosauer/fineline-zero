#include "w_defines.h"

/**
 * Implementation of Search/Lookup functions in btree_impl.h.
 * Separated from btree_impl.cpp.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#include "page_bf_inline.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "btcursor.h"
#include "sm_base.h"
#include "vec_t.h"
#include "w_key.h"
#include "xct.h"

rc_t
btree_impl::_ux_lookup(
    volid_t vol, snum_t store,
    const w_keystr_t&       key,
    bool&               found,
    void*               el,
    smsize_t&           elen
    )        
{
    FUNC(btree_impl::_ux_lookup);
    INC_TSTAT(bt_find_cnt);
    while (true) {
        rc_t rc = _ux_lookup_core (vol, store, key, found, el, elen);
        if (rc.is_error() && rc.err_num() == eLOCKRETRY) {
            continue;
        }
        return rc;
    }
    return RCOK;
}

rc_t
btree_impl::_ux_lookup_core(
    volid_t vol, snum_t store,
    const w_keystr_t&   key,
    bool&               found,
    void*               el,
    smsize_t&           elen
    )        
{
    bool need_lock = g_xct_does_need_lock();
    bool ex_for_select = g_xct_does_ex_lock_for_select();

    btree_p                leaf; // first-leaf

    // find the leaf (potentially) containing the key
    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_SH, leaf));

    w_assert1(leaf.is_fixed());
    w_assert1(leaf.is_leaf());

    // then find the tuple in the page
    slotid_t slot;
    leaf.search_leaf(key, found, slot);
    if (!found) {
        if (need_lock) {
            W_DO(_ux_lock_range(leaf, key, slot, LATCH_SH,
                ex_for_select ? XN : SN,
                ex_for_select ? NX : NS,
                false));
        }
        return RCOK;
    }

    // the key is found (though it might be a ghost)! let's get a lock.
    if (need_lock) {
        // only the key is locked (SN)
        W_DO (_ux_lock_key(leaf, key, LATCH_SH,
            ex_for_select ? XN : SN, false));
    }

    // Copy the element 
    // assume caller provided space
    w_assert1(el != NULL || elen == 0);
    bool ghost;
    bool will_fit = leaf.dat_leaf(slot, (char*) el, elen, ghost); // this sets elen
    if (ghost) {
        found = false;
        return RCOK;
    }
    if (!will_fit) {
        // just return the required size
        return RC(eRECWONTFIT);
    }
    return RCOK;
}

rc_t
btree_impl::_ux_traverse(
    volid_t vol, snum_t store,
    const w_keystr_t &key,
    traverse_mode_t traverse_mode,
    latch_mode_t leaf_latch_mode,
    btree_p &leaf,
    bool allow_retry
    )
{
    FUNC(btree_impl::_ux_traverse);
    INC_TSTAT(bt_traverse_cnt);
    if (key.is_posinf()) {
        if (traverse_mode == t_fence_contain) {
            // this is kind of misuse of this function, but
            // let's tolerate it and consider it as search for last page
            traverse_mode = t_fence_high_match;
        }
        w_assert1(traverse_mode != t_fence_low_match); // surely misuse
    }

    shpid_t leaf_pid_causing_failed_upgrade = 0;
    for (int times = 0; times < 20; ++times) { // arbitrary number
        inquery_verify_init(vol, store); // initialize in-query verification
        btree_p root_p;
        w_assert1(!root_p.is_fixed());
        bool should_try_ex = (leaf_latch_mode == LATCH_EX &&
            leaf_pid_causing_failed_upgrade == smlevel_0::bf->get_root_page_id(vol, store));
        W_DO( root_p.fix_root(vol, store, should_try_ex ? LATCH_EX : LATCH_SH));
        w_assert1(root_p.is_fixed());
        
        if (root_p.get_blink() != 0) {
            // root page has foster-child! let's grow the tree.
            if (root_p.latch_mode() != LATCH_EX) {
                root_p.unfix(); // don't upgrade. re-fix.
                W_DO( root_p.fix_root(vol, store, LATCH_EX));
            }
            W_DO(_sx_grow_tree(root_p));
            --times; // we don't penalize this. do it again.
            continue;
        }
        
        rc_t rc = _ux_traverse_recurse (root_p, key, traverse_mode, leaf_latch_mode, leaf, leaf_pid_causing_failed_upgrade);

        if (rc.is_error()) {
            if (rc.err_num() == eGOODRETRY) {
                // did some opportunistic structure modification, and going to retry
                --times; // we don't penalize this. do it again.
                continue;
            }
            if (rc.err_num() == eRETRY) {
                w_assert1(leaf_latch_mode == LATCH_EX);
                w_assert1(leaf_pid_causing_failed_upgrade != 0);
                // this means we failed to upgrade latch in leaf level.
                if (allow_retry) {
                    continue; //retry! with the leaf_pid_causing_failed_upgrade
                }
            }
            // otherwise it's failed
            return rc;
        } else {
            return RCOK;
        }
    }
#if W_DEBUG_LEVEL>0
    cout << "latch contention too many times!" << endl;
#endif
    return RC (eTOOMANYRETRY);
}

rc_t
btree_impl::_ux_traverse_recurse(
    btree_p&                        start,
    const w_keystr_t&               key,
    btree_impl::traverse_mode_t     traverse_mode,
    latch_mode_t                    leaf_latch_mode,
    btree_p&                        leaf,
    shpid_t&                        leaf_pid_causing_failed_upgrade
    )
{
    FUNC(btree_impl::_ux_traverse_recurse);
    INC_TSTAT(bt_partial_traverse_cnt);

    /// cache the flag to avoid calling the functions each time
    bool do_inquery_verify = (xct() != NULL && xct()->is_inquery_verify());
    
    if (leaf.is_fixed()) {
        leaf.unfix();
    }

    // this part is now loop, not recursion to prevent the stack from growing too long
    btree_p *current = &start;
    btree_p followed_p; // for latch coupling
    btree_p *next = &followed_p;
    while (true) {
        if (do_inquery_verify) inquery_verify_fact(*current); // check the current page

        // use fence key to tell if this is the page
        bool this_is_the_leaf_page = false;
        slot_follow_t slot_to_follow = t_follow_invalid;
        _ux_traverse_search(traverse_mode, current, key,
                            this_is_the_leaf_page, slot_to_follow);
        
        //Should be able to search that:
        // (this_is_the_leaf_page && slot_to_follow==t_follow_invalid) ||
        // (!this_is_the_leaf_page && slot_to_follow=!=t_follow_invalid)
        
        if (this_is_the_leaf_page) {
            leaf = *current;
            // re-fix to apply the given latch mode
            if (leaf_latch_mode != LATCH_SH && leaf.latch_mode() != LATCH_EX) {
                if (!leaf.upgrade_latch_conditional()) {
                    // can't get EX latch, so restart from the root
                    DBGOUT2(<< ": latch update conflict at " << leaf.pid() 
                            << ". need restart from root!");
                    leaf_pid_causing_failed_upgrade = leaf.shpid();
                    leaf.unfix();
                    return RC(eRETRY);
                }
            }
            break; // done!
        }
        
        if (do_inquery_verify) {
            inquery_verify_expect(*current, slot_to_follow); // adds expectation
        }

        shpid_t pid_to_follow;
        if (slot_to_follow == t_follow_blink) {
            pid_to_follow = current->get_blink();
        } else if (slot_to_follow == t_follow_pid0) {
            pid_to_follow = current->pid0();
        } else {
            pid_to_follow = current->child(slot_to_follow);
        }    
        w_assert1(pid_to_follow);

        // if worth it, do eager adoption. If it actually did something, it returns 
        // eGOODRETRY so that we will exit and retry
        if ((current->level() >= 3 || 
             (current->level() == 2 && slot_to_follow == t_follow_blink)) // next will be non-leaf
            && is_ex_recommended(pid_to_follow)) { // next is VERY hot
            W_DO (_ux_traverse_try_eager_adopt(*current, pid_to_follow));
        }
        
        bool should_try_ex = leaf_latch_mode == LATCH_EX && (
            pid_to_follow == leaf_pid_causing_failed_upgrade
            || current->latch_mode() == LATCH_EX // we have EX latch; don't SH latch
            );
        
        if(current->level()==2 && slot_to_follow!=t_follow_blink && leaf_latch_mode==LATCH_EX) {
            //We're likely going to find the target next, so go ahead and EX if we need to.
            //The other possibility is a long adoption chain; if that is the case this is
            //a performance oops, but we're also messed up anyway, so fix the bad chain and
            //still do this.
            should_try_ex = true;
        }
        
        W_DO(next->fix_nonroot(*current, current->vol(), pid_to_follow, 
                               should_try_ex ? LATCH_EX : LATCH_SH));
        
        if (slot_to_follow != t_follow_blink && next->get_blink() != 0) {
            // we followed a real-child pointer and found that it has blink..  let's adopt it! (but
            // opportunistically).  just like eager adoption, it returns eGOODRETRY and we will
            // retry if it succeeded. otherwise, go on.
            W_DO(_ux_traverse_try_opportunistic_adopt(*current, *next));
        }

        current->unfix();
        std::swap(current, next);
    }
        
    return RCOK;
}
void btree_impl::_ux_traverse_search(btree_impl::traverse_mode_t traverse_mode,
                                     btree_p *current,
                                     const w_keystr_t& key,
                                     bool &this_is_the_leaf_page, slot_follow_t &slot_to_follow) {
    if (traverse_mode == t_fence_contain) {
        if (current->compare_with_fence_high(key) < 0) {
            w_assert1(current->fence_contains(key));
            // this page can contain the key, but..
            if (current->is_leaf()) {
                this_is_the_leaf_page = true;
            } else {
                // this is an interior node. find a child to follow
                slotid_t slot;
                current->search_node(key, slot);
                if (slot < 0) {
                    slot_to_follow = t_follow_pid0;
                } else {
                    slot_to_follow = (slot_follow_t) slot;
                }
            }
        } else {
            // this page can't contain the key.
            // If search key is higher than high-fence of "start", which occurs
            // only when "start" has b-link buddies, then follows blink pointers.
            // Note, because the search path has chosen to read "start",
            // this page or one of its blinks have fence keys containing the search key.
            w_assert2(current->get_blink());
            w_assert2(current->compare_with_fence_high(key) >= 0);
            
            // let's follow blink
            slot_to_follow = t_follow_blink;
        }
    } else if (traverse_mode == t_fence_low_match) {
        int d = current->compare_with_fence_low(key);
        if (d == 0) {
            if (current->is_leaf()) {
                this_is_the_leaf_page = true;
            } else {
                // follow left-most child.
                slot_to_follow = t_follow_pid0;
            }
        } else {
            w_assert2(d > 0); // if d<0 (key<fence-low), we failed something
            if (current->compare_with_fence_high(key) >= 0) {
                // key is even higher than fence-high, then there must be blinked page
                // let's follow blink
                slot_to_follow = t_follow_blink;
            } else {
                // otherwise, one of the children must have the fence-low
                w_assert2(!current->is_leaf()); // otherwise we should have seen an exact match
                slotid_t slot;
                current->search_node(key, slot);
                // unlike fence-high key match, we can simply use the search result
                slot_to_follow = (slot_follow_t) slot;
            }
        }
    } else {
        w_assert2(traverse_mode == t_fence_high_match);
        int d = current->compare_with_fence_high(key);
        if (d == 0) {
            if (current->is_leaf()) {
                this_is_the_leaf_page = true;
            } else {
                // must be the last child.
                slot_to_follow = (slot_follow_t) (current->nrecs() - 1);
            }
        } else if (d > 0) {
            // key is higher than fence-high, then there must be blinked page
            // let's follow blink
            slot_to_follow = t_follow_blink;
        } else {
            // key is lower than fence-high. then one of the children must have it.
            w_assert2(!current->is_leaf()); // otherwise we should have seen an exact match
            slotid_t slot;
            current->search_node(key, slot);
            // search_node returns the slot in which key resides or should go.
            // For example, a separator key "AB"
            // sends "AA" to left, "AAZ" to left, "AB" to right,
            // "ABA" to right, "AC" to right.
            
            // However, now we are looking for fence-high key match,
            // so we want to send "AB" to left.
            w_keystr_t slot_key;
            current->node_key(slot, slot_key);
            if (key.compare(slot_key) == 0) {
                --slot; // if exact match, try the previous slot
                w_assert2(slot < current->nrecs() && slot >= -1);
            }
            
            slot_to_follow = (slot_follow_t) slot;
        }
    }
} 
rc_t btree_impl::_ux_traverse_try_eager_adopt(btree_p &current, shpid_t next_pid)
{
    w_assert1(current.is_fixed());
    queue_based_lock_t *mutex = mutex_for_high_contention(next_pid);
    w_assert1(mutex);
    {
        CRITICAL_SECTION(cs, *mutex);
        if (!is_ex_recommended(next_pid)) {
            // probably some other thread has already done it. so just do nothing.
            // even if this is unluckily some accident, that's fine. adoption can be delayed.
            // Also, we should exit critical section as soon as possible!
            // and just go on (return RCOK, not eGOODRETRY) with SH latch
            return RCOK;
        }
        btree_p next;
        W_DO(next.fix_nonroot(current, current.vol(), next_pid, LATCH_EX));
        
        // okay, now we got EX latch, but..
        if (!is_ex_recommended(next_pid)) {
            // same as above
            return RCOK;
        } else {
            // this page has been requested for adoption many times. Let's do it!
            W_DO(_sx_adopt_blink_sweep_approximate(next, 0));
        }
    }
    return RC(eGOODRETRY); // to be safe, let's restart. this is anyway rare event
}
rc_t btree_impl::_ux_traverse_try_opportunistic_adopt(btree_p &current, btree_p &next)
{
    w_assert1(current.is_fixed());
    w_assert1(next.is_fixed());
    bool pushedup;
    W_DO(_sx_opportunistic_adopt_blink(current, next, pushedup));
    // if it's pushed up, we restart the search from root
    // (we can keep searching with a bit complicated code.. but wouldn't worth it)
    if (pushedup) {
        return RC(eGOODRETRY);
    } else {
        return RCOK; // go on
    }           
    
}
