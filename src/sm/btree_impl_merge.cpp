/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of tree merge/rebalance related functions in btree_impl.h.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#include "sm_base.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "w_key.h"
#include "xct.h"
#include "bf.h"
#include "bf_tree.h"
#include "restart.h"

rc_t btree_impl::_sx_rebalance_foster(btree_page_h &page)
{
    w_assert1 (page.latch_mode() == LATCH_EX);
    if (page.get_foster() == 0) {
        return RCOK; // nothing to rebalance
    }

    // How many records we should move from foster-child to foster-parent?
    btree_page_h foster_p;
    W_DO(foster_p.fix_nonroot(page, page.vol(), page.get_foster_opaqueptr(), LATCH_EX));

    smsize_t used = page.used_space();
    smsize_t foster_used = foster_p.used_space();
    smsize_t balanced_size = (used + foster_used) / 2 + SM_PAGESIZE / 10;

    int move_count = 0;
    // worth rebalancing?
    if (used < foster_used * 3) {
        return RCOK; // don't bother
    }
    if (page.is_insertion_skewed_right()) {
        // then, anyway the foster-child will receive many more tuples. leave it.
        // also, this is needed to keep skewed-split meaningful
        return RCOK;
    }
    smsize_t move_size = 0;
    while (used - move_size > balanced_size && move_count < page.nrecs() - 1) {
        ++move_count;
        move_size += page.get_rec_space(page.nrecs() - move_count);
    }

    if (move_count == 0) {
        return RCOK;
    }

    // What would be the new fence key?
    w_keystr_t mid_key;
    shpid_t new_pid0;
    lsn_t   new_pid0_emlsn;
    if (foster_p.is_node()) {
        // Non-leaf node
        // then, choosing the new mid_key is easier, but handling of pid0 is uglier.
        btrec_t lowest (page, page.nrecs() - move_count);
        mid_key = lowest.key();
        new_pid0 = lowest.child();
        new_pid0_emlsn = lowest.child_emlsn();
    } else {
        // Leaf node
        // then, choosing the new mid_key is uglier, but handling of pid0 is easier.
        // pick new mid_key. this is same as split code though we don't look for shorter keys.
        btrec_t k1 (page, page.nrecs() - move_count - 1);
        btrec_t k2 (page, page.nrecs() - move_count);
        size_t common_bytes = k1.key().common_leading_bytes(k2.key());
        w_assert1(common_bytes < k2.key().get_length_as_keystr()); // otherwise the two keys are the same.
        mid_key.construct_from_keystr(k2.key().buffer_as_keystr(), common_bytes + 1);
        new_pid0 = 0;
        new_pid0_emlsn = lsn_t::null;
    }

    W_DO(_sx_rebalance_foster(page, foster_p, move_count, mid_key, new_pid0, new_pid0_emlsn));
    return RCOK;
}
rc_t btree_impl::_sx_rebalance_foster(btree_page_h &page, btree_page_h &foster_p,
            int32_t move_count, const w_keystr_t &mid_key, shpid_t new_pid0,
            lsn_t new_pid0_emlsn) {
    // assure foster-child page has an entry same as fence-low for locking correctness.
    // See jira ticket:84 "Key Range Locking" (originally trac ticket:86).
    W_DO(_ux_assure_fence_low_entry(foster_p)); // this might be another SSX

    // caller_commit == true if caller commits the single log system transaction
    // caller_commit == false if we are doing full logging for rebalance, the single log system transaction
    // will be committed by callee before it starts the full logging
    bool caller_commit = false;
    sys_xct_section_t sxs(true);
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_rebalance_foster_core(page, foster_p, move_count, mid_key, new_pid0,
        new_pid0_emlsn, caller_commit, sxs);
    // Commit the single log system transaction
    if (true == caller_commit)
        W_DO (sxs.end_sys_xct (ret));

    if ((true == restart_m::use_redo_full_logging_restart()) && (false == caller_commit))
    {
        // Rebalance is a system transaction with single log normally,
        // but if we are doing full logging due to page driven REDO, 
        // then a lot of log records have been generated (not a single log anymore)
        //
        // Not forcing a log flush because flushing should not impact recovery operation
        //W_COERCE( smlevel_0::log->flush_all() );        
    }
    return ret;
}

rc_t btree_impl::_ux_rebalance_foster_core(
            btree_page_h &page,             // Source
            btree_page_h &foster_p,         // Destination
            int32_t move_count, 
            const w_keystr_t &mid_key,      // Fence key for destination, also the new foster key in source
            shpid_t new_pid0,               // Non-leaf only
            lsn_t new_pid0_emlsn,           // Non-leaf only
            bool &caller_commit,            // Out: true if caller has to commit the system transaction, not full logging
                                            //       false if callee commits the system transaction, full logging
            sys_xct_section_t& sxs) {
    w_assert1 (g_xct()->is_single_log_sys_xct());
    w_assert1 (page.latch_mode() == LATCH_EX);
    w_assert1 (foster_p.latch_mode() == LATCH_EX);
    w_assert1 (page.get_foster() == foster_p.pid().page);
    caller_commit = true;

    if (move_count == 0) {
        // this means no-record rebalance, which is easier.
        W_DO (_ux_rebalance_foster_norec(page, foster_p, mid_key));
        // No record movement occurred, tell caller to commit the single log system transaction
        caller_commit = true;
        return RCOK;
    }

    // foster-parent should be written later because it's the data source
    // first, mark both dirty.
    page.set_dirty();
    foster_p.set_dirty();
    if (true == restart_m::use_redo_full_logging_restart())
    {
        // If page driven REDO recovery, the recovery operation cannot
        // obey WOD for b-tree rebalance operation, disable minimal logging and
        // use full logging for all the record movements
        // the system transaction log records are used to set page fence keys only,
        // the following log records record the the actual record movements
        // No dependency on WOD (Write Order Dependency) in this case
        caller_commit = false;        
    }
    else
    {
        bool registered = smlevel_0::bf->register_write_order_dependency(page._pp, foster_p._pp);
        if (!registered) {
            // TODO in this case we should do full logging.
            DBGOUT1 (<< "oops, couldn't force write order dependency in rebalance. this should be treated with care");

////////////////////////////////////////
// TODO(Restart)... NYI
// This is an existing issue from Single-Page-Recovery, when a WOD cannot be generated during the execution
// it has to use either 1) full logging or 2) flush the page to persistent device first
// but the current code continue the execution although Write-Order-Dependency was not followed.
// This scenario is not so difficult to encounter, at least one of the test cases in test_restart triggers this error.
// Comment out the fatal error for now since we have not implement the proper solution yet
// 
// Possible solution:
//    1. Add an extra field in the 'page rebalance log record' to indicate whether minimal or full logging for the associated page rebalance.
//    2. If 'Write-Order-Dependency' cannot be followed, turn the full logging flag on in the log record, and use full logging for the operation
//    3. If 'Write-Order-Dependency' can be followed, turn the full logging flag off in the log record, and use minimal logging for the operation
//    4. In REDO and UNDO, check the flag in log record to determine whether use minimal or full logging for the REDO/UNDO operation.
//    5. Once this is implemented, the system can use either minimal or full logging to handle each page rebalance operation on demand,
//        no need to force and limit to use one method for the entire system up time.
//
// Current situation:
//   Both minimal and full logging are implemented (still need more stabilization work, especially full logging), but controlled by
//   restart flags, in other words, we must pick one methond for the entire system up time, including before and after system crash.
//   Also full logging flag is used in M2 test cases only.
////////////////////////////////////////


//            W_FATAL_MSG(fcINTERNAL, << "oops, couldn't force write order dependency in rebalance, full loggig required: NYI");
        }
        // this can't cause cycle as it's always right-to-left depdencency.
    }

    // TODO(Restart)... see the same fence key setting code in btree_impl::_ux_rebalance_foster_apply
    // the assumption is the fence keys in destination page has been set up already    
    w_keystr_t high_key, chain_high_key;
    foster_p.copy_fence_high_key(high_key);          // High (foster) is from destination page, confusing naming, we are assuming the 
                                                     // destination (foster child) page has the proper fence and foster key setup already
    foster_p.copy_chain_fence_high_key(chain_high_key);  // Get chain high fence from destination page, all foster nodes have the same chain_high

    // Note: log_ receives pages in reverse order. "page" is the foster-child (dest),
    // "page2" is the foster-parent (src) to specify redo order.
    // fence - high (foster) key in source and low fence key in destination after rebalance
    // high - high (foster) key in destination after rebalance
    // chain_high - high fence key for all foster nodes
    // In a rebalance case, there are changes in the pid0 and pid_emlsn of the destination page (foster child, non-leaf)
    //    the new information are in the log record.
    // No changes in the foster page id and foster emlsn of the destination page (foster child)
    //    because the assumption is that all these information in foster child page were already setup

    DBGOUT3( << "Generate foster_rebalance log record, fence:: " << mid_key << ", high: " << high_key << ", chain: " << chain_high_key);
    rc_t ret = log_btree_foster_rebalance(foster_p /*page, destination*/, page /*page2, source*/,
                                     move_count, mid_key /* fence*/, new_pid0, new_pid0_emlsn,
                                     high_key /*high*/, chain_high_key /*chain_high*/);
    if (ret.is_error())
        return ret;
    if (true == restart_m::use_redo_full_logging_restart())
    {
        // TODO(Restart)... If we need to move records for rebalance,
        // commit the current single log system transaction before the 
        // actual record movements
        // By doing so, the actual logging and record movements are outside of 
        // the single log system transaction
        // If system failure occurrs during record movements, the standard recovery
        // would handle the REDO/UNDO for each record movement, the system 
        // transaction REDO does not handle record movements
        // this behavior is due to the fact that we are using page driven REDO with Single-Page-Recovery,
        // while the WOD cannot be followed   
        caller_commit = false;        
        W_DO (sxs.end_sys_xct (ret));
    }
    else
    {
        // Not full logging, caller should commit the current system transaction
        caller_commit = true;            
    }
    // Record movements
    W_DO (_ux_rebalance_foster_apply(page, foster_p, move_count, mid_key, new_pid0,
                                     new_pid0_emlsn, restart_m::use_redo_full_logging_restart()));

    return RCOK;
}

rc_t btree_impl::_ux_rebalance_foster_apply(
    btree_page_h &page,             // Source, foster parent page before rebalance
    btree_page_h &foster_p,         // Destination, foster child page before rebalance, 
                                    // it does not have to be an empty page if the operation is for load balance
                                    // among two pages, although the current code is used only for foster 
                                    // child creation, so the destination is always empty in this case
    int32_t move_count, 
    const w_keystr_t &mid_key,      // New fence key
    shpid_t new_pid0,               // Non-leaf node only
    lsn_t new_pid0_emlsn,           // Non-leaf node only
    const bool full_logging)        // true if full logging, generate log records for all record movements
{
    // we are moving significant fraction of records in the pages,
    // so the move will most likely change the fence keys in the middle, thus prefix too.
    // it will not be a simple move, so we have to make the page image from scratch.
    // this is also useful as defragmentation.

    // scratch block of foster_p
    generic_page scratch;
    ::memcpy (&scratch, foster_p._pp, sizeof(scratch));  // scratch is copied from the foster child page, destination
    btree_page_h scratch_p;
    scratch_p.fix_nonbufferpool_page(&scratch);

    w_keystr_t high_key, chain_high_key;
    // The following fence keys are taken from destination page,
    // the assumption is the fence keys in destination page has been set up already
    scratch_p.copy_fence_high_key(high_key);             // High is from destination page, confusing naming, 
                                                         // this is actually the fence key, it should contains valid
                                                         // information only if the destination page has a foster child
    scratch_p.copy_chain_fence_high_key(chain_high_key); // High chain fence is from destination page

    btrec_t lowest (page, page.nrecs() - move_count);
    if (foster_p.is_node()) {  // Non-leaf page
        // The foster_p usually has pid0, which we should keep. However, if the page is
        // totally empty right after norec_split, it doesn't have pid0.
        // Otherwise, we also steal old page's pid0 with low-fence key as a regular record
        w_assert1(foster_p.nrecs() == 0 || foster_p.pid0() != 0);
        bool steal_scratch_pid0 = foster_p.pid0() != 0;
        W_DO(foster_p.format_steal(scratch_p.lsn(),
            scratch_p.pid(),        // destination (foster child page) pid
            scratch_p.btree_root(),
            scratch_p.level(),      // destination (foster child page) is the new page
            new_pid0, new_pid0_emlsn,
            scratch_p.get_foster(),        // Destination's foster page id (if exists)
            scratch_p.get_foster_emlsn(),  // Destination's foster page emlsn (if exists)
            mid_key,                       // low fence key of the destination
            high_key,                      // high key of the destination, confusing naming, this is actually the foster key
                                           // this is the existing high key in destination page, it should be NULL unless it has a chain
            chain_high_key,                // high fence key of the foster chain
            false, // don't log the page_img_format record
            &page, page.nrecs() - move_count + 1, page.nrecs(), // steal from source (foster-parent (+1 to skip lowest record)) into destination
            &scratch_p, 0, scratch_p.nrecs(), // steal from the pre-balance destination page, get everything into destination
            steal_scratch_pid0,
            full_logging,                     // True if doing full logging for record movement, 
            true                              // log_src_1: log the movement from foster parent (page) to foster
                                              // child (scratch_p), which is src 1
        ));
    } else {  // Leaf page
        W_DO(foster_p.format_steal(scratch_p.lsn(),
            scratch_p.pid(),          // destination (foster child page) pid
            scratch_p.btree_root(),
            scratch_p.level(),        // destination (foster child page) is the new page
            0, lsn_t::null,           // Not needed for leaf page
            scratch_p.get_foster(),        // Destination's foster page id (if exists)
            scratch_p.get_foster_emlsn(),  // Destination's foster page emlsn (if exists)
            mid_key,                       // low fence key of the destination
            high_key,                      // high key of the destination, confusing nameing, this is actually the foster key
                                           // this is the existing high key in destination page, it should be NULL unless it has a chain
            chain_high_key,                // high fence key of the foster chain
            false, // don't log the page_img_format record
            &page, page.nrecs() - move_count, page.nrecs(), // steal from source (foster-parent) into destination
            &scratch_p, 0, scratch_p.nrecs(), // steal from pre-balance destination page, get everything into destination
            false,                            // steal_src2_pid0
            full_logging,                     // True if doing full logging for record movemen
            true                              // log_src_1: log the movement from foster parent (page) to foster
                                              // child (scratch_p), which is src 1
        ));
    }

    // next, also scratch and build foster-parent (source)
    ::memcpy (&scratch, page._pp, sizeof(scratch));  // scratch is copied from foster parent page (source)
    w_keystr_t low_key;
    scratch_p.copy_fence_low_key(low_key);                // No change
    W_DO(page.format_steal(scratch_p.lsn(),
             scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(),   // source (foster parent) is the new page
             scratch_p.pid0(), scratch_p.get_pid0_emlsn(),
             scratch_p.get_foster(), scratch_p.get_foster_emlsn(),  // No change in foster relationship
             low_key,        // low fence is the existing one
             mid_key,        // high key, confusing naming, this is actually the foster key, now is the low fence key of the destination page
             chain_high_key, // high fence key of the foster chain, it is the existing one because we have the same chain_high for all foster pages
             false, // don't log the page_img_format record
             &scratch_p, 0, scratch_p.nrecs() - move_count, // steal from old page, only need to records which were not moved
             NULL,         // steal_src2
             0,            // steal_from2
             0,            // steal_to2
             false,        // steal_src2_pid0
             false,        // full_logging = false, do not log record movement because they were logged 
                           //when constructing the foster child page previously
             false         // false so do not log movement for log_src_1, and there is nothing to log for log_src_2
             ));

    w_assert3(page.is_consistent(true, true));
    w_assert3(foster_p.is_consistent(true, true));

    return RCOK;
}

rc_t btree_impl::_ux_rebalance_foster_norec(btree_page_h &page,
            btree_page_h &foster_p, const w_keystr_t &mid_key) {
    w_assert1 (g_xct()->is_single_log_sys_xct());
    w_assert1 (page.latch_mode() == LATCH_EX);
    w_assert1 (foster_p.latch_mode() == LATCH_EX);
    w_assert1 (page.get_foster() == foster_p.pid().page);
    w_assert1 (foster_p.nrecs() == 0); // this should happen only during page split.
    int compared = page.compare_with_fence_high(mid_key);
    if (compared == 0) {
        return RCOK; // then really no change.
    }
    w_assert1(compared < 0); // because foster parent should be giving, not receiving.

    W_DO(log_btree_foster_rebalance_norec(page, foster_p, mid_key));

    w_keystr_t chain_high;
    foster_p.copy_chain_fence_high_key(chain_high);

    // Update foster parent.
    W_DO(page.norecord_split(foster_p.pid().page, foster_p.lsn(), mid_key, chain_high));

    // Update foster child. It should be an empty page so far
    w_keystr_t high;
    foster_p.copy_fence_high_key(high);
    w_keystr_len_t child_prefix_len = mid_key.common_leading_bytes(high);
    W_DO(foster_p.replace_fence_rec_nolog_may_defrag(
        mid_key, high, chain_high, child_prefix_len));

    return RCOK;
}

rc_t btree_impl::_sx_merge_foster(btree_page_h &page)
{
    FUNC(btree_impl::_sx_merge_foster);
    w_assert1 (page.latch_mode() == LATCH_EX);
    if (page.get_foster() == 0) {
        return RCOK; // nothing to rebalance
    }

    btree_page_h foster_p;  // source
    W_DO(foster_p.fix_nonroot(page, page.vol(), page.get_foster_opaqueptr(), LATCH_EX));

    // assure foster-child page has an entry same as fence-low for locking correctness.
    // See jira ticket:84 "Key Range Locking" (originally trac ticket:86).
    W_DO(_ux_assure_fence_low_entry(foster_p)); // This might be one SSX.

    // caller_commit == true if caller commit the single log system transaction
    // caller_commit == false if we are doing full logging for rebalance, the single log system transaction
    // will be committed by callee before it starts full logging
    bool caller_commit = false;

    // another SSX for merging itself
    sys_xct_section_t sxs(true);
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_merge_foster_core(page, foster_p, caller_commit, sxs);
    // Commit the single log system transaction
    if (true == caller_commit)
        W_DO (sxs.end_sys_xct (ret));

    if ((true == restart_m::use_redo_full_logging_restart()) && (false == caller_commit))
    {
        // Merge is a system transaction with single log normally,
        // but if we are doing full logging due to page driven REDO, 
        // then a lot of log records have been generated (not a single log anymore)
        //
        // Not forcing a log flush because flushing should not impact recovery operation
        //W_COERCE( smlevel_0::log->flush_all() );        
        
    }
    return ret;
}

/** this function conservatively estimates.*/
smsize_t estimate_required_space_to_merge (btree_page_h &page, btree_page_h &merged)
{
    smsize_t ret = merged.used_space();

    // we need to replace fence-high key too. (fence-low will be same)
    ret += merged.get_fence_high_length() - page.get_fence_low_length();

    // if there is no more sibling on the right, we will delete chain-fence-high
    if (merged.get_chain_fence_high_length() == 0) {
        ret -= page.get_chain_fence_high_length();
    }
    
    // prefix length might be shorter, resulting in larger record size
    size_t prefix_len_after_merge = w_keystr_t::common_leading_bytes(
        (const unsigned char*) page.get_prefix_key(), page.get_prefix_length(),
        (const unsigned char*) merged.get_prefix_key(), merged.get_prefix_length()
    );
    ret += page.nrecs() * (page.get_prefix_length() - prefix_len_after_merge);
    ret += merged.nrecs() * (merged.get_prefix_length() - prefix_len_after_merge);
    ret += (page.nrecs() + merged.nrecs()) * (8 - 1); // worst-case of alignment
    
    return ret;
}

rc_t btree_impl::_ux_merge_foster_core(btree_page_h &page, btree_page_h &foster_p, 
                                            bool &caller_commit, sys_xct_section_t& sxs) {
    w_assert1 (xct()->is_single_log_sys_xct());
    w_assert1 (page.latch_mode() == LATCH_EX);
    w_assert1 (foster_p.latch_mode() == LATCH_EX);
    caller_commit = true;

    // can we fully absorb it?
    size_t additional = estimate_required_space_to_merge(page, foster_p);
    if (page.usable_space() < additional) {
        // No record movement occurred, tell caller to commit the single log system transaction
        caller_commit = true;
        return RCOK; // don't do it
    }
    
    // foster-child should be written later because it's the data source
    // first, mark them dirty.
    page.set_dirty();
    foster_p.set_dirty();
    if (true == restart_m::use_redo_full_logging_restart())
    {
        // If page driven REDO during recovery, the recovery operation cannot
        // obey WOD for b-tree merge, disable minimal logging and
        // use full logging for all the record movements
        // the system transactions are used to set page fence keys only the following
        // log records record the actual record movements
        // No dependency on WOD (Write Order Dependency) in this case        
        caller_commit = false;        
    }
    else
    {
        bool registered = smlevel_0::bf->register_write_order_dependency(foster_p._pp, page._pp);
        if (!registered) {
            // this means the merging will cause a cycle in write-order.
            // so, let's not do the merging now.
            // see ticket:39 for more details (jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39))

            caller_commit = true;
            return RCOK;
        }
    }

    // TODO(Restart)... see the same fence key setting code in btree_impl::_ux_merge_foster_apply_parent
    w_keystr_t high_key, chain_high_key;
    if (foster_p.get_foster() != 0) 
    {
        foster_p.copy_fence_high_key(high_key);          // high key (the new foster in destination) is the high from source
                                                         //confusing naming, it is actually the foster key
    
        // Source has a foster, after merge we still have a foster, use the same chain_high
        page.copy_chain_fence_high_key(chain_high_key);  // foster chain, get the chain high fence from destination
                                                         // because all foster nodes have the same chain high
    }
    else
    {
        page.copy_chain_fence_high_key(high_key);        // no foster after merge, high key is the same as chain high

        // Source does not have a foster, after merging we do not have foster, chain-high will disappear    
        chain_high_key.clear();   // if no foster chain pre-merge, then no more foster after merge
    }

    // High -new high (foster) key in destination after merge
    // Chain_high - new chain_high key in destination after merge
    // In a merge case, no change in the pid0 and pid_emlsn of the destination page (foster parent, non-leaf)
    // Changes in the foster page id and foster emlsn of the destination page (foster parent)
    //    the new information are taken from the source (foster child page) and stored the log record.    
    rc_t ret = log_btree_foster_merge (page /*destination*/, foster_p /*source*/, 
                                       high_key /*high*/, chain_high_key/*chain_high*/,
                                       foster_p.get_foster() /* foster pid*/,
                                       foster_p.get_foster_emlsn() /* foster emlsn*/);
    if (ret.is_error())
        return ret;
    if (true == restart_m::use_redo_full_logging_restart())
    {
        // TODO(Restart)... If we need to move records for merge, commit 
        // the current single log system transaction before the actual record 
        // movements
        // By doing so, the actual logging and record movements are outside of 
        // single log system transaction
        // If system failure occurred during record movements, the standard recovery
        // would handle the REDO/UNDO for each record movement, the system 
        // transaction REDO is no-op
        // this behavior is due to the fact that we are using page driven REDO with Single-Page-Recovery,
        // while the WOD cannot be followed
        caller_commit = false;        
        W_DO (sxs.end_sys_xct (ret));
    }

    // Move the records now
    _ux_merge_foster_apply_parent(page, foster_p, restart_m::use_redo_full_logging_restart());
    W_COERCE(foster_p.set_to_be_deleted(false));
    return RCOK;
}

void btree_impl::_ux_merge_foster_apply_parent(
                      btree_page_h &page,         // destination, foster parent page before merge
                      btree_page_h &foster_p,     // source, foster child page before merge
                      const bool full_logging)    // In: true if full logging, generate log records for all record movements
{
    if (false == full_logging)
    {
        // If doing full logging, the logging and record movements are outside
        // of single log system transaction
        w_assert1 (g_xct()->is_single_log_sys_xct());
    }

    // like split, use scratch block to cleanly make a new page image
    w_keystr_t low_key, high_key, chain_high_key; // fence keys after merging
    page.copy_fence_low_key(low_key);             // low fence is from the destination page, no change
    if (foster_p.get_foster() != 0) {
        // Foster chain pre-merge, continue having foster after merge
        foster_p.copy_fence_high_key(high_key);          // high key is the high from source, confusing naming, it is actually the foster key
        page.copy_chain_fence_high_key(chain_high_key);  // get the chain high fence from destination
                                                         // all foster nodes have the same chain high
    }
    else
    {
        // No foster chain pre-merge, no more foster after merge
        page.copy_chain_fence_high_key(high_key);        // no foster after merge, high key is the same as chain_high
        chain_high_key.clear();                          // no foster after merging, chain-high will disappear        
    }
    generic_page scratch;
    ::memcpy (&scratch, page._pp, sizeof(scratch));  // scratch is copied from the destination (foster parent page)
    btree_page_h scratch_p;
    scratch_p.fix_nonbufferpool_page(&scratch);
    W_COERCE(page.format_steal(scratch_p.lsn(), scratch_p.pid(), scratch_p.btree_root(),    // destination (foster parent page) is the new page
                               scratch_p.level(),
                               scratch_p.pid0(), scratch_p.get_pid0_emlsn(),  // Non-leaf only
                               // foster-child's foster will be the next one after merge
                               foster_p.get_foster(), foster_p.get_foster_emlsn(),
                               low_key,              // low fence key from destination page
                               high_key,             // high key, confusing naming, it is actually the foster key
                                                     // it is from the source page
                               chain_high_key,       // high fence of the foster chain
                               false, // don't log the page_img_format record
                               &scratch_p, 0, scratch_p.nrecs(), // steal everything from destination (foster-parent) first
                               &foster_p, 0, foster_p.nrecs(),   // steal everything from source (foster-child) next
                               false,                            // steal_src2_pid0
                               full_logging,                     // True if doing full logging for record movement
                               false                             // log_src_1: log the movement from  source (foster child, foster_p)
                                                                 //to source (foster parent, scratch_p), which is src 2
                 ));
    w_assert3(page.is_consistent(true, true));
    w_assert1(page.is_fixed());
}

rc_t btree_impl::_sx_deadopt_foster(btree_page_h &real_parent, slotid_t foster_parent_slot)
{
    FUNC(btree_impl::_sx_deadopt_foster);
    sys_xct_section_t sxs(true);
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_deadopt_foster_core(real_parent, foster_parent_slot);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

void btree_impl::_ux_deadopt_foster_apply_real_parent(btree_page_h &real_parent,
                                                      shpid_t foster_child_id,
                                                      slotid_t foster_parent_slot) {
    w_assert1 (real_parent.latch_mode() == LATCH_EX);
    w_assert1(real_parent.is_node());
    w_assert1 (foster_parent_slot + 1 < real_parent.nrecs());
    w_assert1(btrec_t(real_parent, foster_parent_slot + 1).child() == foster_child_id);
    real_parent.remove_shift_nolog (foster_parent_slot + 1);
}

void btree_impl::_ux_deadopt_foster_apply_foster_parent(btree_page_h &foster_parent,
                                                        shpid_t foster_child_id,
                                                        lsn_t foster_child_emlsn,
                                                        const w_keystr_t &low_key,
                                                        const w_keystr_t &high_key) {
    w_assert1 (foster_parent.latch_mode() == LATCH_EX);
    w_assert1 (foster_parent.get_foster() == 0);
    w_assert1 (low_key.compare(high_key) < 0);
    w_assert1 (foster_parent.compare_with_fence_high(low_key) == 0);
    // set new chain-fence-high
    // note: we need to copy them into w_keystr_t because we are using
    // these values to modify foster_parent itself!
    w_keystr_t org_low_key, org_high_key;
    foster_parent.copy_fence_low_key(org_low_key);
    foster_parent.copy_fence_high_key(org_high_key);
    w_assert1 ((size_t)foster_parent.get_prefix_length() == org_low_key.common_leading_bytes(org_high_key));

    // high_key of adoped child is now chain-fence-high:
    W_COERCE(foster_parent.replace_fence_rec_nolog_may_defrag(
        org_low_key, org_high_key, high_key));
    
    foster_parent.page()->btree_foster = foster_child_id;
    foster_parent.set_emlsn_general(GeneralRecordIds::FOSTER_CHILD, foster_child_emlsn);
}
rc_t btree_impl::_ux_deadopt_foster_core(btree_page_h &real_parent, slotid_t foster_parent_slot)
{
    w_assert1 (xct()->is_single_log_sys_xct());
    w_assert1 (real_parent.is_node());
    w_assert1 (real_parent.latch_mode() == LATCH_EX);
    w_assert1 (foster_parent_slot + 1 < real_parent.nrecs());
    shpid_t foster_parent_pid;
    if (foster_parent_slot < 0) {
        w_assert1 (foster_parent_slot == -1);
        foster_parent_pid = real_parent.pid0_opaqueptr();
    } else {
        foster_parent_pid = real_parent.child_opaqueptr(foster_parent_slot);
    }
    btree_page_h foster_parent;
    W_DO(foster_parent.fix_nonroot(real_parent, real_parent.vol(), foster_parent_pid, LATCH_EX));
    
    if (foster_parent.get_foster() != 0) {
        // De-Adopt can't be processed when foster-parent already has foster-child. Do nothing
        // see ticket:39 (jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39))
        return RCOK; // maybe error?
    }
    
    // get low_key
    btrec_t rec (real_parent, foster_parent_slot + 1);
    const w_keystr_t &low_key = rec.key();
    shpid_t foster_child_id = rec.child();
    lsn_t foster_child_emlsn = rec.child_emlsn();

    // get high_key. if it's the last record, fence-high of real parent
    w_keystr_t high_key;
    if (foster_parent_slot + 2 < real_parent.nrecs()) {
        btrec_t next_rec (real_parent, foster_parent_slot + 2);
        high_key = next_rec.key();
    } else {
        w_assert1(foster_parent_slot + 1 == real_parent.nrecs());
        real_parent.copy_fence_high_key(high_key);
    }
    w_assert1(low_key.compare(high_key) < 0);

    W_DO(log_btree_foster_deadopt(real_parent, foster_parent, foster_child_id,
                                  foster_child_emlsn, foster_parent_slot, low_key, high_key));
    
    _ux_deadopt_foster_apply_real_parent (real_parent, foster_child_id, foster_parent_slot);
    _ux_deadopt_foster_apply_foster_parent (foster_parent,
                                foster_child_id, foster_child_emlsn, low_key, high_key);

    w_assert3(real_parent.is_consistent(true, true));
    w_assert3(foster_parent.is_consistent(true, true));
    return RCOK;
}
