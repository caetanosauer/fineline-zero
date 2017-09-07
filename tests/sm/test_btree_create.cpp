#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btree_page_h.h"
#include "bf_tree.h"

btree_test_env *test_env;

/**
 * Unit test for creating BTree.
 */

w_rc_t create_test(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    return RCOK;
}

TEST (BtreeCreateTest, Create) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(create_test), 0);
}

w_rc_t create_test2(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    StoreID stid;
    W_DO(ssm->create_index(stid));
    W_DO(ssm->abort_xct());

    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    return RCOK;
}

TEST (BtreeCreateTest, Create2) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(create_test2), 0);
}

w_rc_t create_check(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    smlevel_0::bf->get_cleaner()->wakeup(true);
    generic_page buf;

    // CS: why reading 5 pages?? (magic number)
    for (PageID shpid = 1; shpid < 5; ++shpid) {

        cout << "checking pid " << shpid << ":";
        W_IGNORE(smlevel_0::vol->read_page(shpid, &buf));
        cout << "full-pid=" << buf.pid << ",";
        switch (buf.tag) {
            case t_bad_p: cout << "t_bad_p"; break;
            case t_alloc_p: cout << "t_alloc_p"; break;
            case t_stnode_p: cout << "t_stnode_p"; break;
            case t_btree_p: cout << "t_btree_p"; break;
            default:
                cout << "wtf?? " << buf.tag; break;
        }
        if (buf.tag == t_btree_p) {
            btree_page_h p;
            p.fix_nonbufferpool_page(&buf);
            cout << "(level=" << p.level() << ")";
        }

        cout << endl;
    }

    return RCOK;
}

TEST (BtreeCreateTest, CreateCheck) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(create_check), 0);
}

//#include <google/profiler.h>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    // even this simple test takes substantial time. might be good to tune startup/shutdown
    //::ProfilerStart("create.prof");
    int ret;
    //for (int i = 0; i < 20; ++i)
        ret = RUN_ALL_TESTS();
    //::ProfilerStop();
    return ret;
}
