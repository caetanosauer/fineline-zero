#include "btree_test_env.h"

#include <sstream>
#include <fstream>

#include "logarchiver.h"
#include "logfactory.h"

btree_test_env* test_env;
stid_t stid;
lpid_t root_pid;
char HUNDRED_BYTES[100];

// use small block to test boundaries
const size_t BLOCK_SIZE = 8192;

class ArchiverTest {
private:
    LogArchiver::ReaderThread* reader;
    AsyncRingBuffer* readbuf;
};

typedef w_rc_t rc_t;

/******************************************************************************
 * Auxiliary functions
 */

rc_t populateBtree(ss_m* ssm, test_volume_t *test_volume, int count)
{
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    std::stringstream ss("key");

    W_DO(test_env->begin_xct());
    for (int i = 0; i < count; i++) {
        ss.seekp(3);
        ss << i;
        W_DO(test_env->btree_insert(stid, ss.str().c_str(), HUNDRED_BYTES));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

rc_t generateFakeArchive(unsigned bytesPerRun, unsigned runCount,
        bool createIndex, unsigned& total)
{
    total = 0;

    LogFactory factory(true, // sorted
            1, // start with this page ID
            10, // new page ID every 10 logrecs
            1 // increment max pade ID one by one
    );
    logrec_t lr;
    unsigned runsGen = 0;

    // create LogScanner to know which logrecs are ignored
    LogScanner scanner(BLOCK_SIZE);
    LogArchiver::initLogScanner(&scanner);

    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE,
            createIndex);
    LogArchiver::BlockAssembly assemb(&dir);

    while (runsGen < runCount) {
        unsigned bytesGen = 0;
        assemb.start();
        while (bytesGen < bytesPerRun) {
            factory.next(&lr);

            if (scanner.isIgnored(lr.type())) {
                continue;
            }
            assert(lr.valid_header(lr.lsn_ck()));

            if (!assemb.add(&lr)) {
                assemb.finish(runsGen);
                assemb.start();
                assert(assemb.add(&lr));
            }

            bytesGen += lr.length();
            total++;
        }

        assemb.finish(runsGen);
        runsGen++;
    }

    assemb.shutdown();

    return RCOK;
}

rc_t emptyHeapAndCheck(LogArchiver::ArchiverHeap& heap)
{
    logrec_t* lr;
    shpid_t prevPage = (shpid_t) 0;
    lsn_t prevLSN = lsn_t::null;
    while (heap.size() > 0) {
        lr = heap.top();
        EXPECT_TRUE(lr->lsn_ck() != prevLSN);
        EXPECT_TRUE(lr->construct_pid().page >= prevPage);
        if (lr->construct_pid().page == prevPage) {
            EXPECT_TRUE(lr->lsn_ck() > prevLSN);
        }
        prevPage = lr->construct_pid().page;
        prevLSN = lr->lsn_ck();
        heap.pop();
    }

    return RCOK;
}

LogArchiver::ArchiveScanner::RunMerger*
buildRunMergerFromDirectory(LogArchiver::ArchiveDirectory& dir)
{
    std::vector<string> files;
    W_COERCE(dir.listFiles(&files));
    LogArchiver::ArchiveScanner::RunMerger* merger =
        new LogArchiver::ArchiveScanner::RunMerger();

    for (size_t i = 0; i < files.size(); i++) {
        lsn_t beginLSN = LogArchiver::ArchiveDirectory::parseLSN(
                files[i].c_str(), false);
        lsn_t endLSN = LogArchiver::ArchiveDirectory::parseLSN(
                files[i].c_str(), true);

        LogArchiver::ArchiveScanner::RunScanner* rs =
            new LogArchiver::ArchiveScanner::RunScanner(
                beginLSN, endLSN, lpid_t::null, lpid_t::null, 0, &dir);

        merger->addInput(rs);
    }

    return merger;
}

/******************************************************************************
 * TESTS
 */

rc_t consumerTest(ss_m* ssm, test_volume_t* test_vol)
{
    unsigned howManyToInsert = 1000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));

    lsn_t lastLSN = ssm->log->durable_lsn();
    lsn_t prevLSN = lsn_t(1,0);
    LogArchiver::LogConsumer cons(prevLSN, BLOCK_SIZE);
    cons.open(lastLSN);

    logrec_t* lr;
    unsigned int insertCount = 0;
    while (cons.next(lr)) {
        if (lr->type() == logrec_t::t_btree_insert ||
                lr->type() == logrec_t::t_btree_insert_nonghost)
        {
            insertCount++;
        }
        EXPECT_TRUE(lr->lsn_ck() > prevLSN);
        prevLSN = lr->lsn_ck();
    }

    EXPECT_EQ(howManyToInsert, insertCount);

    return RCOK;
}

// TODO implement heapTestFactory that uses LogFactory
rc_t heapTestReal(ss_m* ssm, test_volume_t* test_vol)
{
    unsigned howManyToInsert = 1000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));

    lsn_t lastLSN = ssm->log->durable_lsn();
    lsn_t prevLSN = lsn_t(1,0);
    LogArchiver::LogConsumer cons(prevLSN, BLOCK_SIZE);
    cons.open(lastLSN);

    LogArchiver::ArchiverHeap heap(BLOCK_SIZE);

    logrec_t* lr;
    bool pushed = false;
    while (cons.next(lr)) {
        pushed = heap.push(lr);
        if (!pushed) {
            emptyHeapAndCheck(heap);
            pushed = heap.push(lr);
            EXPECT_TRUE(pushed);
        }
    }

    emptyHeapAndCheck(heap);
    EXPECT_EQ(0, heap.size());

    return RCOK;
}

rc_t fullPipelineTest(ss_m* ssm, test_volume_t* test_vol)
{
    unsigned howManyToInsert = 1000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));

    LogArchiver::LogConsumer cons(lsn_t(1,0), BLOCK_SIZE);
    LogArchiver::ArchiverHeap heap(BLOCK_SIZE);
    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE);
    LogArchiver::BlockAssembly assemb(&dir);

    LogArchiver la(&dir, &cons, &heap, &assemb);
    la.fork();
    la.activate(lsn_t::null, true /* wait */);
    
    // by sending another activation signal with blocking,
    // we wait for logarchiver to consume up to durable LSN,
    // which is used by default when lsn_t::null is given above
    la.activate(lsn_t::null, true);

    la.start_shutdown();
    la.join();

    // TODO use archive scanner to verify:
    // 1) integrity of archive
    // 2) if no logrecs are missing (scan log with same ignores as log archiver
    // and check if each logrec is in archiver -- random access, NL join)

    return RCOK;
}

rc_t runScannerTest(ss_m* /* ssm */, test_volume_t* /* test_vol */)
{
    unsigned total = 0;
    generateFakeArchive(BLOCK_SIZE*8, 1, false, total);

    // TODO: runScanner does not work with index because index blocks are read at the end
    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE,
            false /* createIndex */);
    EXPECT_EQ(NULL, dir.getIndex());
    std::vector<string> files;
    W_DO(dir.listFiles(&files));

    EXPECT_EQ(1, files.size());

    lsn_t beginLSN = LogArchiver::ArchiveDirectory::parseLSN(files[0].c_str(),
            false);
    lsn_t endLSN = LogArchiver::ArchiveDirectory::parseLSN(files[0].c_str(),
            true);

    LogArchiver::ArchiveScanner::RunScanner rs
        (beginLSN, endLSN, lpid_t::null, lpid_t::null, 0, &dir);

    lpid_t prevPID = lpid_t::null;
    lsn_t prevLSN = lsn_t::null;
    unsigned count = 0;
    logrec_t* lr;
    while (rs.next(lr)) {
        EXPECT_TRUE(lr->valid_header(lr->lsn_ck()));
        EXPECT_TRUE(lr->construct_pid().page >= prevPID.page);
        EXPECT_TRUE(lr->lsn_ck() != prevLSN);
        EXPECT_TRUE(lr->lsn_ck() >= rs.runBegin);
        EXPECT_TRUE(lr->lsn_ck() < rs.runEnd);
        prevPID = lr->construct_pid();
        prevLSN = lr->lsn_ck();
        count++;
    }

    EXPECT_EQ(total, count);

    return RCOK;
}

rc_t runMergerSeqTest(ss_m* /* ssm */, test_volume_t* /* test_vol */)
{
    unsigned total = 0;
    generateFakeArchive(BLOCK_SIZE*8, 8, false, total);

    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE,
            false /* createIndex */);
    EXPECT_EQ(NULL, dir.getIndex());


    LogArchiver::ArchiveScanner::RunMerger* merger =
        buildRunMergerFromDirectory(dir);

    lpid_t prevPID = lpid_t::null;
    lsn_t prevLSN = lsn_t::null;
    unsigned count = 0;
    logrec_t* lr;
    while (merger->next(lr)) {
        EXPECT_TRUE(lr->valid_header(lr->lsn_ck()));
        EXPECT_TRUE(lr->construct_pid().page >= prevPID.page);
        EXPECT_TRUE(lr->lsn_ck() != prevLSN);
        prevPID = lr->construct_pid();
        prevLSN = lr->lsn_ck();
        count++;
    }

    EXPECT_EQ(total, count);

    return RCOK;
}

rc_t runMergerFullTest(ss_m* ssm, test_volume_t* test_vol)
{
    unsigned howManyToInsert = 2000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));

    LogArchiver::LogConsumer cons(lsn_t(1,0), BLOCK_SIZE);
    LogArchiver::ArchiverHeap heap(BLOCK_SIZE);
    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE, false);
    LogArchiver::BlockAssembly assemb(&dir);

    LogArchiver la(&dir, &cons, &heap, &assemb);
    la.fork();
    la.activate(lsn_t::null, true /* wait */);
    // wait for logarchiver to consume up to durable LSN,
    la.activate(lsn_t::null, true);

    LogArchiver::ArchiveScanner::RunMerger* merger =
        buildRunMergerFromDirectory(dir);

    lpid_t prevPID = lpid_t::null;
    lsn_t prevLSN = lsn_t::null;
    logrec_t* lr;
    while (merger->next(lr)) {
        EXPECT_TRUE(lr->valid_header(lr->lsn_ck()));
        EXPECT_TRUE(lr->construct_pid().page >= prevPID.page);
        EXPECT_TRUE(lr->lsn_ck() != prevLSN);
        prevPID = lr->construct_pid();
        prevLSN = lr->lsn_ck();
    }

    la.start_shutdown();
    la.join();

    return RCOK;
}


#define DEFAULT_TEST(test, function) \
    TEST (test, function) { \
        test_env->empty_logdata_dir(); \
        EXPECT_EQ(test_env->runBtreeTest(function), 0); \
    }

DEFAULT_TEST (LogArchiverTest, consumerTest);
DEFAULT_TEST (LogArchiverTest, heapTestReal);
DEFAULT_TEST (LogArchiverTest, fullPipelineTest);
DEFAULT_TEST (ArchiveScannerTest, runScannerTest);
DEFAULT_TEST (ArchiveScannerTest, runMergerSeqTest);
DEFAULT_TEST (ArchiveScannerTest, runMergerFullTest);

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
