#include "verifylog.h"

#include <fstream>
#include "logarchiver.h"
#include "alloc_cache.h"
#include "bf_tree.h"

void VerifyLog::setupOptions()
{
    LogScannerCommand::setupOptions();
    boost::program_options::options_description opt("VerifyLog Options");
    opt.add_options()
        ("alloc", po::value<bool>(&verify_alloc)->default_value(false)->
         implicit_value(true), "Verify allocation of pages")
        ("dbfile,d", po::value<string>(&dbfile)->default_value("db"),
            "Path to DB file")
    ;
    options.add(opt);
}

void init_alloc()
{
    // _options.set_string_option("sm_dbfile", dbfile);
    // _options.set_bool_option("sm_vol_cluster_stores", true);

    // smlevel_0::vol = new vol_t(options);
    // smlevel_0::bf = new bf_tree_m(options);
    // smlevel_0::vol->build_caches(format, chkpt_info);
}

void VerifyLog::run()
{
    if (verify_alloc) { init_alloc(); }

    BaseScanner* s = getScanner();
    VerifyHandler h(merge);
    if (!merge) {
        s->openFileCallback = std::bind(&VerifyHandler::newFile, &h,
            std::placeholders::_1);
    }
    s->add_handler(&h);
    s->fork();
    s->join();

    delete s;
}

VerifyHandler::VerifyHandler(bool merge)
    : minLSN(lsn_t::null), maxLSN(lsn_t::null), lastVersion(0),
    lastPID(0), count(0), merge(merge)
{
}

void VerifyHandler::newFile(const char* /*fname*/)
{
    // minLSN = LogArchiver::ArchiveDirectory::parseLSN(fname, false);
    // maxLSN = LogArchiver::ArchiveDirectory::parseLSN(fname, true);
    lastVersion = 0;
    lastPID = 0;
}

lsn_t VerifyHandler::getCurrentPageLSN(PageID pid)
{
    auto it = pageLSNs.find(pid);
    if (it == pageLSNs.end()) {
        return pageLSNs[pid] = lsn_t::null;
    }
    else {
        return pageLSNs[pid];
    }
}

void checkLSN(lsn_t lsn, lsn_t current, lsn_t expected)
{
    if (current.is_null()) {
        return;
    }
    if(expected != current) {
        std::cout << "on " << lsn
            << " current is " << current
            << " but should be " << expected
        << std::endl;
    }
    w_assert0(expected == current);
}

void VerifyHandler::checkAlloc(logrec_t& r)
{
    // auto lsn = r.lsn();
    // CS TODO FINELINE: pass lsn from log consumer into handlers
    lsn_t lsn = lsn_t::null;
    PageID pid = *((PageID*) (r.data()));
    if (r.type() == enum_to_base(LogRecordType::alloc_page_log)) {
        if (allocatedPages.find(pid) != allocatedPages.end()) {
            std::cout << "on " << lsn
                << " alloc_page of pid " << pid
                << " which is already allocated" << std::endl;
            w_assert0(false);
        }
        allocatedPages.insert(pid);
    }
    else if (r.type() == enum_to_base(LogRecordType::dealloc_page_log)) {
        if (allocatedPages.find(pid) == allocatedPages.end()) {
            std::cout << "on " << lsn
                << " dealloc_page of pid " << pid
                << " which is not allocated" << std::endl;
            w_assert0(false);
        }
        allocatedPages.erase(pid);
    }
    else {
        std::cout << "on " << lsn
            << " update on alloc pid " << r.pid()
            << " but invalid logrec type " << ZeroLogInterface::getTypeString(r.type())
            << std::endl;
        w_assert0(false);
    }
}

void VerifyHandler::invoke(logrec_t& r)
{
    w_assert0(r.valid_header());

    auto version = r.page_version();
    PageID pid = r.pid();

    if (r.is_redo()) {
        if (alloc_cache_t::is_alloc_pid(pid)) {
            // checkAlloc(r);
        }
    }

    if (merge) {
        w_assert0(pid >= lastPID);
        if (pid == lastPID) {
            w_assert0(version > lastVersion);
        }
        // w_assert0(merge || lsn >= minLSN);
        // w_assert0(merge || lsn <= maxLSN);
    }

    lastVersion = version;
    lastPID = pid;

    count++;
}

void VerifyHandler::finalize()
{
    cout << "Log verification complete!" << endl;
    cout << "scanned_logrecs " << count << endl;
}
