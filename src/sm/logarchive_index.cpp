#include "logarchive_index.h"

#include <boost/regex.hpp>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sstream>

#include "w_debug.h"
#include "lsn.h"
#include "latches.h"
#include "sm_base.h"
#include "log_core.h"
#include "stopwatch.h"
#include "worker_thread.h"
#include "restart.h"
#include "bf_tree.h"
#include "xct_logger.h"

class RunRecycler : public worker_thread_t
{
public:
    RunRecycler(unsigned replFactor, ArchiveIndex* archIndex)
        : archIndex(archIndex), replFactor(replFactor)
    {}

    virtual void do_work()
    {
        archIndex->deleteRuns(replFactor);
    }

    ArchiveIndex* archIndex;
    const unsigned replFactor;
};

// definition of static members
const string ArchiveIndex::RUN_PREFIX = "archive_";
const string ArchiveIndex::CURR_RUN_PREFIX = "current_run_";
const string ArchiveIndex::run_regex =
    "^archive_([1-9][0-9]*)_([1-9][0-9]*)-([1-9][0-9]*)$";
const string ArchiveIndex::current_regex = "^current_run_[1-9][0-9]*$";

struct RunFooter {
    uint64_t index_begin;
    uint64_t index_size;
    PageID maxPID;
};

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

bool ArchiveIndex::parseRunFileName(string fname, RunId& fstats)
{
    boost::regex run_rx(run_regex, boost::regex::perl);
    boost::smatch res;
    if (!boost::regex_match(fname, res, run_rx)) { return false; }

    fstats.level = std::stoi(res[1]);

    std::stringstream is;
    is.str(res[2]);
    is >> fstats.begin;
    is.clear();
    is.str(res[3]);
    is >> fstats.end;

    return true;
}

size_t ArchiveIndex::getFileSize(int fd)
{
    struct stat stat;
    auto ret = ::fstat(fd, &stat);
    CHECK_ERRNO(ret);
    return stat.st_size;
}

ArchiveIndex::ArchiveIndex(const sm_options& options)
{
    archdir = options.get_string_option("sm_archdir", "archive");

    bool reformat = options.get_bool_option("sm_format", false);
    directIO = options.get_bool_option("sm_arch_o_direct", false);
    _max_open_files = options.get_int_option("sm_arch_max_open_files", 20);

    if (archdir.empty()) {
        W_FATAL_MSG(fcINTERNAL,
                << "Option for archive directory must be specified");
    }

    if (!fs::exists(archdir)) {
        if (reformat) {
            fs::create_directories(archdir);
        } else {
            cerr << "Error: could not open the log directory " << archdir <<endl;
            W_COERCE(RC(eOS));
        }
    }

    maxLevel = 0;
    archpath = archdir;
    fs::directory_iterator it(archpath), eod;
    boost::regex current_rx(current_regex, boost::regex::perl);

    // create/load index
    unsigned runsFound = 0;
    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();
        RunId fstats;

        if (parseRunFileName(fname, fstats)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }

            auto runFile = openForScan(fstats);
            loadRunInfo(runFile, fstats);
            closeScan(fstats);

            if (fstats.level > maxLevel) { maxLevel = fstats.level; }

            runsFound++;
        }
        else if (boost::regex_match(fname, current_rx)) {
            DBGTHRD(<< "Found unfinished log archive run. Deleting");
            fs::remove(fpath);
        }
        else {
            cerr << "ArchiveIndex cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }
    }

    for (unsigned l = 0; l < runs.size(); l++) {
        std::sort(runs[l].begin(), runs[l].end());
    }

    // no runs found in archive log -- start from first available log file
    if (runsFound == 0) {
        std::vector<partition_number_t> partitions;
        if (smlevel_0::log) {
            smlevel_0::log->get_storage()->list_partitions(partitions);
        }

        if (partitions.size() > 0) {
            auto nextPartition = partitions[0];
            if (nextPartition > 1) {
                // create empty run to fill in the missing gap
                openNewRun(1);
                auto lastRun = nextPartition - 1;
                closeCurrentRun(lastRun, 1);
                RunId fstats = {1, lastRun, 1};
                auto runFile = openForScan(fstats);
                loadRunInfo(runFile, fstats);
                closeScan(fstats);
            }
        }
    }

    unsigned replFactor = options.get_int_option("sm_archiver_replication_factor", 0);
    if (replFactor > 0) {
        // CS TODO -- not implemented, see comments on deleteRuns
        // runRecycler.reset(new RunRecycler {replFactor, this});
        // runRecycler->fork();
    }
}

ArchiveIndex::~ArchiveIndex()
{
    if (runRecycler) { runRecycler->stop(); }
}

void ArchiveIndex::listFiles(std::vector<std::string>& list, int level)
{
    list.clear();

    // CS TODO unify with listFileStats
    fs::directory_iterator it(archpath), eod;
    for (; it != eod; it++) {
        string fname = it->path().filename().string();
        RunId fstats;
        if (parseRunFileName(fname, fstats)) {
            if (level < 0 || level == static_cast<int>(fstats.level)) {
                list.push_back(fname);
            }
        }
    }
}

void ArchiveIndex::listFileStats(list<RunId>& list, int level)
{
    list.clear();
    if (level > static_cast<int>(getMaxLevel())) { return; }

    vector<string> fnames;
    listFiles(fnames, level);

    RunId stats;
    for (size_t i = 0; i < fnames.size(); i++) {
        parseRunFileName(fnames[i], stats);
        list.push_back(stats);
    }
}

/**
 * Opens a new run file of the log archive, closing the current run
 * if it exists. Upon closing, the file is renamed to contain the LSN
 * range of the log records contained in that run. The upper boundary
 * (lastLSN) is exclusive, meaning that it will be found on the beginning
 * of the following run. This also allows checking the filenames for any
 * any range of the LSNs which was "lost" when archiving.
 *
 * We assume the rename operation is atomic, even in case of OS crashes.
 *
 */
rc_t ArchiveIndex::openNewRun(unsigned level)
{
    int flags = O_WRONLY | O_CREAT;
    std::string fname = make_current_run_path(level).string();
    auto fd = ::open(fname.c_str(), flags, 0744 /*mode*/);
    CHECK_ERRNO(fd);
    DBGTHRD(<< "Opened new output run in level " << level);


    {
        spinlock_write_critical_section cs(&_mutex);

        appendFd.resize(level+1, -1);
        appendFd[level] = fd;
        appendPos.resize(level+1, 0);
        appendPos[level] = 0;
    }

    return RCOK;
}

fs::path ArchiveIndex::make_run_path(run_number_t begin, run_number_t end, unsigned level)
    const
{
    return archpath / fs::path(RUN_PREFIX + std::to_string(level) + "_" + std::to_string(begin)
            + "-" + std::to_string(end));
}

fs::path ArchiveIndex::make_current_run_path(unsigned level) const
{
    return archpath / fs::path(CURR_RUN_PREFIX + std::to_string(level));
}

rc_t ArchiveIndex::closeCurrentRun(run_number_t currentRun, unsigned level, PageID maxPID)
{
    run_number_t lastRun = 0;
    if (level == 1) {
        // run being created by archiver -- must be the last of all levels
        lastRun = getLastRun();
    }
    else {
        lastRun = getLastRun(level);
    }

    if (appendFd[level] >= 0) {
        if (lastRun != currentRun && currentRun > 0) {
            {
                spinlock_read_critical_section cs(&_mutex);
                // register index information and write it on end of file
                if (appendPos[level] > 0) {
                    // take into account space for skip log record
                    appendPos[level] += logrec_t::get_skip_log().length();
                }
            }

            finishRun(lastRun+1, currentRun, maxPID, appendFd[level], appendPos[level], level);
            fs::path new_path = make_run_path(lastRun+1, currentRun, level);
            fs::rename(make_current_run_path(level), new_path);

            DBGTHRD(<< "Closing current output run: " << new_path.string());
        }

        auto ret = ::fsync(appendFd[level]);
        CHECK_ERRNO(ret);

        ret = ::close(appendFd[level]);
        CHECK_ERRNO(ret);
        appendFd[level] = -1;

        // This step atomically "commits" the creation of the new run
        if (currentRun > 0) {
            spinlock_write_critical_section cs(&_open_file_mutex);

            lastFinished[level]++;

            // Notify other services that depend on archived LSN
            if (level == 1) {
                if (smlevel_0::bf) {
                    smlevel_0::bf->notify_archived_run(currentRun);
                }
            }
        }
    }

    openNewRun(level);

    return RCOK;
}

void ArchiveIndex::append(char* data, size_t length, unsigned level)
{
    // Precondition: there is always space for a skip log record at the end (see BlockAssembly::spaceToReserve)
    memcpy(data + length, &logrec_t::get_skip_log(), logrec_t::get_skip_log().length());

    // beginning of block must be a valid log record
    w_assert1(reinterpret_cast<logrec_t*>(data)->valid_header());

    INC_TSTAT(la_block_writes);
    auto ret = ::pwrite(appendFd[level], data, length + logrec_t::get_skip_log().length(),
                appendPos[level]);
    CHECK_ERRNO(ret);
    appendPos[level] += length;
}

void ArchiveIndex::fsync(unsigned level)
{
    auto ret = ::fsync(appendFd[level]);
    CHECK_ERRNO(ret);
}

RunFile* ArchiveIndex::openForScan(const RunId& runid)
{
    spinlock_write_critical_section cs(&_open_file_mutex);

    auto& file = _open_files[runid];

    if (!file.data) {
        fs::path fpath = make_run_path(runid.begin, runid.end, runid.level);
        int flags = O_RDONLY;
        if (directIO) { flags |= O_DIRECT; }
        file.fd = ::open(fpath.string().c_str(), flags, 0744 /*mode*/);
        CHECK_ERRNO(file.fd);
        file.length = ArchiveIndex::getFileSize(file.fd);
        if (file.length > 0) {
            file.data = (char*) mmap(nullptr, file.length, PROT_READ, MAP_SHARED, file.fd, 0);
            CHECK_ERRNO((long) file.data);
        }
        file.refcount = 0;
        file.runid = runid;
    }

    file.refcount++;

    // Close oldest open file without references
    if (_open_files.size() > _max_open_files) {
	for (auto it = _open_files.cbegin(); it != _open_files.cend();) {
            if (it->second.refcount == 0) {
                w_assert0(it->second.data);
                auto ret = munmap(it->second.data, it->second.length);
                CHECK_ERRNO(ret);
                ret = ::close(it->second.fd);
                CHECK_ERRNO(ret);
                it = _open_files.erase(it);
                Logger::log_sys<comment_log>("closed_run " + to_string(runid.level) +
                        " " + to_string(runid.begin) + " " + to_string(runid.end));
                break;
            }
	    ++it;
	}
    }

    INC_TSTAT(la_open_count);

    return &file;
}

void ArchiveIndex::closeScan(const RunId& runid)
{
    spinlock_write_critical_section cs(&_open_file_mutex);

    auto it = _open_files.find(runid);
    w_assert1(it != _open_files.end());

    it->second.refcount--;
}

void ArchiveIndex::deleteRuns(unsigned replicationFactor)
{
    /*
     * CS TODO: deleting runs is not as trivial as I initially thought.
     * Here are some issues to be aware of:
     * - RunInfo entries in the index should be deleted first while in a
     *   critical section. The actual deletion should happen after leaving
     *   the critical section.
     * - This deletion should not destroy objects eagerly, because previous
     *   index probes might still be using the runs to be deleted. Thus, some
     *   type of reference-counting mechanism is required (e.g., shared_ptr).
     * - Deleting multiple runs cannot be done atomically, so it's an
     *   operation that must be logged and repeated during recovery in
     *   case of a crash.
     * - Not only is logging required, but we must make sure that recovery
     *   correctly covers all possible states -- any of the N runs being
     *   deleted might independently be in one of 3 states: removed from index
     *   but still in use (waiting for garbage collected), not in use and ready
     *   for (or currently undergoing) deletion, and fully deleted but run file
     *   still exists.
     * - Besides all that, it is atually easier to implement *moving* a run to
     *   a different (archival-kind-of) device than to delete it, which makes
     *   more sense in practice, so it might be worth looking into that before
     *   implementing deletion.
     *
     * Currently, runs are only deleted in ss_m::_truncate_los, which runs
     * after shutdown and thus is free of the issues above. That's also why
     * this method currently only removes files
     */
    spinlock_write_critical_section cs(&_mutex);

    if (replicationFactor == 0) { // delete all runs
        fs::directory_iterator it(archpath), eod;
        boost::regex run_rx(run_regex, boost::regex::perl);
        for (; it != eod; it++) {
            string fname = it->path().filename().string();
            if (boost::regex_match(fname, run_rx)) {
                fs::remove(it->path());
            }
        }

        return;
    }

    for (unsigned level = maxLevel; level > 0; level--) {
        if (level <= replicationFactor) {
            // there's no run with the given replication factor -- just return
            return;
        }
        for (int h = lastFinished[level]; h >= 0; h--) {
            auto& high = runs[level][h];
            unsigned levelToClean = level - replicationFactor;
            while (levelToClean > 0) {
                // delete all runs within the LSN range of the higher-level run
                for (int l = lastFinished[levelToClean]; l >= 0; l--) {
                    auto& low = runs[levelToClean][l];
                    if (low.begin >= high.begin && low.end <= high.end)
                    {
                        auto path = make_run_path(low.begin, low.end, levelToClean);
                        fs::remove(path);
                    }
                }
                levelToClean--;
            }
        }
    }
}

void ArchiveIndex::newBlock(const vector<pair<PageID, size_t> >&
        buckets, unsigned level)
{
    spinlock_write_critical_section cs(&_mutex);

    size_t prevOffset = 0;
    for (size_t i = 0; i < buckets.size(); i++) {
        BlockEntry e;
        e.pid = buckets[i].first;
        e.offset = buckets[i].second;
        w_assert1(e.offset == 0 || e.offset > prevOffset);
        prevOffset = e.offset;
        runs[level].back().entries.push_back(e);
    }
}

rc_t ArchiveIndex::finishRun(run_number_t begin, run_number_t end,
        PageID maxPID, int fd, off_t offset, unsigned level)
{
    int lf;
    {
        spinlock_write_critical_section cs(&_mutex);

        if (offset == 0) {
            // at least one entry is required for empty runs
            appendNewRun(level);
        }

        lf = lastFinished[level] + 1;
        w_assert1(lf == 0 || begin == runs[level][lf-1].end + 1);
        w_assert1(lf < (int) runs[level].size());

        runs[level][lf].begin = begin;
        runs[level][lf].end = end;
        runs[level][lf].maxPID = maxPID;
    }

    if (offset > 0 && lf < (int) runs[level].size()) {
        serializeRunInfo(runs[level][lf], fd, offset);
    }

    if (level > 1 && runRecycler) { runRecycler->wakeup(); }

    return RCOK;
}

void ArchiveIndex::serializeRunInfo(RunInfo& run, int fd, off_t offset)
{
    spinlock_read_critical_section cs(&_mutex);
    // Write whole vector at once
    auto index_size = sizeof(BlockEntry) * run.entries.size();
    auto ret = ::pwrite(fd, &run.entries[0], index_size, offset);
    CHECK_ERRNO(ret);
    // Write run footer
    RunFooter footer {offset, index_size, run.maxPID};
    ret = ::pwrite(fd, &footer, sizeof(RunFooter), offset + index_size);
}

void ArchiveIndex::appendNewRun(unsigned level)
{
    RunInfo newRun;
    if (level > maxLevel) {
        maxLevel = level;
        runs.resize(maxLevel+1);
        lastFinished.resize(maxLevel+1, -1);
    }
    runs[level].push_back(newRun);
}

void ArchiveIndex::startNewRun(unsigned level)
{
    spinlock_write_critical_section cs(&_mutex);
    appendNewRun(level);
}

run_number_t ArchiveIndex::getLastRun()
{
    spinlock_read_critical_section cs(&_mutex);

    run_number_t last = 0;

    for (unsigned l = 1; l <= maxLevel; l++) {
        if (lastFinished[l] >= 0) {
            auto& run = runs[l][lastFinished[l]];
            if (run.end > last) {
                last = run.end;
            }
        }
    }

    return last;
}

run_number_t ArchiveIndex::getLastRun(unsigned level)
{
    spinlock_read_critical_section cs(&_mutex);

    if (level > maxLevel) { return 0; }

    if (lastFinished[level] < 0) {
        // No runs exist in the given level. If a previous level exists, it
        // must be one before the first run in that level; otherwise, it's simply 1
        if (level == 0) { return 0; }
        return getFirstRun(level - 1) - 1;
    }

    return runs[level][lastFinished[level]].end;
}

run_number_t ArchiveIndex::getFirstRun(unsigned level)
{
    if (level == 0) { return 0; }
    // If no runs exist at this level, recurse down to previous level;
    if (lastFinished[level] < 0) { return getFirstRun(level-1); }

    return runs[level][0].begin;
}

void ArchiveIndex::loadRunInfo(RunFile* runFile, const RunId& fstats)
{
    RunInfo run;
    if (runFile->length > 0) {
        // Read footer from end of file
        w_assert0(runFile->length > sizeof(RunFooter));
        off_t footer_offset = runFile->length - sizeof(RunFooter);
        RunFooter footer = *(reinterpret_cast<RunFooter*>(runFile->getOffset(footer_offset)));
        run.maxPID = footer.maxPID;
        // Get offset of first index entry
        w_assert0(runFile->length > footer.index_begin);
        w_assert0(runFile->length > sizeof(RunFooter) + footer.index_size);
        off_t index_offset = footer.index_begin;
        // Initialize entry array with read size
        w_assert0(footer.index_size % sizeof(BlockEntry) == 0);
        run.entries.resize(footer.index_size / sizeof(BlockEntry));
        // Copy entries into initialized vector
        BlockEntry* entry = reinterpret_cast<BlockEntry*>(runFile->getOffset(index_offset));
        for (size_t i = 0; i < run.entries.size(); i++) {
            run.entries[i] = *entry;
            entry++;
        }
        // Assert that skip log record is right before the index
        off_t skip_offset = index_offset - logrec_t::get_skip_log().length();
        w_assert0(skip_offset < index_offset);
        w_assert0(memcmp(runFile->getOffset(skip_offset), &logrec_t::get_skip_log(), logrec_t::get_skip_log().length()) == 0);
    }

    run.begin = fstats.begin;
    run.end = fstats.end;

    if (fstats.level > maxLevel) {
        maxLevel = fstats.level;
        // level 0 reserved, so add 1
        runs.resize(maxLevel+1);
        lastFinished.resize(maxLevel+1, -1);
    }
    runs[fstats.level].push_back(run);
    lastFinished[fstats.level] = runs[fstats.level].size() - 1;
}

size_t ArchiveIndex::findRun(run_number_t run, unsigned level)
{
    // Assumption: mutex is held by caller
    if (run == 0) { return 0; }

    /*
     * CS: requests are more likely to access the last runs, so
     * we do a linear search instead of binary search.
     */
    auto& lf = lastFinished[level];

    // No runs at this level
    if (lf < 0) { return 0; }

    if(run > runs[level][lf].end) {
        return lf + 1;
    }

    int result = 0;
    while (result <= lf && run > runs[level][result].end) {
        result++;
    }

    // skip empty runs
    while (runs[level][result].entries.size() == 0 && result < lf) {
        result++;
    }

    // caller must check if returned index is valid
    return result >= 0 ? result : runs[level].size();
}

size_t ArchiveIndex::findEntry(RunInfo* run,
        PageID pid, int from, int to)
{
    // Assumption: mutex is held by caller

    if (from > to) {
        if (from == 0) {
            // Queried pid lower than first in run
            return 0;
        }
        // Queried pid is greater than last in run.  This should not happen
        // because probes must not consider this run if that's the case
        W_FATAL_MSG(fcINTERNAL, << "Invalid probe on archiver index! "
                << " PID = " << pid << " run = " << run->begin);
    }

    // negative value indicates first invocation
    if (to < 0) { to = run->entries.size() - 1; }
    if (from < 0) { from = 0; }

    w_assert1(run);
    w_assert1(run->entries.size() > 0);

    // binary search for page ID within run
    size_t i;
    if (from == to) {
        i = from;
    }
    else {
        i = from/2 + to/2;
    }

    w_assert0(i < run->entries.size());

    /* The if below is for the old index organization that used pages (as in a
     * normal B-tree) rather than buckets. In that case, we have found the pid
     * as soon as the current entry is <= and the next >= than the given pid.
     * In the bucket organization, entries never repeat the same pid, so the
     * search criteria becomes "current entry <= pid && next entry > pid"
     */
    // if (run->entries[i].pid <= pid &&
    //         (i == run->entries.size() - 1 || run->entries[i+1].pid >= pid))
    // {
    //     // found it! must first check if previous does not contain same pid
    //     while (i > 0 && run->entries[i].pid == pid)
    //             //&& run->entries[i].pid == run->entries[i-1].pid)
    //     {
    //         i--;
    //     }
    //     return i;
    // }

    if (run->entries[i].pid <= pid &&
            (i == run->entries.size() - 1 || run->entries[i+1].pid > pid))
    {
        // found it! -- previous cannot contain the same pid in bucket organization
        return i;
    }

    // not found: recurse down
    if (run->entries[i].pid > pid) {
        return findEntry(run, pid, from, i-1);
    }
    else {
        return findEntry(run, pid, i+1, to);
    }
}

void ArchiveIndex::dumpIndex(ostream& out)
{
    for (size_t l = 0; l <= maxLevel; l++) {
        for (int i = 0; i <= lastFinished[l]; i++) {
            size_t offset = 0, prevOffset = 0;
            auto& run = runs[l][i];
            out << "NEW_RUN level: " << l << " begin: " << run.begin
                << " end: " << run.end << " maxPID: " << run.maxPID
                << endl;
            // for (size_t j = 0; j < run.entries.size(); j++) {
            //     offset = run.entries[j].offset;
            //     out << "level " << l << " run " << i
            //         << " entry " << j <<
            //         " pid " << run.entries[j].pid <<
            //         " offset " << offset <<
            //         " delta " << offset - prevOffset <<
            //         endl;
            //     prevOffset = offset;
            // }
        }
    }
}

void ArchiveIndex::dumpIndex(ostream& out, const RunId& runid)
{
    size_t offset = 0, prevOffset = 0;
    auto index = findRun(runid.begin, runid.level);
    auto& run = runs[runid.level][index];
    for (size_t j = 0; j < run.entries.size(); j++) {
        offset = run.entries[j].offset;
        out << "level " << runid.level
            << " run " << index
            << " entry " << j <<
            " pid " << run.entries[j].pid <<
            " offset " << offset <<
            " delta " << offset - prevOffset <<
            endl;
        prevOffset = offset;
    }
}
