#ifndef LOGARCHIVE_INDEX_H
#define LOGARCHIVE_INDEX_H

#include <vector>
#include <list>
#include <unordered_map>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "basics.h"
#include "latches.h"
#include "lsn.h"
#include "sm_options.h"

class RunRecycler;

struct RunId {
    run_number_t begin;
    run_number_t end;
    unsigned level;

    bool operator==(const RunId& other) const
    {
        return begin == other.begin && end == other.end
            && level == other.level;
    }
};

// Controls access to a single run file through mmap
struct RunFile
{
    RunId runid;
    int fd;
    int refcount;
    char* data;
    size_t length;

    RunFile() : fd(-1), refcount(0), data(nullptr), length(0)
    {
    }

    char* getOffset(off_t offset) const { return data + offset; }
};

namespace std
{
    /// Hash function for RunId objects
    /// http://stackoverflow.com/q/17016175/1268568
    template<> struct hash<RunId>
    {
        using argument_type = RunId;
        using result_type = std::size_t;
        result_type operator()(argument_type const& a) const
        {
            result_type const h1 ( std::hash<lsn_t>()(a.begin) );
            result_type const h2 ( std::hash<lsn_t>()(a.end) );
            result_type const h3 ( std::hash<unsigned>()(a.level) );
            return ((h1 ^ (h2 << 1)) >> 1) ^ (h3 << 1);
        }
    };
}

// Comparator for map of open files
struct CmpOpenFiles {
    bool operator()(const RunId& a, const RunId& b) const
    {
	if (a.level != b.level) { return a.level < b.level; }
	return a.begin < b.begin;
    }
};

/** \brief Encapsulates all file and I/O operations on the log archive
 *
 * The directory object serves the following purposes:
 * - Inspecting the existing archive files at startup in order to determine
 *   the last LSN persisted (i.e., from where to resume archiving) and to
 *   delete incomplete or already merged (TODO) files that can result from
 *   a system crash.
 * - Support run generation by providing operations to open a new run,
 *   append blocks of data to the current run, and closing the current run
 *   by renaming its file with the given LSN boundaries.
 * - Support scans by opening files given their LSN boundaries (which are
 *   determined by the archive index), reading arbitrary blocks of data
 *   from them, and closing them.
 * - In the near future, it should also support the new (i.e.,
 *   instant-restore-enabled) asynchronous merge daemon (TODO).
 * - Support auxiliary file-related operations that are used, e.g., in
 *   tests and experiments.  Currently, the only such operation is
 *   parseLSN.
 *
 * \author Caetano Sauer
 */
class ArchiveIndex {
public:
    ArchiveIndex(const sm_options& options);
    virtual ~ArchiveIndex();

    struct BlockEntry {
        size_t offset;
        PageID pid;
    };

    struct RunInfo {
        run_number_t begin;
        run_number_t end;

        // Used as a filter to avoid unneccessary probes on older runs
        PageID maxPID;

        std::vector<BlockEntry> entries;

        bool operator<(const RunInfo& other) const
        {
            return begin < other.begin;
        }
    };

    std::string getArchDir() const { return archdir; }

    run_number_t getLastRun();
    run_number_t getLastRun(unsigned level);
    run_number_t getFirstRun(unsigned level);

    // run generation methods
    rc_t openNewRun(unsigned level);
    void append(char* data, size_t length, unsigned level);
    void fsync(unsigned level);
    rc_t closeCurrentRun(run_number_t currentRun, unsigned level, PageID maxPID = 0);

    // run scanning methods
    RunFile* openForScan(const RunId& runid);
    void closeScan(const RunId& runid);

    void listFiles(std::vector<std::string>& list, int level = -1);
    void listFileStats(std::list<RunId>& list, int level = -1);
    void deleteRuns(unsigned replicationFactor = 0);

    static bool parseRunFileName(string fname, RunId& fstats);
    static size_t getFileSize(int fd);

    void newBlock(const vector<pair<PageID, size_t> >& buckets, unsigned level);

    rc_t finishRun(run_number_t first, run_number_t last, PageID maxPID,
            int fd, off_t offset, unsigned level);

    template <class Input>
    void probe(std::vector<Input>&, PageID, PageID, run_number_t runBegin,
            run_number_t& runEnd);

    void loadRunInfo(RunFile*, const RunId&);
    void startNewRun(unsigned level);

    unsigned getMaxLevel() const { return maxLevel; }
    size_t getRunCount(unsigned level) {
        if (level > maxLevel) { return 0; }
        return runs[level].size();
    }

    void dumpIndex(ostream& out);
    void dumpIndex(ostream& out, const RunId& runid);

    template <class OutputIter>
    void listRunsNonOverlapping(OutputIter out)
    {
        auto level = maxLevel;
        run_number_t nextRun = 1;

        // Start collecting runs on the max level, which has the largest runs
        // and therefore requires the least random reads
        while (level > 0) {
            auto index = findRun(nextRun, level);

            while ((int) index <= lastFinished[level]) {
                auto& run = runs[level][index];
                out = RunId{run.begin, run.end, level};
                nextRun = run.end + 1;
                index++;
            }

            level--;
        }
    }

private:

    void appendNewRun(unsigned level);
    size_t findRun(run_number_t run, unsigned level);
    // binary search
    size_t findEntry(RunInfo* run, PageID pid,
            int from = -1, int to = -1);
    void serializeRunInfo(RunInfo&, int fd, off_t);

private:
    std::string archdir;
    std::vector<int> appendFd;
    std::vector<off_t> appendPos;

    fs::path archpath;

    // Run information for each level of the index
    std::vector<std::vector<RunInfo>> runs;

    // Last finished run on each level -- this is required because runs are
    // generated asynchronously, so that a new one may be appended to the
    // index before the last one is finished. Thus, when calling finishRun(),
    // we cannot simply take the last run in the vector.
    std::vector<int> lastFinished;

    unsigned maxLevel;

    std::unique_ptr<RunRecycler> runRecycler;

    mutable srwlock_t _mutex;

    /// Cache for open files (for scans only)
    std::map<RunId, RunFile, CmpOpenFiles> _open_files;
    mutable srwlock_t _open_file_mutex;
    size_t _max_open_files;
    bool directIO;

    fs::path make_run_path(run_number_t begin, run_number_t end, unsigned level = 1) const;
    fs::path make_current_run_path(unsigned level) const;

public:
    const static string RUN_PREFIX;
    const static string CURR_RUN_PREFIX;
    const static string run_regex;
    const static string current_regex;
};

template <class Input>
void ArchiveIndex::probe(std::vector<Input>& inputs,
        PageID startPID, PageID endPID, run_number_t runBegin, run_number_t& runEnd)
{
    spinlock_read_critical_section cs(&_mutex);

    Input input;
    input.endPID = endPID;
    unsigned level = maxLevel;
    inputs.clear();
    run_number_t nextRun = runBegin;

    while (level > 0) {
        if (runEnd > 0 && nextRun > runEnd) { break; }

        size_t index = findRun(nextRun, level);
        while ((int) index <= lastFinished[level]) {
            auto& run = runs[level][index];
            index++;
            nextRun = run.end;

            if (startPID > run.maxPID) {
                // INC_TSTAT(la_avoided_probes);
                continue;
            }

            if (run.entries.size() > 0) {
                size_t entryBegin = findEntry(&run, startPID);

                if (run.entries[entryBegin].pid >= endPID) {
                    // INC_TSTAT(la_avoided_probes);
                    continue;
                }

                input.pos = run.entries[entryBegin].offset;
                input.runFile =
                    openForScan(RunId{run.begin, run.end, level});
                w_assert1(input.pos < input.runFile->length);
                inputs.push_back(input);
            }
        }

        level--;
    }

    // Return last probed run as out-parameter
    runEnd = nextRun;
}

#endif
