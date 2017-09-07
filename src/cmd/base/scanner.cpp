#include "scanner.h"

#include <logarchive_scanner.h>
#include <chkpt.h>
#include <sm.h>
#include <restart.h>
#include <dirent.h>

// CS TODO isolate this to log archive code
const static int DFT_BLOCK_SIZE = 1024 * 1024; // 1MB = 128 pages

const auto& parseRunFileName = ArchiveIndex::parseRunFileName;

void BaseScanner::handle(logrec_t* lr)
{
    for (auto h : handlers) {
        h->invoke(*lr);
    }
}

void BaseScanner::finalize()
{
    for (auto h : handlers) {
        h->finalize();
    }
}

void BaseScanner::initialize()
{
    for (auto h : handlers) {
        h->initialize();
    }
}

BlockScanner::BlockScanner(const po::variables_map& options,
        bitset<t_max_logrec>* filter)
    : BaseScanner(options), pnum(-1)
{
    logdir = options["logdir"].as<string>().c_str();
    // blockSize = options["sm_archiver_block_size"].as<int>();
    // CS TODO no option for archiver block size
    blockSize = DFT_BLOCK_SIZE;
    logScanner = new LogScanner(blockSize);
    currentBlock = new char[blockSize];

    if (filter) {
        logScanner->ignoreAll();
        for (int i = 0; i < t_max_logrec; i++) {
            if (filter->test(i)) {
                logScanner->unsetIgnore((kind_t) i);
            }
        }
        // skip cannot be ignored because it tells us when file ends
        logScanner->unsetIgnore(skip_log);
    }
}

void BlockScanner::findFirstFile()
{
    pnum = numeric_limits<int>::max();
    DIR* dir = opendir(logdir);
    if (!dir) {
        cerr << "Error: could not open recovery log dir: " << logdir << endl;
        W_COERCE(RC(fcOS));
    }
    struct dirent* entry = readdir(dir);
    const char * PREFIX = "log.";

    while (entry != NULL) {
        const char* fname = entry->d_name;
        if (strncmp(PREFIX, fname, strlen(PREFIX)) == 0) {
            int p = atoi(fname + strlen(PREFIX));
            if (p < pnum) {
                pnum = p;
            }
        }
        entry = readdir(dir);
    }
    closedir(dir);
}

string BlockScanner::getNextFile()
{
    stringstream fname;
    fname << logdir << "/";
    if (pnum < 0) {
        findFirstFile();
    }
    else {
        pnum++;
    }
    fname << "log." << pnum;

    if (openFileCallback) {
        openFileCallback(fname.str().c_str());
    }

    return fname.str();
}

void BlockScanner::run()
{
    BaseScanner::initialize();

    size_t bpos = 0;
    streampos fpos = 0, fend = 0;
    //long count = 0;
    int firstPartition = pnum;
    logrec_t* lr = NULL;

    while (true) {
        // open partition number pnum
        string fname = restrictFile.empty() ? getNextFile() : restrictFile;
        ifstream in(fname, ios::binary | ios::ate);

        // does the file exist?
        if (!in.good()) {
            in.close();
            break;
        }

        // file is opened at the end
        fend = in.tellg();
        fpos = 0;

        cerr << "Scanning log file " << fname << endl;

        while (fpos < fend) {
            //cerr << "Reading block at " << fpos << " from " << fname.str();

            // read next block from partition file
            in.seekg(fpos);
            if (in.fail()) {
                throw runtime_error("IO error seeking into file");
            }
            in.read(currentBlock, blockSize);
            if (in.eof()) {
                // partial read on end of file
                fpos = fend;
            }
            else if (in.gcount() == 0) {
                // file ended exactly on block boundary
                break;
            }
            else if (in.fail()) {
                // EOF implies fail, so we check it first
                throw runtime_error("IO error reading block from file");
            }
            else {
                fpos += blockSize;
            }

            //cerr << " - " << in.gcount() << " bytes OK" << endl;

            bpos = 0;
            while (logScanner->nextLogrec(currentBlock, bpos, lr)) {
                handle(lr);
                if (lr->type() == skip_log) {
                    fpos = fend;
                    break;
                }
            }
        }

        in.close();

        if (!restrictFile.empty()) {
            break;
        }
    }

    if (pnum == firstPartition && bpos == 0) {
        throw runtime_error("Could not find/open log files in "
                + string(logdir));
    }

    BaseScanner::finalize();
}

BlockScanner::~BlockScanner()
{
    delete currentBlock;
    delete logScanner;
}


LogArchiveScanner::LogArchiveScanner(const po::variables_map& options)
    : BaseScanner(options), runBegin(lsn_t::null), runEnd(lsn_t::null)
{
    archdir = options["logdir"].as<string>();
    level = options["level"].as<int>();
    scan_pid = options["pid"].as<PageID>();
}

bool runCompare (string a, string b)
{
    RunId fstats;
    parseRunFileName(a, fstats);
    lsn_t lsn_a = fstats.beginLSN;
    parseRunFileName(b, fstats);
    lsn_t lsn_b = fstats.beginLSN;
    return lsn_a < lsn_b;
}

void LogArchiveScanner::run()
{
    BaseScanner::initialize();

    // CS TODO no option for archiver block size
    // size_t blockSize = LogArchiver::DFT_BLOCK_SIZE;
    // size_t blockSize = options["sm_archiver_block_size"].as<int>();
    sm_options opt;
    opt.set_string_option("sm_archdir", archdir);
    // opt.set_int_option("sm_archiver_block_size", blockSize);
    auto directory = std::make_shared<ArchiveIndex>(opt);

    std::vector<std::string> runFiles;

    if (restrictFile.empty()) {
        directory->listFiles(runFiles, level);
        std::sort(runFiles.begin(), runFiles.end(), runCompare);
    }
    else {
        runFiles.push_back(restrictFile);
    }

    RunId fstats;
    parseRunFileName(runFiles[0], fstats);
    runBegin = fstats.beginLSN;
    runEnd = fstats.endLSN;
    std::vector<std::string>::const_iterator it;
    for(size_t i = 0; i < runFiles.size(); i++) {
        if (i > 0) {
            // begin of run i must be equal to end of run i-1
            parseRunFileName(runFiles[i], fstats);
            runBegin = fstats.beginLSN;
            if (runBegin != runEnd) {
                throw runtime_error("Hole found in run boundaries!");
            }
            runEnd = fstats.endLSN;
        }

        if (openFileCallback) {
            openFileCallback(runFiles[i].c_str());
        }

        ArchiveScan scan {directory};
        scan.open(
                    scan_pid, // first PID
                    0, // last PID
                    runBegin,
                    runEnd
                    // CS TODO: level not supported yet
                    // fstats.level
            );

        lsn_t prevLSN = lsn_t::null;
        PageID prevPid = 0;

        logrec_t* lr;
        while (scan.next(lr)) {
            w_assert0(lr->pid() >= prevPid);
            w_assert0(lr->has_page_img(lr->pid()) ||
                    lr->pid() != prevPid ||
                    lr->page_prev_lsn() == lsn_t::null ||
                    lr->page_prev_lsn() == prevLSN);
            w_assert0(lr->lsn_ck() >= runBegin);
            w_assert0(lr->lsn_ck() < runEnd);

            handle(lr);

            prevLSN = lr->lsn_ck();
            prevPid = lr->pid();
        };
    }

    BaseScanner::finalize();
}

MergeScanner::MergeScanner(const po::variables_map& options)
    : BaseScanner(options)
{
    archdir = options["logdir"].as<string>();
    level = options["level"].as<int>();
    scan_pid = options["pid"].as<PageID>();
}

void MergeScanner::run()
{
    BaseScanner::initialize();

    sm_options opt;
    opt.set_string_option("sm_archdir", archdir);
    // opt.set_int_option("sm_archiver_block_size", blockSize);
    // opt.set_int_option("sm_archiver_bucket_size", bucketSize);

    auto directory = std::make_shared<ArchiveIndex>(opt);
    ArchiveScan scan {directory};
    scan.open(scan_pid, 0, lsn_t::null);

    logrec_t* lr;

    lsn_t prevLSN = lsn_t::null;
    PageID prevPid = 0;

    while (scan.next(lr)) {
        w_assert0(lr->pid() >= prevPid);
        w_assert0(lr->has_page_img(lr->pid()) ||
                lr->pid() != prevPid ||
                lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == prevLSN);

        handle(lr);

        prevLSN = lr->lsn_ck();
        prevPid = lr->pid();
    }

    BaseScanner::finalize();
}

