#ifndef SCANNER_H
#define SCANNER_H


#include "sm_base.h"
#include "lsn.h"
#include "logarchiver.h"

#include "basethread.h"
#include "handler.h"

#include <bitset>
#include <functional>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

class BaseScanner : public basethread_t {
public:
    BaseScanner(const po::variables_map& options)
        : options(options), restrictFile("")
    {}

    virtual ~BaseScanner()
    {}

    void setRestrictFile(string fname) { restrictFile = fname; }

    std::function<void(const char*)> openFileCallback;

    void add_handler(Handler* h)
    {
        handlers.push_back(h);
    }

protected:
    virtual void handle(logrec_t* lr, lsn_t lsn = lsn_t::null);
    virtual void finalize();
    virtual void initialize();
    po::variables_map options;

    vector<Handler*> handlers;

    string restrictFile;
};

class BlockScanner : public BaseScanner {
public:
    BlockScanner(const po::variables_map& options,
            bitset<t_max_logrec>* filter = NULL);
    virtual ~BlockScanner();

    virtual void run();
private:
    LogScanner* logScanner;
    char* currentBlock;
    const char* logdir;
    size_t blockSize;
    int pnum;

    void findFirstFile();
    string getNextFile();
};

class LogArchiveScanner : public BaseScanner {
public:
    LogArchiveScanner(const po::variables_map& options);
    virtual ~LogArchiveScanner() {};

    virtual void run();
private:
    string archdir;
    run_number_t runBegin;
    run_number_t runEnd;
    int level;
    PageID scan_pid;

    void findFirstFile();
    string getNextFile();
};

class MergeScanner : public BaseScanner {
public:
    MergeScanner(const po::variables_map& options);
    virtual ~MergeScanner() {};

    virtual void run();
private:
    string archdir;
    int level;
    PageID scan_pid;
};

#endif
