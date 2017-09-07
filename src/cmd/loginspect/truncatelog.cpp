#include "truncatelog.h"

#include <fstream>

#include "sm.h"

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

void TruncateLog::setupOptions()
{
    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
         "Directory containing log to be truncated")
        ("partition,p", po::value<size_t>(&partition)->required(),
         "Partition number to generate");
}

void TruncateLog::run()
{
    // CS TODO fix this
    // start_base();
    // start_log(logdir);
    // start_buffer();
    // start_other();

    // ss_m::SSM->_truncate_log();

    // CS TODO: temporary code to generate empty log file
    const size_t bufsize = 8192;
    char* buffer = new char[bufsize];
    ::memset(buffer, 0, bufsize);
    size_t pos = 0;

    logrec_t* logrec = (logrec_t*) (buffer + pos);
    logrec->init_header(skip_log);
    // reinterpret_cast<skip_log*>(logrec)->construct();
    logrec->set_lsn_ck(lsn_t(partition, pos));
    pos += logrec->length();

    {
        string fname = logdir + "/log." + std::to_string(partition);
        std::ofstream ofs (fname, std::ofstream::out | std::ofstream::binary
                | std::ofstream::trunc);

        ofs.write(buffer, bufsize);
        ofs.close();
    }

    delete buffer;
}
