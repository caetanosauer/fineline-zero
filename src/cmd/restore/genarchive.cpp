#include "genarchive.h"
#include "xct_logger.h"

#include <fstream>

// CS TODO: LA metadata -- should be serialized on run files
const size_t BLOCK_SIZE = 8*1024*1024;

void GenArchive::setupOptions()
{
    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Directory containing the log to be archived")
        ("archdir,a", po::value<string>(&archdir)->required(),
            "Directory where the archive runs will be stored (must exist)")
        ("bucket", po::value<size_t>(&bucketSize)->default_value(1),
            "Size of log archive index bucked in output runs")
        // ("maxLogSize,m", po::value<long>(&maxLogSize)->default_value(m),
        //     "max_logsize parameter of Shore-MT (default should be fine)")
    ;
}

void GenArchive::run()
{
    // CS TODO: sm_options is gone!
    // opt.set_int_option("sm_archiver_block_size", BLOCK_SIZE);
    // opt.set_int_option("sm_archiver_bucket_size", bucketSize);
    // opt.set_int_option("sm_page_img_compression", 16384);

    LogManager* log = new LogManager(logdir);
    log->init();
    smlevel_0::log = log;
    // CS TODO: little hack -- empty log causes file not found in archiveUntilLSN
    Logger::log_sys<LogRecordType::comment_log>("o hi there");
    log->flush_all();

    LogArchiver* la = new LogArchiver(archdir, log, true /*format*/, false /*merge*/);

    lsn_t durableLSN = log->durable_lsn();
    cerr << "Activating log archiver until LSN " << durableLSN << endl;

    la->fork();

    la->requestFlushSync(durableLSN);

    la->shutdown();
    la->join();

    delete la;
    delete log;
}
