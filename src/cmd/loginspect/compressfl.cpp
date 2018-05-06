#include "compressfl.h"

#include <memory>
#include "logarchiver.h"
#include "xct_logger.h"
#include "alloc_cache.h"
#include "bf_tree.h"

void CompressFl::setupOptions()
{
    boost::program_options::options_description opt("VerifyLog Options");
    opt.add_options()
        ("archdir", po::value<string>(&archdir), "Path to archdir")
        ("logdir", po::value<string>(&logdir), "Path to logdir")
    ;
    options.add(opt);
}

void CompressFl::run()
{
    sm_options options;

    auto log = std::make_unique<LogManager>(logdir, false /*format*/);
    smlevel_0::log = log.get();
    log->init();
    auto arch = std::make_unique<LogArchiver>(archdir, log.get(), true /*format*/, false /*merge*/);
    smlevel_0::logArchiver = arch.get();
    arch->fork();
    auto bf = std::make_unique<bf_tree_m>(options);
    smlevel_0::bf = bf.get();
    auto stnode = std::make_unique<stnode_cache_t>(false);
    auto alloc = std::make_unique<alloc_cache_t>(*stnode, false, true);

    W_COERCE(ss_m::begin_xct());
    PageID last_pid = alloc->get_last_allocated_pid();
    for (PageID pid = 0; pid <= last_pid; pid++) {
        if (!alloc->is_allocated(pid)) { continue; }
        fixable_page_h p;
        p.fix_direct(pid, LATCH_SH);
        sys_xct_section_t ssx;
        Logger::log_p<LogRecordType::page_img_format_log>(&p);
        W_COERCE(ssx.end_sys_xct(RCOK));
    }
    W_COERCE(ss_m::commit_xct());

    log->flush_all();
    arch->shutdown();
    bf->shutdown();
    log->shutdown();
}
