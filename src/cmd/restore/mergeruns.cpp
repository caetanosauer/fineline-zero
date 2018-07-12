#include "mergeruns.h"

#include "logarchiver.h"

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;


void MergeRuns::setupOptions()
{
    options.add_options()
        ("indir", po::value<string>(&indir)->required(),
            "Directory containing the runs to be merged")
        ("outdir", po::value<string>(&outdir)->default_value(""),
            "Directory where the merged runs will be stored (empty for same as indir)")
        ("level", po::value<size_t>(&level)->default_value(1),
            "Level whose runs will be merged (a run in level+1 will be created)")
        ("fanin", po::value<size_t>(&fanin)->required(),
            "Merge fan-in (required, larger than 1)")
        ("repl", po::value<size_t>(&replFactor)->default_value(0),
            "Delete runs after merge to maintain given replication factor")
        ("compression", po::value<size_t>(&compression)->default_value(0),
            "Page-img compression threshold value, in bytes")
    ;
    Command::setupSMOptions(options);
}

void MergeRuns::run()
{
    if (fanin <= 1) {
        throw runtime_error("Invalid merge fan-in (must be > 1)");
    }

    // CS TODO: sm_options is gone!
    // sm_options opt;
    // opt.set_string_option("sm_archdir", indir);
    // opt.set_int_option("sm_page_img_compression", compression);
    auto in = std::make_shared<ArchiveIndex>(indir, nullptr /*logStorage*/, false /*format*/);

    auto out = in;
    if (!outdir.empty() && outdir != indir) {
        // if directory does not exist, create it
        fs::path fspath(outdir);
        if (!fs::exists(fspath)) {
            fs::create_directories(fspath);
        }
        else {
            if (!fs::is_directory(fspath)) {
                throw runtime_error("Provided path is not a directory!");
            }
        }

        // opt.set_string_option("sm_archdir", outdir);
        out = std::make_shared<ArchiveIndex>(outdir, nullptr /*logStorage*/, false /*format*/);
    }

    MergerDaemon merge(in, fanin, out);
    merge.doMerge(level);

    if (replFactor > 0) {
        out->deleteRuns(replFactor);
    }
}
