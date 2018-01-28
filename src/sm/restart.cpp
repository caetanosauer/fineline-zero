#include "restart.h"
#include "logarchiver.h"
#include "xct_logger.h"

SprIterator::SprIterator()
    : archive_scan{smlevel_0::logArchiver ? smlevel_0::logArchiver->getIndex() : nullptr},
    img_consumed{false}
{
}

SprIterator::~SprIterator()
{
}

void SprIterator::open(PageID pid)
{
    archive_scan.open(pid, pid+1);
    img_consumed = false;
}

void SprIterator::apply(fixable_page_h &p)
{
    PageID pid = p.pid();
    logrec_t* lr;

    while (archive_scan.next(lr)) {
        w_assert1(lr->valid_header());
        w_assert1(lr->is_redo());
        w_assert1(lr->page_version() > 0);
        w_assert1(pid == lr->pid());
        w_assert1(lr->has_page_img() || p.version() > 0);
        w_assert1(p.version() < lr->page_version()); // Always true in FineLine

        // This is a hack to circumvent a problem with page-img compression. Since it is an SSX,
        // it may appear in the log before a page update with lower version. Usually, that's not a
        // problem, because the updates will be ordered by version when scanning. But, in the
        // special case where the lower update ends up in the next log file, it will not be
        // eliminated by page-img compression, and thus SprIterator will not see the page image
        // as the first log record. This is fixed with the check below.
        if (lr->has_page_img()) {
            img_consumed = true;
        }
        else if (!img_consumed) {
            continue;
        }

        lr->redo(&p);
    }
}

