#include "restart.h"
#include "logarchiver.h"
#include "xct_logger.h"

SprIterator::SprIterator()
    : archive_scan{smlevel_0::logArchiver ? smlevel_0::logArchiver->getIndex() : nullptr}
{
}

SprIterator::~SprIterator()
{
}

void SprIterator::open(PageID pid)
{
    archive_scan.open(pid, pid+1);
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

        lr->redo(&p);
    }
}

