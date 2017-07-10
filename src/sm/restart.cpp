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
    archive_scan.open(pid, pid+1, lsn_t::null);
}

bool SprIterator::next(logrec_t*& lr)
{
    if (archive_scan.next(lr)) {
        last_lsn = lr->lsn();
        replayed_count++;
        return true;
    }
    return false;
}

void SprIterator::apply(fixable_page_h &p)
{
    lsn_t prev_lsn = lsn_t::null;
    PageID pid = p.pid();
    logrec_t* lr;

    while (next(lr)) {
        w_assert1(lr->valid_header(lsn_t::null));

        if (lr->is_redo() && p.lsn() < lr->lsn()) {
            DBGOUT1(<< "SPR page(" << p.pid()
                    << ") LSN=" << p.lsn() << ", log=" << *lr);

            w_assert1(pid == lr->pid() || pid == lr->pid2());
            lr->redo(&p);
        }

        prev_lsn = lr->lsn();
    }
}

