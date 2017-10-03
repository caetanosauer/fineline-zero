#include "log_consumer.h"

#include "log_core.h"

// files and stuff
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// CS TODO: use option
const static int IO_BLOCK_COUNT = 8; // total buffer = 8MB

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

ArchiverControl::ArchiverControl(std::atomic<bool>* shutdownFlag)
    : endLSN(lsn_t::null), activated(false), listening(false), shutdownFlag(shutdownFlag)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&activateCond, NULL));
}

ArchiverControl::~ArchiverControl()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
    DO_PTHREAD(pthread_cond_destroy(&activateCond));
}

bool ArchiverControl::activate(bool wait, lsn_t lsn)
{
    if (wait) {
        DO_PTHREAD(pthread_mutex_lock(&mutex));
    }
    else {
        if (pthread_mutex_trylock(&mutex) != 0) {
            return false;
        }
    }
    // now we hold the mutex -- signal archiver thread and set endLSN

    /* Make sure signal is sent only if thread is listening.
     * TODO: BUG? The mutex alone cannot guarantee that the signal is not lost,
     * since the activate call may happen before the thread ever starts
     * listening. If we ever get problems with archiver getting stuck, this
     * would be one of the first things to try. We could, e.g., replace
     * the listening flag with something like "gotSignal" and loop this
     * method until it's true.
     */
    // activation may not decrease the endLSN
    w_assert0(lsn >= endLSN);
    endLSN = lsn;
    activated = true;
    DO_PTHREAD(pthread_cond_signal(&activateCond));
    DO_PTHREAD(pthread_mutex_unlock(&mutex));

    /*
     * Returning true only indicates that signal was sent, and not that the
     * archiver thread is running with the given endLSN. Another thread
     * calling activate may get the mutex before the log archiver and set
     * another endLSN. In fact, it does not even mean that the signal was
     * received, since the thread may not be listening yet.
     */
    return activated;
}

bool ArchiverControl::waitForActivation()
{
    // WARNING: mutex must be held by caller!
    listening = true;
    while(!activated) {
        struct timespec timeout;
        smthread_t::timeout_to_timespec(100, timeout); // 100ms
        int code = pthread_cond_timedwait(&activateCond, &mutex, &timeout);
        if (code == ETIMEDOUT) {
            //DBGTHRD(<< "Wait timed out -- try again");
            if (*shutdownFlag) {
                DBGTHRD(<< "Activation failed due to shutdown. Exiting");
                return false;
            }
        }
        DO_PTHREAD_TIMED(code);
    }
    listening = false;
    return true;
}

ReaderThread::ReaderThread(AsyncRingBuffer* readbuf, lsn_t startLSN)
    :
      log_worker_thread_t(-1 /* interval_ms */),
      buf(readbuf), currentFd(-1), pos(0), localEndLSN(0)
{
    // position initialized to startLSN
    pos = startLSN.lo();
    nextPartition = startLSN.hi();
}

rc_t ReaderThread::openPartition()
{
    if (currentFd != -1) {
        auto ret = ::close(currentFd);
        CHECK_ERRNO(ret);
    }
    currentFd = -1;

    // open file for read -- copied from partition_t::peek()
    int fd;
    string fname = smlevel_0::log->make_log_name(nextPartition);

    int flags = O_RDONLY;
    fd = ::open(fname.c_str(), flags, 0744 /*mode*/);
    CHECK_ERRNO(fd);

    struct stat stat;
    auto ret = ::fstat(fd, &stat);
    CHECK_ERRNO(ret);
    if (stat.st_size == 0) { return RC(eEOF); }
    off_t partSize = stat.st_size;

    /*
     * The size of the file must be at least the offset of endLSN, otherwise
     * the given endLSN was incorrect. If this is not the partition of
     * endLSN.hi(), then we simply assert that its size is not zero.
     */
    if (localEndLSN.hi() == nextPartition) {
        w_assert0(partSize >= localEndLSN.lo());
    }
    else {
        w_assert1(partSize > 0);
    }

    DBGTHRD(<< "Opened log partition for read " << fname);

    currentFd = fd;
    nextPartition++;
    return RCOK;
}

void ReaderThread::do_work()
{
    auto blockSize = getBlockSize();
    // copy endLSN into local var to avoid it changing in-between steps below
    localEndLSN = getEndLSN();

    DBGTHRD(<< "Reader thread activated until " << localEndLSN);

    /*
     * CS: The code was changed to not rely on the file size anymore,
     * because we may read from a file that is still being appended to.
     * The correct behavior is to rely on the given endLSN, which must
     * be guaranteed to be persistent on the file. Therefore, we cannot
     * read past the end of the file if we only read until endLSN.
     * A physical read past the end is OK because we use pread_short().
     * The position from which the first logrec will be read is set in pos
     * by the activate method, which takes the startLSN as parameter.
     */

    while(true) {
        unsigned currPartition =
            currentFd == -1 ? nextPartition : nextPartition - 1;
        if (localEndLSN.hi() == currPartition && pos >= localEndLSN.lo())
        {
            /*
             * The requested endLSN is within a block which was already
             * read. Stop and wait for next activation, which must start
             * reading from endLSN, since anything beyond that might
             * have been updated alread (usually, endLSN is the current
             * end of log). Hence, we update pos with it.
             */
            pos = localEndLSN.lo();
            DBGTHRD(<< "Reader thread reached endLSN -- sleeping."
                    << " New pos = " << pos);
            break;
        }

        if (should_exit()) {
            DBGTHRD(<< "Reader thread got shutdown request.");
            break;
        }

        // get buffer space to read into
        char* dest = buf->producerRequest();
        if (!dest) {
            W_FATAL_MSG(fcINTERNAL,
                    << "Error requesting block on reader thread");
            break;
        }


        if (currentFd == -1) {
            W_COERCE(openPartition());
        }

        // Read only the portion which was ignored on the last round
        size_t blockPos = pos % blockSize;
        int bytesRead = ::pread(currentFd, dest + blockPos, blockSize - blockPos, pos);
        CHECK_ERRNO(bytesRead);

        if (bytesRead == 0) {
            // Reached EOF -- open new file and try again
            DBGTHRD(<< "Reader reached EOF (bytesRead = 0)");
            W_COERCE(openPartition());
            pos = 0;
            blockPos = 0;
            bytesRead = ::pread(currentFd, dest, blockSize, pos);
            CHECK_ERRNO(bytesRead);
            if (bytesRead == 0) {
                W_FATAL_MSG(fcINTERNAL,
                        << "Error reading from partition "
                        << nextPartition - 1);
            }
        }

        DBGTHRD(<< "Read block " << (void*) dest << " from fpos " << pos <<
                " with size " << bytesRead << " into blockPos "
                << blockPos);
        w_assert0(bytesRead > 0);

        pos += bytesRead;
        buf->producerRelease();
    }
}

LogConsumer::LogConsumer(lsn_t startLSN, size_t blockSize, bool ignore)
    : nextLSN(startLSN), endLSN(lsn_t::null), currentBlock(NULL),
    blockSize(blockSize)
{
    DBGTHRD(<< "Starting log archiver at LSN " << nextLSN);

    // pos must be set to the correct offset within a block
    pos = startLSN.lo() % blockSize;

    readbuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    reader = new ReaderThread(readbuf, startLSN);
    logScanner = new LogScanner(blockSize);

    if(ignore) { initLogScanner(logScanner); }
    reader->fork();
}

LogConsumer::~LogConsumer()
{
    if (!readbuf->isFinished()) {
        shutdown();
    }
    delete reader;
    delete readbuf;
}

void LogConsumer::initLogScanner(LogScanner* logScanner)
{
    // CS TODO use flags to filter -- there's gotta be a better way than this
    logScanner->setIgnore(comment_log);
    logScanner->setIgnore(chkpt_begin_log);
    logScanner->setIgnore(xct_end_log);
    logScanner->setIgnore(restore_begin_log);
    logScanner->setIgnore(restore_segment_log);
    logScanner->setIgnore(restore_end_log);
    logScanner->setIgnore(tick_sec_log);
    logScanner->setIgnore(tick_msec_log);
    logScanner->setIgnore(page_read_log);
    logScanner->setIgnore(page_write_log);
}

void LogConsumer::shutdown()
{
    if (!readbuf->isFinished()) {
        readbuf->set_finished();
        reader->stop();
    }
}

void LogConsumer::open(lsn_t endLSN, bool readWholeBlocks)
{
    this->endLSN = endLSN;
    this->readWholeBlocks = readWholeBlocks;

    reader->wakeup_until_lsn(endLSN);

    nextBlock();
}

bool LogConsumer::nextBlock()
{
    if (currentBlock) {
        readbuf->consumerRelease();
        DBGTHRD(<< "Released block for replacement " << (void*) currentBlock);
        currentBlock = NULL;
    }

    // get a block from the reader thread
    currentBlock = readbuf->consumerRequest();
    if (!currentBlock) {
        if (!readbuf->isFinished()) {
            // This happens if log scanner finds a skip logrec, but
            // then the next partition does not exist. This would be a bug,
            // because endLSN should always be an existing LSN, or one
            // immediately after an existing LSN but in the same partition.
            W_FATAL_MSG(fcINTERNAL, << "Consume request failed!");
        }
        return false;
    }
    DBGTHRD(<< "Picked block for replacement " << (void*) currentBlock);
    if (pos >= blockSize) {
        // If we are reading the same block but from a continued reader cycle,
        // pos should be maintained. For this reason, pos should be set to
        // blockSize on constructor.
        pos = 0;
    }

    return true;
}

bool LogConsumer::next(logrec_t*& lr, lsn_t* lsn)
{
    w_assert1(nextLSN <= endLSN);

    if (!currentBlock) {
        if (!nextBlock()) { return false; }
    }

    int lrLength = 0;
    bool scanned = logScanner->nextLogrec(currentBlock, pos, lr, &nextLSN,
            &endLSN, &lrLength);

    if (scanned && lsn) {
        *lsn = nextLSN - lr->length();
    }

    bool stopReading = nextLSN == endLSN;
    if (!scanned && readWholeBlocks && !stopReading) {
        /*
         * If the policy is to read whole blocks only, we must also stop
         * reading when an incomplete log record was fetched on the last block.
         * Under normal circumstances, we would fetch the next block to
         * assemble the remainder of the log record. In this case, however, we
         * must wait until the next activation. This case is detected when the
         * length of the next log record is larger than the space remaining in
         * the current block, or if the length is negative (meaning there are
         * not enough bytes left on the block to tell the length).
         */
        stopReading = endLSN.hi() == nextLSN.hi() &&
            (lrLength <= 0 || (endLSN.lo() - nextLSN.lo() < lrLength));
    }

    if (!scanned && stopReading) {
        DBGTHRD(<< "Consumer reached end LSN on " << nextLSN);
        /*
         * nextLogrec returns false if it is about to read the LSN given in the
         * last argument (endLSN). This means we should stop and not read any
         * further blocks.  On the next archiver activation, replacement must
         * start on this LSN, which will likely be in the middle of the block
         * currently being processed. However, we don't have to worry about
         * that because reader thread will start reading from this LSN on the
         * next activation.
         */
        return false;
    }

    w_assert1(nextLSN <= endLSN);
    // FINELINE
    // w_assert1(!scanned || lr->lsn_ck() + lr->length() == nextLSN);

    if (!scanned || (lrLength > 0 && lr->type() == skip_log)) {
        /*
         * nextLogrec returning false with nextLSN != endLSN means that we are
         * suppose to read another block and call the method again.
         */
        if (scanned && lr->type() == skip_log) {
            // Try again if reached skip -- next block should be from next file
            nextLSN = lsn_t(nextLSN.hi() + 1, 0);
            pos = 0;
            DBGTHRD(<< "Reached skip logrec, set nextLSN = " << nextLSN);
            logScanner->reset();
            w_assert1(!logScanner->hasPartialLogrec());
        }
        if (!nextBlock()) {
            // reader thread finished and consume request failed
            DBGTHRD(<< "LogConsumer next-block request failed");
            return false;
        }
        return next(lr, lsn);
    }

    w_assert1(!lsn || lr->valid_header());
    return true;
}

bool LogScanner::hasPartialLogrec()
{
    return truncCopied > 0;
}

void LogScanner::reset()
{
    truncCopied = 0;
}

/**
 * Fetches a log record from the read buffer ('src' in offset 'pos').
 * Handles incomplete records due to block boundaries in the buffer
 * and skips checkpoints and skip log records. Returns false if whole
 * record could not be read in the current buffer block, indicating that
 * the caller must fetch a new block into 'src' and invoke method again.
 *
 * Method loops until any in-block skipping is completed.
 */
bool LogScanner::nextLogrec(char* src, size_t& pos, logrec_t*& lr, lsn_t* nextLSN,
        lsn_t* stopLSN, int* lrLength)
{
tryagain:
    if (nextLSN && stopLSN && *stopLSN == *nextLSN) {
        return false;
    }

    // whole log record is not guaranteed to fit in a block
    size_t remaining = blockSize - pos;
    if (remaining == 0) {
        return false;
    }

    lr = (logrec_t*) (src + pos);

    if (truncCopied > 0) {
        // finish up the trunc logrec from last block
        // DBG3(<< "Reading partial log record -- missing: "
        //         << truncMissing << " of " << truncCopied + truncMissing);
        // w_assert1(truncMissing <= remaining);
        memcpy(truncBuf + truncCopied, src + pos, sizeof(logrec_t) - truncCopied);
        lr = (logrec_t*) truncBuf;
        pos += (lr->length() - truncCopied);
        truncCopied = 0;
    }
    // we need at least the header bytes to read the length
    else if (remaining < sizeof(baseLogHeader) || lr->length() > remaining) {
        DBG3(<< "Log record with length "
                << (remaining >= sizeof(baseLogHeader) ? lr->length() : -1)
                << " does not fit in current block of " << remaining);
        w_assert0(remaining <= sizeof(logrec_t));
        memcpy(truncBuf, src + pos, remaining);
        truncCopied = remaining;
        pos += remaining;

        if (lrLength) {
            *lrLength = (remaining >= sizeof(baseLogHeader)) ? lr->length() : -1;
        }

        return false;
    }

    // w_assert1(lr->valid_header(nextLSN == NULL ? lsn_t::null : *nextLSN));
    w_assert1(lr->valid_header());

    if (nextLSN) {
        *nextLSN += lr->length();
    }

    if (lrLength) {
        *lrLength = lr->length();
    }

    // handle ignorred logrecs
    if (ignore[lr->type()]) {
        // if logrec was assembled from truncation, pos was already
        // incremented, and skip is not necessary
        if ((void*) lr == (void*) truncBuf) {
            goto tryagain;
        }
        // DBGTHRD(<< "Found " << lr->type_str() << " on " << lr->lsn_ck()
        //         << " pos " << pos << ", skipping " << lr->length());
        toSkip += lr->length();
    }

    // see if we have something to skip
    if (toSkip > 0) {
        if (toSkip <= remaining) {
            // stay in the same block after skipping
            pos += toSkip;
            //DBGTHRD(<< "In-block skip for replacement, new pos = " << pos);
            toSkip = 0;
            goto tryagain;
        }
        else {
            DBGTHRD(<< "Skipping to next block until " << toSkip);
            toSkip -= remaining;
            return false;
        }
    }

    // if logrec was assembled from truncation, pos was already incremented
    if ((void*) lr != (void*) truncBuf) {
        pos += lr->length();
    }

    // DBGTHRD(<< "Log scanner returning  " << lr->type_str()
    //         << " on pos " << pos << " lsn " << lr->lsn_ck());


    return true;
}
