#include "xctlatency.h"

#include "logrec_serialize.h"

void XctLatency::setupOptions()
{
    LogScannerCommand::setupOptions();
    boost::program_options::options_description agglog("XctLatency Options");
    agglog.add_options()
        ("interval,i", po::value<int>(&interval)->default_value(1),
            "Size of the aggregation groups in number of ticks (default 1)")
        ("begin,b", po::value<string>(&beginType)->default_value(""),
            "Only begin aggregation once logrec of given type is found")
        ("end,e", po::value<string>(&endType)->default_value(""),
            "Finish aggregation once logrec of given type is found")
    ;
    options.add(agglog);
}

void XctLatency::run()
{
    uint8_t begin = ZeroLogInterface::typeCount;
    uint8_t end = ZeroLogInterface::typeCount;

    for (uint8_t i = 0; i < ZeroLogInterface::typeCount; i++) {
        if (beginType == string(ZeroLogInterface::getTypeString(i)))
        {
            begin = i;
        }
        if (endType == string(ZeroLogInterface::getTypeString(i)))
        {
            end = i;
        }
    }

    LatencyHandler h(interval, begin, end);

    BaseScanner* s = getScanner();
    s->add_handler(&h);
    s->fork();
    s->join();
    delete s;
}

LatencyHandler::LatencyHandler(int interval, uint8_t begin, uint8_t end)
    : interval(interval), currentTick(0), begin(begin), end(end),
    seenBegin(false), accum_latency(0), count(0)
{
    assert(interval > 0);

    if (begin == ZeroLogInterface::typeCount) {
        seenBegin = true;
    }

    cout << "#xct_latency_in_nsec" << endl;
}

void LatencyHandler::invoke(logrec_t& r)
{
    if (!seenBegin) {
        if (r.type() == begin) {
            seenBegin = true;
        }
        else {
            return;
        }
    }

    if (r.type() == end) {
        seenBegin = false;
        return;
    }

    auto type = base_to_enum<LogRecordType>(r.type());
    if (type == LogRecordType::tick_sec_log || type == LogRecordType::tick_msec_log) {
        currentTick++;
        if (currentTick == interval) {
            currentTick = 0;
            dump();
        }
    }
    else if (type == LogRecordType::xct_latency_dump_log) {
        unsigned long latency;
        deserialize_log_fields(&r, latency);
        accum_latency += latency;
        count++;
    }
}

void LatencyHandler::dump()
{
    cout << (count > 0 ? (accum_latency / count) : 0) << endl;
    accum_latency = 0;
    count = 0;
}

void LatencyHandler::finalize()
{
    dump();
}
