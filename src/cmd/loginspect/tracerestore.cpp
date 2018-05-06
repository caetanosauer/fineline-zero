#include "tracerestore.h"

#include <iostream>

void RestoreTrace::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void RestoreTrace::run()
{
    RestoreTraceHandler h;
    BaseScanner* s = getScanner();
    s->add_handler(&h);
    s->fork();
    s->join();
    delete s;
}

RestoreTraceHandler::RestoreTraceHandler()
    : currentTick(0)
{
}

void RestoreTraceHandler::invoke(logrec_t& r)
{
    auto type = base_to_enum<LogRecordType>(r.type());
    if (type == LogRecordType::tick_sec_log || type == LogRecordType::tick_msec_log) {
        currentTick++;
    }
    else if (type == LogRecordType::restore_segment_log) {
        uint32_t segment = *((uint32_t*) r.data());
        std::cout << currentTick << " " << segment << std::endl;
    }
}
