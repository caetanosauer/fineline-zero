#ifndef AGGLOG_H
#define AGGLOG_H

#include "command.h"
#include "handler.h"

#include <vector>
#include <bitset>

class AggLog : public LogScannerCommand {
public:
    void run();
    void setupOptions();
    string jsonReply();

private:
    vector<string> typeStrings;
    string beginType;
    string endType;
    string json;
    int interval;
};

class AggregateHandler : public Handler {
public:
    AggregateHandler(bitset<ZeroLogInterface::typeCount> filter, int interval = 1,
            uint8_t begin = ZeroLogInterface::typeCount,
            uint8_t end = ZeroLogInterface::typeCount);
    virtual void invoke(logrec_t& r);
    virtual void finalize();
    string jsonReply();

protected:
    vector<unsigned> counts;
    bitset<ZeroLogInterface::typeCount> filter;
    const int interval;
    int currentTick, jsonResultIndex;
    std::stringstream ssJsonResult[ZeroLogInterface::typeCount];

    uint8_t begin;
    uint8_t end;
    bool seenBegin;

    void dumpCounts();
};

#endif
