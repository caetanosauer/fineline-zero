#ifndef XCTLATENCY_H
#define XCTLATENCY_H

#include "command.h"
#include "handler.h"

class XctLatency : public LogScannerCommand {
public:
    void run();
    void setupOptions();

private:
    string beginType;
    string endType;
    int interval;
};

class LatencyHandler : public Handler {
public:
    LatencyHandler(int interval = 1,
            uint8_t begin = ZeroLogInterface::typeCount,
            uint8_t end = ZeroLogInterface::typeCount);
    virtual void invoke(logrec_t& r);
    virtual void finalize();
protected:
    const int interval;
    int currentTick;

    uint8_t begin;
    uint8_t end;
    bool seenBegin;

    unsigned long accum_latency;
    unsigned count;

    void dump();
};

#endif
