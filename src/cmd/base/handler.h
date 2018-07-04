#ifndef HANDLER_H
#define HANDLER_H

#include "logrec.h"
#include "generic_page.h"

#include <fstream>
#include <sstream>
#include <unordered_set>
#include <algorithm>

class Handler {
public:
    Handler() : hout(std::cout)
    {}

    virtual ~Handler() {};

    virtual void initialize() {};
    virtual void finalize() {};

    virtual void invoke(logrec_t& r)
    {
    }

    virtual void invoke(logrec_t &r, lsn_t)
    {
        invoke(r);
    }

    virtual void newFile(const char* /* fname */) {};

    Handler(const Handler&) = delete;
    Handler& operator=(const Handler&) = delete;

    void setFileOutput(string fpath)
    {
        fileOutput.reset(new ofstream(fpath));
        hout = *fileOutput;
    }

protected:
    ostream& out() { return hout.get(); }

private:
    reference_wrapper<ostream> hout;
    unique_ptr<ofstream> fileOutput;
};

class PageHandler {
public:
    virtual void finalize() {};
    virtual void handle(const generic_page& page) = 0;
};

class StoreHandler {
public:
    virtual void finalize() {};
    virtual void handle(const StoreID&) = 0;
};

#endif
