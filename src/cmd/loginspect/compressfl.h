#ifndef COMPRESSFL_H
#define COMPRESSFL_H

#include <unordered_map>

#include "command.h"
#include "handler.h"

class CompressFl : public Command
{
public:
    void setupOptions();
    void run();

private:
    string logdir;
    string archdir;
};

#endif

