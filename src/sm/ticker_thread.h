#pragma once

#include "sm_thread_wrapper.h"
#include "xct_logger.h"
#include <fstream>

class ticker_thread_t : public sm_thread_wrapper_t
{
public:
    ticker_thread_t(bool msec = false, bool print_tput = true, bool log_ticks = true)
        : msec(msec), print_tput(print_tput), log_ticks(log_ticks), even_round(true)
    {
        interval_usec = 1000; // 1ms
        if (!msec) { interval_usec *= 1000; }
        stop = false;

        stats[0].fill(0);
        stats[1].fill(0);
    }

    void shutdown()
    {
        stop = true;
    }

    void run()
    {
        std::ofstream ofs;
        std::ofstream ofs2;
        if (print_tput) {
            ofs.open("tput.txt", std::ofstream::out | std::ofstream::trunc);
            // ofs2.open("evict_time.txt", std::ofstream::out | std::ofstream::trunc);
        }

        while (true) {
            if (stop) { break; }

            std::this_thread::sleep_for(std::chrono::microseconds(interval_usec));

            if (print_tput) {
                auto& st = stats[even_round ? 0 : 1];
                auto& prev_st = stats[even_round ? 1 : 0];

                ss_m::gather_stats(st);
                auto diff = st[enum_to_base(sm_stat_id::commit_xct_cnt)] -
                    prev_st[enum_to_base(sm_stat_id::commit_xct_cnt)];
                ofs << diff << std::endl;

                // auto duration = st[enum_to_base(sm_stat_id::bf_evict_duration)] -
                //     prev_st[enum_to_base(sm_stat_id::bf_evict_duration)];
                // auto count = st[enum_to_base(sm_stat_id::bf_evict)] -
                //     prev_st[enum_to_base(sm_stat_id::bf_evict)];
                // auto evict_attempts = st[enum_to_base(sm_stat_id::bf_eviction_attempts)] -
                //     prev_st[enum_to_base(sm_stat_id::bf_eviction_attempts)];
                // auto evict_time = count > 0 ? (duration / count) : 0;

                // ofs2 << evict_time << "\t" << evict_attempts << std::endl;
            }

            if (log_ticks) {
               if (msec) { Logger::log_sys<LogRecordType::tick_msec_log>(); }
               else { Logger::log_sys<LogRecordType::tick_sec_log>(); }
            }

            even_round ^= true;
        }

        if (print_tput) { ofs.close(); }
        // if (print_tput) { ofs2.close(); }
    }

private:
    int interval_usec;
    bool msec;
    bool print_tput;
    bool log_ticks;
    std::atomic<bool> stop;

    std::array<sm_stats_t, 2> stats;
    bool even_round;
};
