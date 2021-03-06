#ifndef SM_THREAD_WRAPPER_H
#define SM_THREAD_WRAPPER_H

#include <thread>
#include <memory>

#include "w_rc.h"
#include "tls.h"
#include "smthread.h"

// SM version of finelog's thread_wrapper_t that initializes TLS memory management and handles SM stats
class sm_thread_wrapper_t
{
public:

    sm_thread_wrapper_t()
    {
    }

    virtual ~sm_thread_wrapper_t()
    {
        thread_ptr.reset();
    }

    /*
     * Virtual methods to be overridden by sub-classes.
     */
    virtual void run() = 0;
    virtual void before_run() {};
    virtual void after_run() {};

    void spawn()
    {
        // CS TODO: these are required for the old shore TLS allocator, which is still used.
        // With C++11 and the thread_local specifier, it should be much easier to perform this
        // type of static initialization;
        tls_tricks::tls_manager::thread_init();
        smthread_t::add_me_to_thread_list();

        before_run();
        run();
        after_run();

        // save my stats before leaving
        smlevel_0::add_to_global_stats(smthread_t::TL_stats()); // before detaching them
        smthread_t::remove_me_from_thread_list();
        tls_tricks::tls_manager::thread_fini();

        // latch_t maintains some static data structures that must be deleted manually
        // latch_t::on_thread_destroy();
    }

    void fork()
    {
        thread_ptr.reset(new std::thread ([this] { spawn(); }));
    }

    void join()
    {
        if (thread_ptr) {
            thread_ptr->join();
            thread_ptr = nullptr;
        }
    }

private:
    std::unique_ptr<std::thread> thread_ptr;
};

#endif
