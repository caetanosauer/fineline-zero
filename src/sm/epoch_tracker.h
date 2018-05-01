#ifndef EPOCH_TRACKER_H
#define EPOCH_TRACKER_H

#include <array>
#include <atomic>

template <typename Epoch = uint64_t, size_t ArraySize = 8192>
class EpochTracker
{
private:
    using Counter = std::atomic<size_t>;
    std::array<Counter, ArraySize> slots;
    std::atomic<Epoch> first;
    std::atomic<Epoch> last;

    Counter& get_slot(Epoch e) { return slots[e % ArraySize]; }

    static constexpr size_t invalid_value = ~0ul;

public:
    EpochTracker()
    {
        // invariants: last > first && first > 0
        first = 1;
        last = 2;
        for (size_t i = 0; i < ArraySize; i++) {
            slots[i] = 0;
        }
    };

    Epoch acquire()
    {
        while (true) {
            auto current = last.load();
            auto& slot = get_slot(current);
            auto old = slot.load();
            if (old != invalid_value && slot.compare_exchange_strong(old, old+1)) {
                return current;
            }
        }
    }

    void release(Epoch e)
    {
        auto& slot = get_slot(e);
        w_assert1(slot.load() > 0 && slot.load() != invalid_value);
        --slot;
    }

    Epoch get_lowest_active_epoch()
    {
        w_assert1(first < last);
        return first;
    }

    Epoch advance_epoch()
    {
        while (last - first >= ArraySize - 2) {
            // epochs don't overlap and at least one is reserved for resetting below
            std::this_thread::yield();
            try_recycle();
        }
        auto ret = last++;
        try_recycle();
        // Reset value of future epoch (from invalid_value to zero)
        w_assert1(last+1 != first);
        get_slot(last+1) = 0;
        return ret;
    }

private:

    void try_recycle()
    {
        while (first < last-2) {
            auto& slot = get_slot(first);
            if (slot > 0) { return; }
            size_t expected = 0;
            if (slot.compare_exchange_strong(expected, invalid_value)) {
                first++;
            }
            else { return; }
        }
    }
};

#endif
