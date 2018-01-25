#ifndef LOGREC_SERIALIZE_H
#define LOGREC_SERIALIZE_H

#include "encoding.h"
#include "logrec.h"
#include "logrec_support.h"

template <typename... T>
using LogEncoder = typename foster::VariadicEncoder<foster::InlineEncoder, T...>;

template <typename... T>
void serialize_log_fields(logrec_t* lr, const T&... fields)
{
    char* offset = lr->data();
    char* end = LogEncoder<T...>::encode(offset, fields...);
    lr->set_size(end - offset);
}

template <typename... T>
void deserialize_log_fields(logrec_t* lr, T&... fields)
{
    const char* offset = lr->data();
    LogEncoder<T...>::decode(offset, &fields...);
}

template <kind_t LR>
struct UndoLogrecSerializer
{
    template <typename... T>
    static size_t serialize(const T&...) { return 0; }
};

template <kind_t LR>
struct LogrecSerializer
{
    template <typename PagePtr, typename... T>
    static void serialize(PagePtr /*unused*/, logrec_t* lr, const T&... fields)
    {
        serialize_log_fields(lr, fields...);
    }

//     template <typename PagePtr, typename... T>
//     void deserialize(PagePtr /*unused*/, logrec_t* lr, const T&... fields)
//     {
//         deserialize_log_fields(lr, fields...);
//     }
};

template <>
struct LogrecSerializer<page_img_format_log>
{
    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        auto format =
            new (lr->data()) page_img_format_t(p->get_generic_page());
        lr->set_size(format->size());
    }
};

template <>
struct LogrecSerializer<btree_insert_log>
{
    static void construct(logrec_t* lr, const w_keystr_t& key,
            const cvec_t& el)
    {
        lr->set_size(
             (new (lr->data()) serialized_kv_pair_t(key, el))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct UndoLogrecSerializer<btree_insert_log>
{
    static size_t construct(char* dest, const w_keystr_t& key,
            const cvec_t& /*el*/)
    {
        // Insert undo does not need element data
        cvec_t empty_vec;
        return (new (dest) serialized_kv_pair_t(key, empty_vec))->size();
    }

    template <typename... T>
    static size_t serialize(char* dest, const T&... fields)
    {
        return construct(dest, fields...);
    }
};

template <>
struct LogrecSerializer<btree_compress_page_log>
{
    static void construct(logrec_t* lr,
            const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain)
    {
        uint16_t low_len = low.get_length_as_keystr();
        uint16_t high_len = high.get_length_as_keystr();
        uint16_t chain_len = chain.get_length_as_keystr();

        char* ptr = lr->data();
        memcpy(ptr, &low_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        memcpy(ptr, &high_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        memcpy(ptr, &chain_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);

        low.serialize_as_keystr(ptr);
        ptr += low_len;
        high.serialize_as_keystr(ptr);
        ptr += high_len;
        chain.serialize_as_keystr(ptr);
        ptr += chain_len;

        lr->set_size(ptr - lr->data());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_insert_nonghost_log>
{
    static void construct(logrec_t* lr,
            const w_keystr_t &key, const cvec_t &el)
    {
        lr->set_size(
                (new (lr->data()) serialized_kv_pair_t(key, el))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct UndoLogrecSerializer<btree_insert_nonghost_log>
{
    static size_t construct(char* dest, const w_keystr_t& key,
            const cvec_t& /*el*/)
    {
        // Insert undo does not need element data
        cvec_t empty_vec;
        return (new (dest) serialized_kv_pair_t(key, empty_vec))->size();
    }

    template <typename... T>
    static size_t serialize(char* dest, const T&... fields)
    {
        return construct(dest, fields...);
    }
};

template <>
struct LogrecSerializer<btree_update_log>
{
    static void construct(logrec_t* lr,
        const w_keystr_t& key, const char* /*old_el*/, int /*old_elen*/,
        const cvec_t& new_el)
    {
        // Redo logrec has after-image
        lr->set_size(
             (new (lr->data()) serialized_kv_pair_t(key, new_el))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct UndoLogrecSerializer<btree_update_log>
{
    static size_t construct(char* dest,
        const w_keystr_t& key, const char* old_el, int old_elen,
        const cvec_t& /*new_el*/)
    {
        // Undo logrec has before-image
        return (new (dest) serialized_kv_pair_t(key, cvec_t{old_el, old_elen}))->size();
    }

    template <typename... T>
    static size_t serialize(char* dest, const T&... fields)
    {
        return construct(dest, fields...);
    }
};

template <>
struct LogrecSerializer<btree_overwrite_log>
{
    static void construct(logrec_t* lr, const w_keystr_t&
            key, const char* /*old_el*/, const char *new_el, uint16_t offset,
            size_t elen)
    {
        char* ptr = lr->data();
        memcpy(ptr, &offset, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        // Redo logrec has after-image
        lr->set_size( (new (ptr) serialized_kv_pair_t(
                        key, new_el, elen))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct UndoLogrecSerializer<btree_overwrite_log>
{
    static size_t construct(char* dest, const w_keystr_t&
            key, const char* old_el, const char* /*new_el*/, uint16_t offset,
            size_t elen)
    {
        char* ptr = dest;
        memcpy(ptr, &offset, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        // Undo logrec has before-image
        auto kvp = new (ptr) serialized_kv_pair_t(key, old_el, elen);
        return sizeof(uint16_t) + kvp->size();
    }

    template <typename... T>
    static size_t serialize(char* dest, const T&... fields)
    {
        return construct(dest, fields...);
    }
};

template <>
struct LogrecSerializer<btree_ghost_mark_log>
{
    template <typename PagePtr>
    static void construct(logrec_t* lr, const PagePtr p, const
            vector<slotid_t>& slots, bool is_sys_txn)
    {
        lr->set_size((new (lr->data()) btree_ghost_t<PagePtr>(p, slots,
                        is_sys_txn))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, p, fields...);
    }
};

template <>
struct LogrecSerializer<btree_ghost_reclaim_log>
{
    template <typename PagePtr>
    static void construct(logrec_t* lr, const PagePtr p, const
            vector<slotid_t>& slots)
    {
        lr->set_size((new (lr->data()) btree_ghost_t<PagePtr>(p, slots,
                        false))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, p, fields...);
    }
};

template <>
struct LogrecSerializer<btree_ghost_reserve_log>
{
    static void construct (logrec_t* lr,
        const w_keystr_t& key, int element_length)
    {
        lr->set_size((new (lr->data()) btree_ghost_reserve_t(key,
                        element_length))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_bulk_delete_log>
{
    template <typename PagePtr>
    static void construct (logrec_t* lr, PagePtr p, PageID new_foster_child,
            uint16_t move_count, const w_keystr_t& new_high_fence,
            const w_keystr_t& new_chain)
    {
        btree_bulk_delete_t* bulk =
            new (lr->data()) btree_bulk_delete_t(
                    new_foster_child, move_count,
                    new_high_fence, new_chain);
        lr->set_size(bulk->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, p, fields...);
    }
};

template <>
struct LogrecSerializer<btree_foster_adopt_log>
{
    template <typename PagePtr>
    static void construct (logrec_t* lr, PagePtr p,
            PageID new_child_pid, lsn_t new_child_emlsn, const w_keystr_t&
            new_child_key)
    {
        lr->set_size((new (lr->data()) btree_foster_adopt_t(
                        new_child_pid, new_child_emlsn,
                        new_child_key))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, p, fields...);
    }
};

#endif
