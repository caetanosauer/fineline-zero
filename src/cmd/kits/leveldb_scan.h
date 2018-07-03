#pragma once

#ifdef USE_LEVELDB
class leveldb_cursor_t
{
private:
    leveldb::Iterator* cursor;
    Database* _db;

    bool _forward = true;
    bool _consumed_first = false;

    w_keystr_t _key;
    char _elem [SM_PAGESIZE];
    size_t _elen;

    w_keystr_t _stop_condition;
    bool _stop_inclusive = false;

public:
     leveldb_cursor_t(Database* db, StoreID stid, bool forward)
        : _db(db), _forward(forward)
     {
         w_assert1(stid != 0);
         cursor = _db->NewIterator(leveldb::ReadOptions());
         char stidPrefix = static_cast<char>(stid);
         char openPrefix[2] = {'+', _forward ? stidPrefix : (stidPrefix+1)};
         cursor->Seek(leveldb::Slice{&openPrefix[0], 2});
         if (!_forward && cursor->Valid()) { cursor->Prev(); }
         char stopPrefix = forward ? (stidPrefix+1) : (stidPrefix-1);
         _stop_condition.construct_regularkey(&stopPrefix, sizeof(char));
         w_assert1(cursor->key().data()[0] == '+');
         w_assert1(cursor->key().data()[1] == stid);
     }

     leveldb_cursor_t(Database* db, StoreID stid, char* bound, int bsize, bool inclusive, bool forward)
        : _db(db), _forward(forward)
     {
         w_assert1(stid != 0);
         cursor = _db->NewIterator(leveldb::ReadOptions());
         char stidPrefix = static_cast<char>(stid);

         // Use _elem as a temporary buffer for the seek key
         char* pos = _elem;
         *pos = '+';
         pos++;
         // *pos = stidPrefix
         // pos++;
         memcpy(pos, bound, bsize);
         pos += bsize;
         cursor->Seek(leveldb::Slice{_elem, pos - _elem});

         char stopPrefix = forward ? (stidPrefix+1) : (stidPrefix-1);
         _stop_condition.construct_regularkey(&stopPrefix, sizeof(char));
         if (!inclusive) { _consumed_first = true; }
     }

     leveldb_cursor_t(Database* db, StoreID stid,
           char* lower, int lsize, bool lower_incl,
           char* upper, int usize, bool upper_incl,
           bool forward)
        : _db(db), _forward(forward)
     {
         w_assert1(stid != 0);
         cursor = _db->NewIterator(leveldb::ReadOptions());
         char stidPrefix = static_cast<char>(stid);

         // Use _elem as a temporary buffer for the stop condition
         // (note: stop contidion should already have the stid encoded)
         char* pos = _elem;
         if (!forward) {
            memcpy(pos, lower, lsize);
            pos += lsize;
         } else {
            memcpy(pos, upper, usize);
            pos += usize;
         }
         _stop_condition.construct_regularkey(_elem, pos - _elem);

         // Use _elem as a temporary buffer for the seek key
         // Seek key should already contain stid
         pos = _elem;
         *pos = '+';
         pos++;
         // *pos = stidPrefix;
         // pos++;
         if (forward) {
            memcpy(pos, lower, lsize);
            pos += lsize;
         } else {
            memcpy(pos, upper, usize);
            pos += usize;
         }
         cursor->Seek(leveldb::Slice{_elem, pos - _elem});

         if (forward) {
            // TODO we actually have to call next() for as long as the key equals lower
            w_assert1(lower_incl);
            // _stop_inclusive = upper_incl;
         } else {
            // TODO we actually have to call next() for as long as the key equals upper
            w_assert1(upper_incl);
            // _stop_inclusive = lower_incl;
         }
     }

     ~leveldb_cursor_t()
     {
         if (cursor) { w_assert0(cursor->status().ok()); delete cursor; }
     }

     rc_t next()
     {
         w_assert1(cursor);
         if (eof()) { return RCOK; }

         if (!_consumed_first) {
            _consumed_first = true;
         } else if (_forward) {
             cursor->Next();
         } else {
             cursor->Prev();
         }

         w_assert1(cursor->key().size() > 2);
         _key.construct_from_keystr(cursor->key().data(), cursor->key().size());
         _elen = cursor->value().size();
         memcpy(_elem, cursor->value().data(), _elen);
         return RCOK;
     }

     bool eof() {
        if (!cursor->Valid()) { return true; }

        // if _key is still null, we haven't fetched anything yet
        if (!_key.buffer_as_keystr()) { return false; }

        int cmp = _stop_condition.compare(_key);
        if (_forward) {
           return _stop_inclusive ? (cmp < 0) : (cmp <= 0);
        } else {
           return _stop_inclusive ? (cmp > 0) : (cmp >= 0);
        }
     }

     char* elem() { return _elem; }
     size_t& elen() { return _elen; }
     w_keystr_t& key() {
         w_assert1(reinterpret_cast<const char*>(_key.buffer_as_keystr())[0] == '+');
         w_assert1(reinterpret_cast<const char*>(_key.buffer_as_keystr())[1] != 0);
         return _key;
     }
};
#endif
