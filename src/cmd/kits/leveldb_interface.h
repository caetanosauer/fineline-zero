#pragma once

#ifdef USE_LEVELDB

#include <atomic>
#include <string>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>

#include "w_key.h"
#include "finelog_basics.h"

struct LevelDBTxn
{
   leveldb::WriteBatch wb;
   unsigned count{0};
};

class LevelDBInterface
{
private:
   static LevelDBTxn& getTxn()
   {
      static thread_local LevelDBTxn txn;
      return txn;
   }

public:

   static bool useWriteBatches;

   static bool levelDBProbe(leveldb::DB* db, const w_keystr_t& kstr, char* dest)
   {
      std::string value;
      // cout << "Probing key: ";
      // for (int j = 0; j < kstr.get_length_as_keystr(); j++) {
      //    unsigned char c = reinterpret_cast<const unsigned char*>(kstr.buffer_as_keystr())[j];
      //    cout << static_cast<unsigned>(c) << " ";
      // }
      // cout << endl;
      auto status = db->Get(leveldb::ReadOptions(), keystrToSlice(kstr), &value);
      if (status.IsNotFound()) {
         return false;
      } else {
         w_assert1(status.ok());
         w_assert1(value.length() > 0);
         ::memcpy(dest, value.c_str(), value.length());
         // cout << "Probe returns value: ";
         // for (int j = 0; j < value.length(); j++) {
         //    unsigned char c = value.c_str()[j];
         //    cout << static_cast<unsigned>(c) << " ";
         // }
         // cout << endl;
         return true;
      }
   }

   static void beginTxn()
   {
      if (useWriteBatches) {
         w_assert1(getTxn().count == 0);
      }
   }

   static void commitTxn(leveldb::DB* db)
   {
      if (useWriteBatches) {
         auto& txn = getTxn();
         if (txn.count > 0) {
            auto status = db->Write(leveldb::WriteOptions(), &txn.wb);
            w_assert1(status.ok());
            txn.count = 0;
         }
      }
   }

   static void abortTxn()
   {
      if (useWriteBatches) {
         auto& txn = getTxn();
         txn.wb.Clear();
         txn.count = 0;
      }
   }

   static void levelDBInsert(leveldb::DB* db, const w_keystr_t& kstr, char* data, size_t len)
   {
      // Check that key prefixes conform to Zero's format and that stid is present
      w_assert1(reinterpret_cast<const char*>(kstr.buffer_as_keystr())[0] == '+');
      w_assert1(reinterpret_cast<const char*>(kstr.buffer_as_keystr())[1] != 0);

      if (useWriteBatches) {
         auto& txn = getTxn();
         txn.wb.Put(keystrToSlice(kstr), leveldb::Slice{data, len});
         txn.count++;
      } else {
         db->Put(leveldb::WriteOptions(), keystrToSlice(kstr), leveldb::Slice{data, len});
      }
   }

   static void levelDBDelete(leveldb::DB* db, const w_keystr_t& kstr)
   {
      if (useWriteBatches) {
         auto& txn = getTxn();
         txn.wb.Delete(keystrToSlice(kstr));
         txn.count++;
      } else {
         db->Delete(leveldb::WriteOptions(), keystrToSlice(kstr));
      }
   }

   static leveldb::Slice keystrToSlice(const w_keystr_t& kstr)
   {
      return leveldb::Slice{reinterpret_cast<const char*>(kstr.buffer_as_keystr()), kstr.get_length_as_keystr()};
   }

};
#endif
