// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "core/expire_period.h"
#include "core/intent_lock.h"
#include "server/cluster/cluster_config.h"
#include "server/conn_context.h"
#include "server/detail/table.h"
#include "server/top_keys.h"

namespace dfly {

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using ExpireTable = DashTable<PrimeKey, ExpirePeriod, detail::ExpireTablePolicy>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid if a different entry in the table was mutated.
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;
using ExpireIterator = ExpireTable::iterator;
using ExpireConstIterator = ExpireTable::const_iterator;

inline bool IsValid(PrimeIterator it) {
  return !it.is_done();
}

inline bool IsValid(ExpireIterator it) {
  return !it.is_done();
}

inline bool IsValid(PrimeConstIterator it) {
  return !it.is_done();
}

inline bool IsValid(ExpireConstIterator it) {
  return !it.is_done();
}

struct SlotStats {
  uint64_t key_count = 0;
  uint64_t total_reads = 0;
  uint64_t total_writes = 0;
  uint64_t memory_bytes = 0;
  SlotStats& operator+=(const SlotStats& o);
};

struct DbTableStats {
  // Number of inline keys.
  uint64_t inline_keys = 0;

  // Object memory usage besides hash-table capacity.
  // Applies for any non-inline objects.
  size_t obj_memory_usage = 0;

  size_t listpack_blob_cnt = 0;
  size_t listpack_bytes = 0;
  size_t tiered_entries = 0;
  size_t tiered_size = 0;

  std::array<size_t, OBJ_TYPE_MAX> memory_usage_by_type = {};

  // Mostly used internally, exposed for tiered storage.
  void AddTypeMemoryUsage(unsigned type, int64_t delta);

  DbTableStats& operator+=(const DbTableStats& o);
};

// We use LockKey for LockTable keys because of the multi transactions
// that unlock asynchronously. We must ensure the existence of keys outside of
// multi-transaction lifecycle.

class LockKey {
 public:
  explicit LockKey(std::string_view str) : val_(str) {
  }
  explicit LockKey(const std::string& str) : val_(str) {
  }

  std::string_view AsView() const {
    if (std::holds_alternative<std::string_view>(val_)) {
      return std::get<std::string_view>(val_);
    } else {
      return std::get<std::string>(val_);
    }
  }

  bool operator==(std::string_view str) const {
    return AsView() == str;
  }

  bool operator==(const LockKey& o) const {
    return *this == o.AsView();
  }

  LockKey& operator=(std::string_view str) {
    val_.emplace<std::string_view>(str);
    return *this;
  }

  LockKey& operator=(std::string str) {
    val_.emplace<std::string_view>(str);
    return *this;
  }

  struct Hasher {
    using is_transparent = void;  // to allow heterogeneous lookups.

    size_t operator()(const LockKey& o) const {
      return absl::Hash<std::string_view>{}(o.AsView());
    }

    size_t operator()(std::string_view s) const {
      return absl::Hash<std::string_view>{}(s);
    }
  };

  struct Eq {
    using is_transparent = void;  // to allow heterogeneous lookups.

    bool operator()(const LockKey& left, const LockKey& right) const {
      return left == right;
    }

    bool operator()(const LockKey& left, std::string_view right) const {
      return left == right;
    }
  };

  friend std::ostream& operator<<(std::ostream& stream, const LockKey& lk) {
    stream << lk.AsView();
    return stream;
  }

 private:
  std::variant<std::string_view, std::string> val_;
};

using LockTable = absl::flat_hash_map<LockKey, IntentLock, LockKey::Hasher, LockKey::Eq>;

// A single Db table that represents a table that can be chosen with "SELECT" command.
struct DbTable : boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> {
  PrimeTable prime;
  ExpireTable expire;
  DashTable<PrimeKey, uint32_t, detail::ExpireTablePolicy> mcflag;

  // Contains transaction locks
  LockTable trans_locks;

  // Stores a list of dependant connections for each watched key.
  absl::flat_hash_map<std::string, std::vector<ConnectionState::ExecInfo*>> watched_keys;

  mutable DbTableStats stats;
  std::vector<SlotStats> slots_stats;
  ExpireTable::Cursor expire_cursor;

  TopKeys top_keys;
  DbIndex index;

  explicit DbTable(PMR_NS::memory_resource* mr, DbIndex index);
  ~DbTable();

  void Clear();
};

// We use reference counting semantics of DbTable when doing snapshotting.
// There we need to preserve the copy of the table in case someone flushes it during
// the snapshot process. We copy the pointers in StartSnapshotInShard function.
using DbTableArray = std::vector<boost::intrusive_ptr<DbTable>>;

}  // namespace dfly
