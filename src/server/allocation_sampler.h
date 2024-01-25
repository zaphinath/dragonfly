// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <mimalloc.h>

#include <atomic>
#include <mutex>

#include "absl/base/internal/spinlock.h"
#include "absl/container/flat_hash_map.h"
#include "base/logging.h"
#include "src/core/fibers.h"
#include "util/fibers/stacktrace.h"

namespace dfly {

// A simple (read: naive) implementation of memory allocations tracking.
//
// It can track all memory allocations and releases via `new` and `delete`, and can print what
// it had found, along with full call stacks.
//
// Limitations:
// * Currently it only tracks `new` and `delete`, but can be wrapped around mimalloc API to also
//   track that memory as well.
// * It may miss tracking allocations in another thread, while a current thread is book keeping an
//   allocation.
// * Tracking is slow, and should not be performed in production (or enabled in an official
//   release).
//
// Usage:
// 1. #define INJECT_ALLOCATION_SAMPLER
// 2. AllocationSampler::Get().Enable()
// 3. Do whatever you're investigating
// 4. AllocationSampler::Get().Print()
// 5. AllocationSampler::Get().Disable()
//
// TODOs:
// * Allow only printing un-released memory
// * Support the case of new, which returns a previously-tracked address instead of overriding it
// * Add ability to sample every X allocations
// * Output to some format which will allow using with tools such as pprof

class AllocationSampler {
 public:
  static AllocationSampler& Get();

  void Enable();
  void Disable();
  bool IsEnabled() const;

  void TrackNew(void* address, size_t size);
  void TrackDelete(void* address);

  void Print();

 private:
  struct Entry {
    std::string callstack;
    size_t size = 0;
    bool released = false;
  };

  std::atomic_bool enabled_ = false;
  absl::base_internal::SpinLock mutex_;
  absl::flat_hash_map<void*, Entry> entries_ ABSL_GUARDED_BY(mutex_);
};
}  // namespace dfly

#ifdef INJECT_ALLOCATION_SAMPLER

// Code here is copied from mimalloc-new-delete, and modified to add tracking
void operator delete(void* p) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free(p);
};
void operator delete[](void* p) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free(p);
};

void operator delete(void* p, const std::nothrow_t&) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free(p);
}
void operator delete[](void* p, const std::nothrow_t&) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free(p);
}

void* operator new(std::size_t n) noexcept(false) {
  auto v = mi_new(n);
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}
void* operator new[](std::size_t n) noexcept(false) {
  auto v = mi_new(n);
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}

void* operator new(std::size_t n, const std::nothrow_t& tag) noexcept {
  (void)(tag);
  auto v = mi_new_nothrow(n);
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}
void* operator new[](std::size_t n, const std::nothrow_t& tag) noexcept {
  (void)(tag);
  auto v = mi_new_nothrow(n);
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}

#if (__cplusplus >= 201402L || _MSC_VER >= 1916)
void operator delete(void* p, std::size_t n) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_size(p, n);
};
void operator delete[](void* p, std::size_t n) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_size(p, n);
};
#endif

#if (__cplusplus > 201402L || defined(__cpp_aligned_new))
void operator delete(void* p, std::align_val_t al) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_aligned(p, static_cast<size_t>(al));
}
void operator delete[](void* p, std::align_val_t al) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_aligned(p, static_cast<size_t>(al));
}
void operator delete(void* p, std::size_t n, std::align_val_t al) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_size_aligned(p, n, static_cast<size_t>(al));
};
void operator delete[](void* p, std::size_t n, std::align_val_t al) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_size_aligned(p, n, static_cast<size_t>(al));
};
void operator delete(void* p, std::align_val_t al, const std::nothrow_t&) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_aligned(p, static_cast<size_t>(al));
}
void operator delete[](void* p, std::align_val_t al, const std::nothrow_t&) noexcept {
  dfly::AllocationSampler::Get().TrackDelete(p);
  mi_free_aligned(p, static_cast<size_t>(al));
}

void* operator new(std::size_t n, std::align_val_t al) noexcept(false) {
  auto v = mi_new_aligned(n, static_cast<size_t>(al));
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}
void* operator new[](std::size_t n, std::align_val_t al) noexcept(false) {
  auto v = mi_new_aligned(n, static_cast<size_t>(al));
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}
void* operator new(std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
  auto v = mi_new_aligned_nothrow(n, static_cast<size_t>(al));
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}
void* operator new[](std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
  auto v = mi_new_aligned_nothrow(n, static_cast<size_t>(al));
  dfly::AllocationSampler::Get().TrackNew(v, n);
  return v;
}
#endif

#endif  // INJECT_ALLOCATION_SAMPLER
