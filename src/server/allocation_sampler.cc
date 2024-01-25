#include "allocation_sampler.h"

namespace dfly {

namespace {
static AllocationSampler g_sampler;
}

AllocationSampler& AllocationSampler::Get() {
  return g_sampler;
}

void AllocationSampler::Enable() {
  enabled_ = true;
}

void AllocationSampler::Disable() {
  enabled_ = false;
}

bool AllocationSampler::IsEnabled() const {
  return enabled_.load();
}

void AllocationSampler::TrackNew(void* address, size_t size) {
  if (!IsEnabled()) {
    return;
  }

  absl::base_internal::SpinLockHolder lock{&mutex_};
  // disable tracking for internal memory tracking to not be included
  enabled_ = false;
  entries_[address] = {
      .callstack = util::fb2::GetStacktrace(),
      .size = size,
  };
  enabled_ = true;
}

void AllocationSampler::TrackDelete(void* address) {
  if (!IsEnabled()) {
    return;
  }

  absl::base_internal::SpinLockHolder lock{&mutex_};
  auto it = entries_.find(address);
  if (it != entries_.end()) {
    it->second.released = true;
  }
}

void AllocationSampler::Print() {
  if (!IsEnabled()) {
    LOG(ERROR) << "Can't print allocation sampling when disabled.";
    return;
  }

  LOG(INFO) << ">>> Printing allocations";
  absl::base_internal::SpinLockHolder lock{&mutex_};
  for (const auto& [_, entry] : entries_) {
    LOG(INFO) << "Allocated=" << entry.size << " bytes, released=" << entry.released
              << ", stack: " << entry.callstack;
  }
  LOG(INFO) << ">>> Finished printing allocations";
}

}  // namespace dfly
