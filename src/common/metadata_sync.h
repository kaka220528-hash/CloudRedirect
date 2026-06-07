#pragma once
#include <cstdint>
#include <atomic>

namespace MetadataSync {

extern std::atomic<bool> steamToolsPresent;
extern std::atomic<bool> syncLuas;

inline bool IsEnabled() {
    return steamToolsPresent.load(std::memory_order_relaxed) &&
           syncLuas.load(std::memory_order_relaxed);
}

}
