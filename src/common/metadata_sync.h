#pragma once
#include <cstdint>
#include <atomic>

namespace MetadataSync {

extern std::atomic<bool> steamToolsPresent;
extern std::atomic<bool> syncLuas;

// Native stats/playtime sync gates (config: sync_achievements / sync_playtime).
// When false, the corresponding native path does NOT interfere at all: stats
// pass straight through to Steam's real server (no import/synthesize), and
// playtime is neither tracked nor merged. Default true (sync enabled).
extern std::atomic<bool> syncAchievements;
extern std::atomic<bool> syncPlaytime;

// Experimental: proactively fetch missing achievement/stats schemas from the CM
// (config: experimental_schema_fetch). When false, no schema requests are sent.
// Default false (opt-in experimental feature).
extern std::atomic<bool> schemaFetch;

inline bool IsEnabled() {
    return steamToolsPresent.load(std::memory_order_relaxed) &&
           syncLuas.load(std::memory_order_relaxed);
}

}
