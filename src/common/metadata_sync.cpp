#include "metadata_sync.h"

namespace MetadataSync {

std::atomic<bool> steamToolsPresent{false};
std::atomic<bool> syncLuas{false};
// Default ON: stats/playtime sync is the expected behavior; the user opts out.
std::atomic<bool> syncAchievements{true};
std::atomic<bool> syncPlaytime{true};
// Default OFF: experimental opt-in feature.
std::atomic<bool> schemaFetch{false};

}
