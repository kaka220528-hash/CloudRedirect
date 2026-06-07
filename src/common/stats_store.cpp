#include "stats_store.h"
#include "json.h"
#include "vdf.h"
#include "log.h"

#include <fstream>
#include <sstream>
#include <filesystem>
#include <chrono>
#include <algorithm>
#include <cstring>

namespace fs = std::filesystem;

namespace StatsStore {

static std::string g_storageRoot;
static std::string g_steamPath;   // e.g. "C:\Games\Steam\" (used to locate native blobs)
static std::mutex g_mutex;

// Forward decl: deterministic CRC over an AppStats (caller holds g_mutex).
uint32_t ComputeCrcLocked(const AppStats& stats);
static std::unordered_map<uint32_t, AppStats> g_cache;
static std::unordered_map<uint32_t, bool> g_dirty;

// Cloud-backing provider (installed by the platform layer; see SetCloudProvider).
static CloudPullFn g_cloudPull;
static CloudPushFn g_cloudPush;

// Resolves the current Steam accountId for locating native UserGameStats blobs.
static AccountIdProvider g_accountIdProvider;

void SetCloudProvider(CloudPullFn pull, CloudPushFn push) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_cloudPull = std::move(pull);
    g_cloudPush = std::move(push);
}

void SetAccountIdProvider(AccountIdProvider provider) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_accountIdProvider = std::move(provider);
}

// ── Native UserGameStats (BKV) reader ────────────────────────────────────
// Steam stores per-user stats as a binary-KV tree in
//   appcache\stats\UserGameStats_<accountId>_<appId>.bin
// Tree shape:
//   cache (SECTION)
//     ├── crc (INT)            -- Steam's own token (we recompute our own)
//     ├── PendingChanges (INT)
//     └── <statId> (SECTION)   -- decimal-string name
//           ├── data (INT/FLOAT/UINT64/INT64)   -- stat value (achievement: bitfield)
//           └── AchievementTimes (SECTION)       -- optional
//                 └── <bit> (INT)  -- unlock unix timestamp per bit index
// Parser mirrors the (now-removed) bkv_stats.cpp reader.
namespace {

enum BkvType : uint8_t {
    BKV_SECTION = 0x00,
    BKV_STRING  = 0x01,
    BKV_INT     = 0x02,
    BKV_FLOAT   = 0x03,
    BKV_UINT64  = 0x07,
    BKV_END     = 0x08,
    BKV_INT64   = 0x0A,
};

struct BkvNode {
    BkvType type{};
    std::string name;
    uint32_t intVal = 0;
    float    floatVal = 0.0f;
    uint64_t uint64Val = 0;
    int64_t  int64Val = 0;
    std::string strVal;
    std::vector<BkvNode> children;
};

constexpr int    BKV_MAX_DEPTH = 128;
constexpr size_t BKV_MAX_NODES = 100000;

bool BkvRead(const uint8_t* data, size_t len, size_t& pos,
             std::vector<BkvNode>& out, int depth, size_t& totalNodes) {
    if (depth > BKV_MAX_DEPTH) return false;
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == BKV_END) return true;

        BkvNode node;
        node.type = static_cast<BkvType>(tag);

        const char* nameStart = reinterpret_cast<const char*>(data + pos);
        size_t nameEnd = pos;
        while (nameEnd < len && data[nameEnd] != 0) nameEnd++;
        if (nameEnd >= len) return false;
        node.name.assign(nameStart, nameEnd - pos);
        pos = nameEnd + 1;

        switch (node.type) {
        case BKV_SECTION:
            if (!BkvRead(data, len, pos, node.children, depth + 1, totalNodes))
                return false;
            break;
        case BKV_STRING: {
            const char* s = reinterpret_cast<const char*>(data + pos);
            size_t end = pos;
            while (end < len && data[end] != 0) end++;
            if (end >= len) return false;
            node.strVal.assign(s, end - pos);
            pos = end + 1;
            break;
        }
        case BKV_INT:
        case BKV_FLOAT:
            if (pos + 4 > len) return false;
            if (node.type == BKV_INT) std::memcpy(&node.intVal, data + pos, 4);
            else                      std::memcpy(&node.floatVal, data + pos, 4);
            pos += 4;
            break;
        case BKV_UINT64:
            if (pos + 8 > len) return false;
            std::memcpy(&node.uint64Val, data + pos, 8);
            pos += 8;
            break;
        case BKV_INT64:
            if (pos + 8 > len) return false;
            std::memcpy(&node.int64Val, data + pos, 8);
            pos += 8;
            break;
        default:
            return false;
        }
        if (++totalNodes > BKV_MAX_NODES) return false;
        out.push_back(std::move(node));
    }
    return depth == 0;
}

const BkvNode* BkvFind(const std::vector<BkvNode>& nodes, const std::string& name) {
    for (const auto& n : nodes)
        if (n.name == name) return &n;
    return nullptr;
}

// Coerce a "data" node's numeric value to uint32 (stat values and achievement
// bitfields are stored as INT; we only need the 32-bit payload for the wire).
uint32_t BkvDataAsU32(const BkvNode& dataNode) {
    switch (dataNode.type) {
        case BKV_INT:    return dataNode.intVal;
        case BKV_UINT64: return (uint32_t)dataNode.uint64Val;
        case BKV_INT64:  return (uint32_t)dataNode.int64Val;
        case BKV_FLOAT: { uint32_t v; std::memcpy(&v, &dataNode.floatVal, 4); return v; }
        default:         return 0;
    }
}

} // namespace

// Active play sessions: appId -> session start (unix time)
static std::unordered_map<uint32_t, uint32_t> g_activeSessions;

static uint32_t NowUnix() {
    return (uint32_t)std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static std::string StatsPath(uint32_t appId) {
    return g_storageRoot + "/" + std::to_string(appId) + ".json";
}

static std::string SchemaPath(uint32_t appId) {
    return g_storageRoot + "/schemas/" + std::to_string(appId) + ".bin";
}

// Simple CRC32 over stat id/value pairs
static uint32_t Crc32(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(int32_t)(crc & 1)));
        }
    }
    return ~crc;
}

// Reconcile playtime from Steam's localconfig.vdf.
// Steam writes Playtime/Playtime2wks/LastPlayed under
//   UserLocalConfigStore > Software > Valve > Steam > Apps > {appid}
// If localconfig has more playtime than our stats JSON, update ours.
// This catches sessions where the user played without CloudRedirect loaded.
static void ReconcileLocalConfig(const std::string& cloudRoot, const std::string& steamPath) {
    std::error_code ec;
    fs::path userdataDir = fs::path(steamPath) / "userdata";
    if (!fs::exists(userdataDir, ec)) return;

    // Only reconcile accounts that have storage in our cloud_redirect directory
    fs::path storageDir = fs::path(cloudRoot) / "storage";
    std::vector<std::string> ourAccounts;
    if (fs::exists(storageDir, ec)) {
        for (auto& entry : fs::directory_iterator(storageDir, ec)) {
            if (entry.is_directory())
                ourAccounts.push_back(entry.path().filename().string());
        }
    }
    if (ourAccounts.empty()) return;

    int reconciled = 0;
    for (auto& acctId : ourAccounts) {
        fs::path acctDir = userdataDir / acctId;
        if (!fs::is_directory(acctDir, ec)) continue;

        fs::path lcPath = acctDir / "config" / "localconfig.vdf";
        if (!fs::exists(lcPath, ec)) continue;

        std::ifstream f(lcPath);
        if (!f.good()) continue;
        std::string vdf((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
        f.close();
        if (vdf.empty()) continue;

        // Find the "Apps" section: UserLocalConfigStore > Software > Valve > Steam > Apps
        const char* basePath[] = {"UserLocalConfigStore", "Software", "Valve", "Steam", "Apps"};
        size_t appsStart = 0, appsEnd = 0;
        if (!VdfUtil::FindVdfSectionRange(vdf, basePath, 5, appsStart, appsEnd))
            continue;

        // Enumerate child sections (each is an appid)
        VdfUtil::ForEachChildInSection(vdf, basePath, 5, [&](std::string_view name) -> bool {
            uint32_t appId = 0;
            // Parse appid from section name
            for (char c : name) {
                if (c < '0' || c > '9') return true; // skip non-numeric
                appId = appId * 10 + (c - '0');
            }
            if (appId == 0) return true;

            // Only reconcile apps we already track (namespace apps with stats JSON)
            if (!fs::exists(StatsPath(appId), ec)) return true;

            // Read Playtime/Playtime2wks/LastPlayed from this app's VDF section
            std::string appIdStr = std::to_string(appId);
            const char* appPath[] = {"UserLocalConfigStore", "Software", "Valve", "Steam", "Apps", appIdStr.c_str()};
            uint32_t vdfPlaytime = 0, vdfPlaytime2wks = 0, vdfLastPlayed = 0;

            VdfUtil::ForEachFieldInSection(vdf, appPath, 6, [&](const VdfUtil::FieldInfo& fi) -> bool {
                if (fi.key == "Playtime")
                    try { vdfPlaytime = (uint32_t)std::stoul(std::string(fi.value)); } catch (...) {}
                else if (fi.key == "Playtime2wks")
                    try { vdfPlaytime2wks = (uint32_t)std::stoul(std::string(fi.value)); } catch (...) {}
                else if (fi.key == "LastPlayed")
                    try { vdfLastPlayed = (uint32_t)std::stoul(std::string(fi.value)); } catch (...) {}
                return true;
            });

            if (vdfPlaytime == 0) return true;

            auto cacheIt = g_cache.find(appId);
            if (cacheIt == g_cache.end()) {
                LoadAppStats(appId, g_cache[appId]);
                cacheIt = g_cache.find(appId);
            }
            AppStats& stats = cacheIt->second;

            if (stats.playtime.minutesForever >= vdfPlaytime) return true;

            // Local has more playtime -- update
            uint32_t delta = vdfPlaytime - stats.playtime.minutesForever;
            stats.playtime.minutesForever = vdfPlaytime;
            stats.playtime.minutesLastTwoWeeks = (std::max)(stats.playtime.minutesLastTwoWeeks, vdfPlaytime2wks);
            if (vdfLastPlayed > stats.playtime.lastPlayedTime)
                stats.playtime.lastPlayedTime = vdfLastPlayed;
#ifdef _WIN32
            stats.playtime.playtimeWindows += delta;
#elif defined(__APPLE__)
            stats.playtime.playtimeMac += delta;
#else
            stats.playtime.playtimeLinux += delta;
#endif
            SaveAppStats(appId, stats);
            reconciled++;
            LOG("[Stats] Reconciled app %u from localconfig: %u -> %u min (+%u)",
                appId, vdfPlaytime - delta, vdfPlaytime, delta);
            return true;
        });
    }
    if (reconciled > 0) {
        LOG("[Stats] Reconciled %d apps from localconfig.vdf", reconciled);
    }
}

void Init(const std::string& storageRoot, const std::string& steamPath) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_storageRoot = storageRoot + "/stats";
    g_steamPath = steamPath;
    fs::create_directories(g_storageRoot);
    fs::create_directories(g_storageRoot + "/schemas");

    ReconcileLocalConfig(storageRoot, steamPath);

    LOG("[Stats] Store initialized at %s", g_storageRoot.c_str());
}

// Import Steam's native UserGameStats + schema blobs for an app into `out`.
// Returns true if real stat data was imported. Used to seed our authoritative
// store on first access (so we can answer GetUserStats with real data).
// Caller must hold g_mutex (reads g_steamPath / g_accountIdProvider).
static bool ImportNativeStats(uint32_t appId, AppStats& out) {
    if (g_steamPath.empty() || !g_accountIdProvider) return false;
    uint32_t accountId = g_accountIdProvider();
    if (accountId == 0) return false;

    // Schema: appcache\stats\UserGameStatsSchema_<appId>.bin
    {
        std::string schemaPath = g_steamPath + "appcache\\stats\\UserGameStatsSchema_"
            + std::to_string(appId) + ".bin";
        std::ifstream sf(schemaPath, std::ios::binary);
        if (sf.good()) {
            out.schema.assign(std::istreambuf_iterator<char>(sf),
                              std::istreambuf_iterator<char>());
        }
    }

    // Stats: appcache\stats\UserGameStats_<accountId>_<appId>.bin
    std::string statsPath = g_steamPath + "appcache\\stats\\UserGameStats_"
        + std::to_string(accountId) + "_" + std::to_string(appId) + ".bin";
    std::ifstream f(statsPath, std::ios::binary);
    if (!f.good()) return false;
    std::vector<uint8_t> blob((std::istreambuf_iterator<char>(f)),
                              std::istreambuf_iterator<char>());
    f.close();
    if (blob.empty()) return false;

    size_t pos = 0, nodeCount = 0;
    std::vector<BkvNode> root;
    if (!BkvRead(blob.data(), blob.size(), pos, root, 0, nodeCount)) {
        LOG("[Stats] ImportNativeStats app=%u: BKV parse failed (%zu bytes)", appId, blob.size());
        return false;
    }

    const BkvNode* cache = BkvFind(root, "cache");
    if (!cache) return false;

    size_t importedStats = 0, importedAch = 0;
    for (const auto& stat : cache->children) {
        if (stat.type != BKV_SECTION) continue;       // skip crc / PendingChanges
        // Section name is the decimal stat id.
        uint32_t statId = 0;
        bool numeric = !stat.name.empty();
        for (char c : stat.name) { if (c < '0' || c > '9') { numeric = false; break; } }
        if (!numeric) continue;
        statId = (uint32_t)strtoul(stat.name.c_str(), nullptr, 10);

        const BkvNode* dataNode = BkvFind(stat.children, "data");
        if (!dataNode) continue;
        uint32_t value = BkvDataAsU32(*dataNode);

        out.stats.push_back(StatEntry{statId, value});
        ++importedStats;

        // Achievement unlock times -> AchievementBlock. The 'data' INT is the
        // unlocked-bit bitfield; AchievementTimes holds per-bit timestamps.
        const BkvNode* achTimes = BkvFind(stat.children, "AchievementTimes");
        if (achTimes) {
            AchievementBlock blk{};
            blk.statId = statId;
            blk.bits = value;
            for (const auto& bitNode : achTimes->children) {
                if (bitNode.type != BKV_INT) continue;
                uint32_t bit = (uint32_t)strtoul(bitNode.name.c_str(), nullptr, 10);
                if (bit < 32) blk.unlockTimes[bit] = bitNode.intVal;
            }
            out.achievements.push_back(blk);
            ++importedAch;
        }
    }

    LOG("[Stats] ImportNativeStats app=%u: imported %zu stat(s), %zu achievement block(s), schema=%zu bytes",
        appId, importedStats, importedAch, out.schema.size());
    return importedStats > 0;
}

// Parse the JSON document (stats/achievements/playtime) into `out`.
// Does NOT touch the separate on-disk schema blob.
static bool ParseAppStatsJson(const std::string& content, AppStats& out) {
    auto root = Json::Parse(content);
    if (root.isNull()) return false;

    out.crcStats = (uint32_t)root["crc_stats"].integer();
    out.stats.clear();
    out.achievements.clear();
    out.playtime = {};

    const auto& statsArr = root["stats"];
    if (statsArr.type == Json::Type::Array) {
        for (size_t i = 0; i < statsArr.size(); i++) {
            const auto& item = statsArr[i];
            StatEntry e;
            e.statId = (uint32_t)item["id"].integer();
            e.value = (uint32_t)item["value"].integer();
            out.stats.push_back(e);
        }
    }

    const auto& achArr = root["achievements"];
    if (achArr.type == Json::Type::Array) {
        for (size_t i = 0; i < achArr.size(); i++) {
            const auto& item = achArr[i];
            AchievementBlock blk = {};
            blk.statId = (uint32_t)item["stat_id"].integer();
            blk.bits = (uint32_t)item["bits"].integer();
            const auto& times = item["unlock_times"];
            if (times.type == Json::Type::Array) {
                for (size_t j = 0; j < times.size() && j < 32; j++) {
                    blk.unlockTimes[j] = (uint32_t)times[j].integer();
                }
            }
            out.achievements.push_back(blk);
        }
    }

    const auto& pt = root["playtime"];
    if (pt.type == Json::Type::Object) {
        out.playtime.minutesForever = (uint32_t)pt["minutes_forever"].integer();
        out.playtime.minutesLastTwoWeeks = (uint32_t)pt["minutes_2weeks"].integer();
        out.playtime.lastPlayedTime = (uint32_t)pt["last_played"].integer();
        out.playtime.playtimeWindows = (uint32_t)pt["windows"].integer();
        out.playtime.playtimeMac = (uint32_t)pt["mac"].integer();
        out.playtime.playtimeLinux = (uint32_t)pt["linux"].integer();
    }

    return true;
}

// Serialize the stats document (everything except the raw schema blob) to JSON.
static std::string BuildAppStatsJson(const AppStats& stats) {
    Json::Value root = Json::Object();
    root.objVal["crc_stats"] = Json::Number(stats.crcStats);

    Json::Value statsArr = Json::Array();
    for (auto& s : stats.stats) {
        Json::Value item = Json::Object();
        item.objVal["id"] = Json::Number(s.statId);
        item.objVal["value"] = Json::Number(s.value);
        statsArr.arrVal.push_back(std::move(item));
    }
    root.objVal["stats"] = std::move(statsArr);

    Json::Value achArr = Json::Array();
    for (auto& a : stats.achievements) {
        Json::Value item = Json::Object();
        item.objVal["stat_id"] = Json::Number(a.statId);
        item.objVal["bits"] = Json::Number(a.bits);
        Json::Value times = Json::Array();
        for (int i = 0; i < 32; i++) {
            times.arrVal.push_back(Json::Number(a.unlockTimes[i]));
        }
        item.objVal["unlock_times"] = std::move(times);
        achArr.arrVal.push_back(std::move(item));
    }
    root.objVal["achievements"] = std::move(achArr);

    Json::Value pt = Json::Object();
    pt.objVal["minutes_forever"] = Json::Number(stats.playtime.minutesForever);
    pt.objVal["minutes_2weeks"] = Json::Number(stats.playtime.minutesLastTwoWeeks);
    pt.objVal["last_played"] = Json::Number(stats.playtime.lastPlayedTime);
    pt.objVal["windows"] = Json::Number(stats.playtime.playtimeWindows);
    pt.objVal["mac"] = Json::Number(stats.playtime.playtimeMac);
    pt.objVal["linux"] = Json::Number(stats.playtime.playtimeLinux);
    root.objVal["playtime"] = std::move(pt);

    return Json::Stringify(root);
}

bool LoadAppStats(uint32_t appId, AppStats& out) {
    std::string path = StatsPath(appId);
    std::string content;

    std::ifstream f(path);
    if (f.good()) {
        content.assign((std::istreambuf_iterator<char>(f)),
                       std::istreambuf_iterator<char>());
        f.close();
    } else if (g_cloudPull && g_cloudPull(appId, content) && !content.empty()) {
        // No local copy; pull the cloud blob and materialize it locally so
        // subsequent reads hit disk and FlushAll round-trips correctly.
        std::ofstream wf(path, std::ios::trunc);
        wf << content;
        wf.close();
        LOG("[Stats] Pulled app %u stats from cloud (%zu bytes)", appId, content.size());
    }

    if (content.empty()) return false;
    if (!ParseAppStatsJson(content, out)) return false;

    // Load schema blob if exists (separate binary sidecar).
    std::string schemaPath = SchemaPath(appId);
    std::ifstream sf(schemaPath, std::ios::binary);
    if (sf.good()) {
        out.schema.assign(std::istreambuf_iterator<char>(sf),
                          std::istreambuf_iterator<char>());
    }
    return true;
}

void SaveAppStats(uint32_t appId, const AppStats& stats) {
    std::string path = StatsPath(appId);
    std::string json = BuildAppStatsJson(stats);

    std::ofstream f(path, std::ios::trunc);
    f << json;
    f.close();

    // Save schema blob separately if present
    if (!stats.schema.empty()) {
        std::string schemaPath = SchemaPath(appId);
        std::ofstream sf(schemaPath, std::ios::binary | std::ios::trunc);
        sf.write(reinterpret_cast<const char*>(stats.schema.data()), stats.schema.size());
    }

    // Cloud-back the stats document (fire-and-forget; provider queues upload).
    if (g_cloudPush) g_cloudPush(appId, json);
}

// Apps for which native import has been successfully attempted (imported real
// data OR confirmed Steam genuinely has none). Distinct from a cache entry,
// because reconcile/session-tracking can create an empty cache entry before any
// import runs -- we must still import on first stats access in that case.
static std::unordered_map<uint32_t, bool> g_importAttempted;

// Seed `stats` from Steam's native UserGameStats blob if we hold no stat data
// yet. Retries across calls while accountId is unavailable (returns 0); only
// marks "attempted" once we had a real accountId to look with. Caller holds mutex.
static void EnsureNativeImportLocked(uint32_t appId, AppStats& stats) {
    if (!stats.stats.empty()) return;                 // already have data
    if (g_importAttempted.count(appId)) return;        // already tried with a valid acct
    if (!g_accountIdProvider || g_accountIdProvider() == 0) {
        // accountId not ready yet (not logged in) -- don't mark attempted; retry later.
        return;
    }

    AppStats native;
    native.playtime = stats.playtime; // preserve any playtime already loaded
    bool imported = ImportNativeStats(appId, native);
    g_importAttempted[appId] = true; // accountId was valid; this is a definitive attempt
    if (imported) {
        stats.stats = std::move(native.stats);
        stats.achievements = std::move(native.achievements);
        if (!native.schema.empty()) stats.schema = std::move(native.schema);
        stats.crcStats = ComputeCrcLocked(stats);
        g_dirty[appId] = true;
        SaveAppStats(appId, stats);
    }
}

AppStats& GetOrCreate(uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_cache.find(appId);
    if (it != g_cache.end()) {
        // Cache hit, but a reconcile/session path may have created an empty
        // entry before import ran. Ensure native stats are imported on first
        // actual stats access (and on later retries once accountId is ready).
        EnsureNativeImportLocked(appId, it->second);
        return it->second;
    }

    AppStats& stats = g_cache[appId];
    bool loaded = LoadAppStats(appId, stats);
    if (!loaded) {
        stats.crcStats = 0;
        stats.playtime = {};
    }
    EnsureNativeImportLocked(appId, stats);
    return stats;
}

// Deterministic, order-independent CRC over stat values AND achievement
// unlock state. This is our opaque sync token (per IDA: the client just stores
// and echoes whatever crc we send; it never recomputes). It MUST be stable
// (same data -> same crc) and change when stats/achievements change.
static void AppendU32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(v & 0xFF);
    buf.push_back((v >> 8) & 0xFF);
    buf.push_back((v >> 16) & 0xFF);
    buf.push_back((v >> 24) & 0xFF);
}

uint32_t ComputeCrcLocked(const AppStats& stats) {
    // Sort stat ids so insertion order can't change the token.
    std::vector<const StatEntry*> sortedStats;
    sortedStats.reserve(stats.stats.size());
    for (auto& s : stats.stats) sortedStats.push_back(&s);
    std::sort(sortedStats.begin(), sortedStats.end(),
              [](const StatEntry* a, const StatEntry* b) { return a->statId < b->statId; });

    std::vector<uint8_t> buf;
    for (auto* s : sortedStats) {
        AppendU32(buf, s->statId);
        AppendU32(buf, s->value);
    }

    // Fold achievement unlock times (sorted by statId, then bit).
    std::vector<const AchievementBlock*> sortedAch;
    sortedAch.reserve(stats.achievements.size());
    for (auto& a : stats.achievements) sortedAch.push_back(&a);
    std::sort(sortedAch.begin(), sortedAch.end(),
              [](const AchievementBlock* a, const AchievementBlock* b) { return a->statId < b->statId; });
    for (auto* a : sortedAch) {
        AppendU32(buf, a->statId);
        AppendU32(buf, a->bits);
        for (int bit = 0; bit < 32; ++bit)
            if (a->unlockTimes[bit]) { AppendU32(buf, (uint32_t)bit); AppendU32(buf, a->unlockTimes[bit]); }
    }

    return buf.empty() ? 0 : Crc32(buf.data(), buf.size());
}

uint32_t ComputeCrc(uint32_t appId) {
    return ComputeCrcLocked(g_cache[appId]); // caller holds g_mutex
}

uint32_t SetStat(uint32_t appId, uint32_t statId, uint32_t value) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto& stats = g_cache[appId];

    bool found = false;
    for (auto& s : stats.stats) {
        if (s.statId == statId) {
            s.value = value;
            found = true;
            break;
        }
    }
    if (!found) {
        stats.stats.push_back({statId, value});
    }

    g_dirty[appId] = true;
    stats.crcStats = ComputeCrc(appId);
    return stats.crcStats;
}

uint32_t SetStats(uint32_t appId, const std::vector<StatEntry>& entries) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto& stats = g_cache[appId];

    for (auto& e : entries) {
        bool found = false;
        for (auto& s : stats.stats) {
            if (s.statId == e.statId) {
                s.value = e.value;
                found = true;
                break;
            }
        }
        if (!found) {
            stats.stats.push_back(e);
        }
    }

    g_dirty[appId] = true;
    stats.crcStats = ComputeCrc(appId);
    return stats.crcStats;
}

uint32_t SetAchievement(uint32_t appId, uint32_t statId, uint32_t bit, uint32_t unlockTime) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto& stats = g_cache[appId];

    AchievementBlock* blk = nullptr;
    for (auto& a : stats.achievements) {
        if (a.statId == statId) { blk = &a; break; }
    }
    if (!blk) {
        stats.achievements.push_back({});
        blk = &stats.achievements.back();
        blk->statId = statId;
        blk->bits = 0;
        memset(blk->unlockTimes, 0, sizeof(blk->unlockTimes));
    }

    if (bit < 32) {
        blk->bits |= (1u << bit);
        blk->unlockTimes[bit] = unlockTime;
    }

    g_dirty[appId] = true;
    stats.crcStats = ComputeCrc(appId);
    return stats.crcStats;
}

void SetSchema(uint32_t appId, const uint8_t* data, size_t len) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto& stats = g_cache[appId];
    stats.schema.assign(data, data + len);
    g_dirty[appId] = true;
}

const std::vector<uint8_t>& GetSchema(uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_cache[appId].schema;
}

void StartSession(uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_activeSessions[appId] = NowUnix();
    auto& stats = g_cache[appId];
    if (stats.playtime.lastPlayedTime == 0) {
        LoadAppStats(appId, stats);
    }
    stats.playtime.lastPlayedTime = NowUnix();
    g_dirty[appId] = true;
    LOG("[Stats] Session started for app %u", appId);
}

void EndSession(uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_activeSessions.find(appId);
    if (it == g_activeSessions.end()) return;

    uint32_t now = NowUnix();
    uint32_t elapsed = (now > it->second) ? (now - it->second) : 0;
    uint32_t minutes = elapsed / 60;
    g_activeSessions.erase(it);

    auto& stats = g_cache[appId];
    stats.playtime.minutesForever += minutes;
    stats.playtime.minutesLastTwoWeeks += minutes;
    stats.playtime.lastPlayedTime = now;

#ifdef _WIN32
    stats.playtime.playtimeWindows += minutes;
#elif defined(__APPLE__)
    stats.playtime.playtimeMac += minutes;
#else
    stats.playtime.playtimeLinux += minutes;
#endif

    g_dirty[appId] = true;
    SaveAppStats(appId, stats);
    g_dirty[appId] = false;
    LOG("[Stats] Session ended for app %u: +%u min (total %u)",
        appId, minutes, stats.playtime.minutesForever);
}

PlaytimeData GetPlaytime(uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto& stats = g_cache[appId];
    PlaytimeData pt = stats.playtime;

    auto it = g_activeSessions.find(appId);
    if (it != g_activeSessions.end()) {
        uint32_t now = NowUnix();
        uint32_t elapsed = (now > it->second) ? (now - it->second) : 0;
        uint32_t minutes = elapsed / 60;
        pt.minutesForever += minutes;
        pt.minutesLastTwoWeeks += minutes;
    }
    return pt;
}

std::vector<uint32_t> GetTrackedApps() {
    std::lock_guard<std::mutex> lock(g_mutex);
    std::vector<uint32_t> out;
    out.reserve(g_cache.size());
    for (const auto& [appId, stats] : g_cache) {
        if (stats.playtime.minutesForever > 0 || stats.playtime.lastPlayedTime > 0)
            out.push_back(appId);
    }
    return out;
}

void FlushAll() {
    std::lock_guard<std::mutex> lock(g_mutex);
    for (auto& [appId, dirty] : g_dirty) {
        if (dirty) {
            auto it = g_cache.find(appId);
            if (it != g_cache.end()) {
                SaveAppStats(appId, it->second);
                LOG("[Stats] Flushed app %u to disk", appId);
            }
            dirty = false;
        }
    }
}

} // namespace StatsStore
