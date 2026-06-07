#include "rpc_handlers.h"
#include "metadata_sync.h"
#include "autocloud_scan.h"
#include "autocloud_util.h"
#include "batch_tracker.h"
#include "cloud_intercept.h"
#include "local_storage.h"
#include "manifest_store.h"
#include "http_server.h"
#include "http_util.h"
#include "cloud_staging.h"
#include "app_state.h"
#include "cloud_storage.h"
#include "pending_ops_journal.h"
#include "file_util.h"
#include "remotecache_repair.h"
#include "steam_kv_injector.h"
#include "vdf.h"
#include "log.h"
#include "json.h"
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <sstream>
#include <thread>

#ifndef _WIN32
#include <unistd.h>
extern "C" void CR_SetCrashContext(const char* hook, const char* method, uint32_t appId);
#endif

namespace CloudIntercept {

// per-app upload batch tracking -- state lives in batch_tracker.cpp

static std::mutex g_conflictMutex;
static std::unordered_set<uint32_t> g_conflictKeepLocal;

void RecordConflictResolution(uint32_t appId, bool choseLocal) {
    std::lock_guard<std::mutex> lock(g_conflictMutex);
    if (choseLocal) {
        g_conflictKeepLocal.insert(appId);
        LOG("[NS] ConflictResolution app=%u: user chose keep-local, will skip pre-restore", appId);
    } else {
        g_conflictKeepLocal.erase(appId);
        LOG("[NS] ConflictResolution app=%u: user chose keep-cloud", appId);
    }
}

bool ConsumeConflictLocalChoice(uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_conflictMutex);
    return g_conflictKeepLocal.erase(appId) > 0;
}

// Per-(account,app) cleanNames with confirmed-present remotecache.vdf rows.
// Pre-seeded persiststate=0 closes Steam's reconcile false-delete window.
static std::mutex g_remotecacheRepairMutex;
static std::unordered_map<uint64_t, std::unordered_set<std::string>> g_remotecachePlantedRows;

// Per-(account,app) IO lock for read+atomic-write of remotecache.vdf.
static std::mutex g_remotecacheRepairIoMapMutex;
static std::unordered_map<uint64_t, std::shared_ptr<std::mutex>> g_remotecacheRepairIoMutexes;

static std::shared_ptr<std::mutex> AcquireRemotecacheRepairIoMutex(uint64_t appKey) {
    std::lock_guard<std::mutex> lock(g_remotecacheRepairIoMapMutex);
    auto& slot = g_remotecacheRepairIoMutexes[appKey];
    if (!slot) slot = std::make_shared<std::mutex>();
    return slot;
}

static bool RequireAccountId(const char* op, uint32_t appId, uint32_t& accountId) {
    // Brief wait for the network thread to publish g_steamId.
    constexpr uint64_t timeoutMs = 200;
    constexpr uint32_t sleepMs = 5;

#ifdef _WIN32
    ULONGLONG deadline = GetTickCount64() + timeoutMs;
    do {
        accountId = GetAccountId();
        if (accountId != 0) return true;
        Sleep(sleepMs);
    } while (GetTickCount64() < deadline);
#else
    auto start = std::chrono::steady_clock::now();
    do {
        accountId = GetAccountId();
        if (accountId != 0) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
    } while (std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count() < (int64_t)timeoutMs);
#endif

    LOG("[NS] %s app=%u no Steam account ID after %llums -- returning safe no-op",
        op, appId, timeoutMs);
    return false;
}

// Clamp file sizes at the wire boundary; Steam's protobuf fields are uint32.
static uint32_t ClampFileSizeToUint32(uint64_t rawSize, const char* fieldName,
                                      uint32_t appId, const std::string& filename) {
    if (rawSize > 0xFFFFFFFFull) {
        LOG("[Wire] %s for app %u file '%s' is %llu bytes; "
            "Steam's protobuf field is uint32, clamping to UINT32_MAX",
            fieldName, appId, filename.c_str(), (unsigned long long)rawSize);
        return 0xFFFFFFFFu;
    }
    return static_cast<uint32_t>(rawSize);
}


static void SetRpcCrashContext(const char* phase, const char* method, uint32_t appId) {
#ifndef _WIN32
    CR_SetCrashContext(phase, method, appId);
#else
    (void)phase;
    (void)method;
    (void)appId;
#endif
}

// Shutdown

void ShutdownRpcHandlers() {
}

// Per-app root tokens (e.g., "%GameInstall%") seen on uploads.
static std::unordered_map<uint64_t, std::unordered_set<std::string>> g_appRootTokens;
static std::mutex g_rootTokenMutex;

// Per-app file -> root-token. Each file is emitted only under its upload token.
static std::unordered_map<uint64_t, std::unordered_map<std::string, std::string>> g_fileTokens;
static std::mutex g_fileTokensMutex;

// Apps with batch-dirty file tokens; persisted once at HandleCompleteBatch.
static std::unordered_set<uint64_t> g_fileTokensDirtyApps;
static std::mutex g_fileTokensDirtyMutex;

// Per-batch AutoCloud canonical root map: resolve once, reuse across begin/commit.
static std::unordered_map<uint64_t, std::unordered_map<std::string, std::string>> g_batchCanonicalTokens;
static std::mutex g_batchCanonicalTokensMutex;

// Serializes token load-merge-save cycles.
static std::mutex g_tokenCaptureMutex;

static bool EnsureVdfSectionPath(std::string& vdfContent,
                                  const char* const* sections,
                                  size_t sectionCount);

// Cloud sync-icon state (last_sync_state) was never wired up: SetCloudSyncState
// had no callers, so the icon writer is dead code. Retained as a no-op to keep
// the OnUnload contract; the value is only read by Steam's UI, never by us.
void FlushPendingSyncStates() {}

// Namespace apps may lack PICS data (ufs.quota/maxnumfiles default to 0,
// causing over-quota eviction). Inject cached PICS values or fallback.
static constexpr uint64_t kFallbackQuotaBytes = 1073741824ULL; // 1 GB
static constexpr uint32_t kFallbackMaxFiles   = 10000;

// Steam's AC-exit disk walk (CAutoCloudManager::YldOnAppExit) counts each save
// file once PER RULE that matches it -- the native per-rule dedup is dead on the
// exit path (sub_1384D1DA0 @ 0x1384d221a is seeded with a null "previous path"
// there; it is only live on the staging/save path). So when two effective-platform
// rules resolve to the SAME physical directory (via rootoverrides), every file
// there is counted twice against maxnumfiles, tripping a false "over quota" that
// deletes all cloud files (e.g. app 1583520: 5 files x 2 colliding rules = 10 >
// maxnumfiles=5 -> wipe).
//
// We size the live maxnumfiles to the EXACT instance count that exit walk charges,
// computed by GetFileList (ScanResult.countedInstances) which mirrors the native
// per-rule, pre-dedup counting -- including siblings and macOS/Linux-only rules
// that simply never resolve to a dir on this OS and therefore add nothing. Unlike
// the old fileCount*ruleCount estimate this is collision-aware: an app whose rules
// resolve to DISTINCT paths reports maxCollisionFactor==1 and we no-op (no wipe
// risk), and an app with partial collisions (e.g. 3 rules, 2 colliding) gets x2,
// not x3.
//
// All inputs are immutable on-disk facts we never write, so the target can NEVER
// compound across sessions (a prior fix scaled the LIVE maxnumfiles and read its
// own previous bump back, running away 5->31->109->343). We only RAISE the budget.
static void EnsureQuotaSurvivesRuleMultiplier(uint32_t accountId, uint32_t appId,
                                              uint64_t liveQuota,
                                              uint32_t liveFiles) {
    std::string steamPath = CloudIntercept::GetSteamPath();
    if (steamPath.empty()) return;

    AutoCloudScan::ScanResult scan;
    try {
        scan = AutoCloudScan::GetFileList(steamPath, accountId, appId);
    } catch (...) { return; }

    // A collision factor <= 1 means every effective rule resolves to a distinct
    // physical directory: the native exit walk counts each file once, exactly like
    // the dedup-protected staging path, so there is no over-quota risk to cover.
    if (scan.maxCollisionFactor <= 1) return;
    if (scan.countedInstances == 0) return;

    // neededFiles = exact instances the exit walk charges + derived slack. The
    // slack replaces the old magic +16: YldOnAppExit's loop also consumes
    // (1 + siblingCount) budget per colliding rule pass, captured per worst dir as
    // collisionSiblingHeadroom. Keep a small floor so we are never under by 1-2.
    size_t derivedHeadroom = scan.collisionSiblingHeadroom;
    if (derivedHeadroom < scan.maxCollisionFactor) derivedHeadroom = scan.maxCollisionFactor;
    uint64_t needed = static_cast<uint64_t>(scan.countedInstances) + derivedHeadroom;

    // Guard against pathological inputs producing an implausible budget.
    constexpr uint64_t kMaxPlausibleFiles = 1000000ULL;
    if (needed > kMaxPlausibleFiles) needed = kMaxPlausibleFiles;
    uint32_t neededFiles = static_cast<uint32_t>(needed);
    if (neededFiles <= liveFiles) return; // current budget already covers it

    // Raise quota bytes proportionally too (rough: keep per-file budget constant).
    uint64_t neededQuota = liveFiles > 0
        ? liveQuota * neededFiles / liveFiles
        : liveQuota;
    if (neededQuota < liveQuota) neededQuota = liveQuota;

    SteamKvInjector::SetAppQuota(appId, neededQuota, neededFiles);
    LOG("[NS] EnsureQuotaSurvivesRuleMultiplier app=%u: %zu unique files, "
        "%zu counted instances x%zu collision (+%zu headroom) -> "
        "raise maxnumfiles %u->%u quota %llu->%llu",
        appId, scan.files.size(), scan.countedInstances, scan.maxCollisionFactor,
        derivedHeadroom, liveFiles, neededFiles,
        (unsigned long long)liveQuota, (unsigned long long)neededQuota);
}

static bool EnsureAppQuotaInjected(uint32_t accountId, uint32_t appId,
                                   CloudStorage::CloudAppState* cloudState) {
    if (!SteamKvInjector::IsReady()) {
        LOG("[NS] EnsureAppQuotaInjected app=%u: KV injector not ready", appId);
        return false;
    }

    uint64_t existingQuota = 0;
    uint32_t existingFiles = 0;
    bool readOk = SteamKvInjector::ReadAppQuota(appId, existingQuota, existingFiles);

        if (readOk && existingQuota > 0 && existingFiles > 0) {
        if (cloudState &&
            (cloudState->quota.quotaBytes != existingQuota ||
             cloudState->quota.maxNumFiles != existingFiles)) {
            cloudState->quota.quotaBytes = existingQuota;
            cloudState->quota.maxNumFiles = existingFiles;
            cloudState->quota.fetchedAtUnix = static_cast<uint64_t>(time(nullptr));
            cloudState->quota.lastSeenBuildId = cloudState->appBuildId;
            LOG("[NS] EnsureAppQuotaInjected app=%u: caching PICS quota=%llu files=%u (publish deferred to next batch)",
                appId, (unsigned long long)existingQuota, existingFiles);
            // Quota persisted on next CompleteBatch; async publish risks overwriting newer state.
        }
        LOG("[NS] EnsureAppQuotaInjected app=%u: Steam has quota=%llu files=%u",
            appId, (unsigned long long)existingQuota, existingFiles);
        EnsureQuotaSurvivesRuleMultiplier(accountId, appId, existingQuota, existingFiles);
        return true;
    }

    uint64_t injectQuota = kFallbackQuotaBytes;
    uint32_t injectFiles = kFallbackMaxFiles;
    const char* source = "fallback";

    if (cloudState && CloudStorage::QuotaConfigIsUsable(cloudState->quota)) {
        injectQuota = cloudState->quota.quotaBytes;
        injectFiles = cloudState->quota.maxNumFiles;
        source = "cached";
    }

    LOG("[NS] EnsureAppQuotaInjected app=%u: no PICS quota (readOk=%d existing=%llu/%u) "
        "-- injecting %s %lluB / %u files",
        appId, readOk ? 1 : 0,
        (unsigned long long)existingQuota, existingFiles,
        source, (unsigned long long)injectQuota, injectFiles);

    return SteamKvInjector::InjectAppQuota(appId, injectQuota, injectFiles);
}

// Inject savefiles rules so AC exit-sync builds a valid file-root tree.
// Without this, namespace apps get all files deleted ("no longer matches patterns").
static std::unordered_set<uint32_t> g_saveFilesInjected;
static std::mutex g_saveFilesInjectedMutex;

static void EnsureSaveFilesInjected(uint32_t appId) {
    {
        std::lock_guard<std::mutex> lock(g_saveFilesInjectedMutex);
        if (g_saveFilesInjected.count(appId)) return;
    }

    if (!SteamKvInjector::IsReady()) return;

    std::string steamPath = CloudIntercept::GetSteamPath();
    if (steamPath.empty()) return;

    auto rules = AutoCloudScan::GetRules(steamPath, appId);
    if (rules.empty()) return;

    std::vector<SteamKvInjector::SaveFileRule> kvRules;
    kvRules.reserve(rules.size());
    for (const auto& r : rules) {
        SteamKvInjector::SaveFileRule sr;
        sr.root = r.root;
        sr.path = r.path;
        sr.pattern = r.pattern;
        sr.recursive = r.recursive;
        sr.platforms = r.platforms;
        kvRules.push_back(std::move(sr));
    }

    if (SteamKvInjector::InjectSaveFiles(appId, kvRules)) {
        std::lock_guard<std::mutex> lock(g_saveFilesInjectedMutex);
        g_saveFilesInjected.insert(appId);
    }
}

// g_lastVerifiedCN removed -- session lock in unified state file prevents concurrent writes.

// Strip Steam root token prefix plus any \r\n between token and path.
// Sanitize control chars in RPC-sourced strings before logging to prevent log injection.
static std::string SanitizeForLog(const std::string& s) {
    std::string out = s;
    for (auto& c : out) { if (c == '\n' || c == '\r') c = '?'; }
    return out;
}

static std::string StripRootToken(const std::string& filename) {
    if (filename.size() >= 2 && filename[0] == '%') {
        size_t end = filename.find('%', 1);
        if (end != std::string::npos && end + 1 < filename.size()) {
            size_t start = end + 1;
            while (start < filename.size() && (filename[start] == '\r' || filename[start] == '\n'))
                ++start;
            return filename.substr(start);
        }
    }
    return filename;
}

// Extract the root token (e.g., "%GameInstall%") or empty string.
static std::string ExtractRootToken(const std::string& filename) {
    if (filename.size() >= 2 && filename[0] == '%') {
        size_t end = filename.find('%', 1);
        if (end != std::string::npos && end + 1 < filename.size()) {
            return filename.substr(0, end + 1);
        }
    }
    return "";
}

static void PrepareBatchCanonicalTokens(uint32_t accountId, uint32_t appId) {
    uint64_t key = MakeAppAccountKey(accountId, appId);
    {
        std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
        if (g_batchCanonicalTokens.find(key) != g_batchCanonicalTokens.end()) return;
    }

    // File->root-token mappings are persisted to disk by the upload path; load
    // them directly. (Previously also checked the AutoCloud bootstrap's in-memory
    // cache, but that component is gone -- disk is the single source of truth.)
    std::unordered_map<std::string, std::string> tokens =
        CloudStorage::LoadFileTokens(accountId, appId);
    if (tokens.empty()) return;

    std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
    g_batchCanonicalTokens.emplace(key, std::move(tokens));
}

static void ClearBatchCanonicalTokens(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
    g_batchCanonicalTokens.erase(MakeAppAccountKey(accountId, appId));
}

static std::string CanonicalizeUploadRootToken(uint32_t accountId, uint32_t appId,
                                               const std::string& cleanName,
                                               const std::string& fallbackToken) {
    if (cleanName.empty()) return fallbackToken;

    // Check batch cache first (populated by PrepareBatchCanonicalTokens)
    std::string canonical;
    bool foundCanonical = false;
    {
        std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
        auto appIt = g_batchCanonicalTokens.find(MakeAppAccountKey(accountId, appId));
        if (appIt != g_batchCanonicalTokens.end()) {
            auto tokenIt = appIt->second.find(cleanName);
            if (tokenIt != appIt->second.end()) {
                canonical = tokenIt->second;
                foundCanonical = true;
            }
        }
    }

    if (!foundCanonical) return fallbackToken;
    if (canonical != fallbackToken) {
        LOG("[NS-TOK] Canonicalized upload token for account %u app %u file %s: %s -> %s",
            accountId, appId, cleanName.c_str(), fallbackToken.c_str(), canonical.c_str());
    }
    return canonical;
}

// Capture a per-app root token seen on an upload. Serializes against
// concurrent persist operations; unions memory + disk before save.
static bool TryCaptureRootToken(uint32_t accountId, uint32_t appId, const std::string& token) {
    if (token.empty()) return false;

    uint64_t key = MakeAppAccountKey(accountId, appId);
    std::lock_guard<std::mutex> captureLock(g_tokenCaptureMutex);

    bool isNew = false;
    std::unordered_set<std::string> memorySnapshot;
    {
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        auto& tokenSet = g_appRootTokens[key];
        auto result = tokenSet.insert(token);
        isNew = result.second;
        if (isNew) {
            LOG("[NS-TOK] Captured root token for account %u app %u: %s (now %zu tokens)",
                accountId, appId, token.c_str(), tokenSet.size());
            memorySnapshot = tokenSet;
        }
    }
    if (!isNew) return false;

    auto diskTokens = LocalMetadataStore::LoadRootTokens(accountId, appId);
    size_t memoryOnlyCount = memorySnapshot.size();
    memorySnapshot.insert(diskTokens.begin(), diskTokens.end());
    if (memorySnapshot.size() > memoryOnlyCount) {
        LOG("[NS-TOK] Merged %zu extra root token(s) from disk for account %u app %u during capture",
            memorySnapshot.size() - memoryOnlyCount, accountId, appId);
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        auto& tokenSet = g_appRootTokens[key];
        tokenSet.insert(memorySnapshot.begin(), memorySnapshot.end());
        memorySnapshot = tokenSet;
    }
    if (!CloudStorage::SaveRootTokens(accountId, appId, memorySnapshot)) {
        LOG("[TryCaptureRootToken] root_token.dat local persist FAILED app %u -- in-memory set diverges from disk", appId);
    }
    return isNew;
}

// Record which root token a file was uploaded under.
static bool RecordFileToken(uint32_t accountId, uint32_t appId, const std::string& cleanName, const std::string& token) {
    if (cleanName.empty()) return false;
    if (IsReservedBlobFilename(cleanName)) return false;
    std::lock_guard<std::mutex> lock(g_fileTokensMutex);
    auto& fileTokens = g_fileTokens[MakeAppAccountKey(accountId, appId)];
    auto it = fileTokens.find(cleanName);
    if (it != fileTokens.end() && it->second == token) return false;
    fileTokens[cleanName] = token;
    LOG("[NS-FT] Recorded file token: account=%u app=%u file=%s token=%s",
        accountId, appId, cleanName.c_str(), token.c_str());
    return true;
}

// Remove a file's token mapping (called on delete).
static bool RemoveFileToken(uint32_t accountId, uint32_t appId, const std::string& cleanName) {
    std::lock_guard<std::mutex> lock(g_fileTokensMutex);
    auto appIt = g_fileTokens.find(MakeAppAccountKey(accountId, appId));
    if (appIt != g_fileTokens.end()) {
        if (appIt->second.erase(cleanName) > 0) {
            LOG("[NS-FT] Removed file token: account=%u app=%u file=%s", accountId, appId, cleanName.c_str());
            return true;
        }
    }
    return false;
}

// Persist file -> root-token map (memory wins on key conflicts).
static void PersistFileTokens(uint32_t accountId, uint32_t appId) {
    uint64_t key = MakeAppAccountKey(accountId, appId);
    std::lock_guard<std::mutex> captureLock(g_tokenCaptureMutex);

    std::unordered_map<std::string, std::string> snapshot;
    {
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        auto it = g_fileTokens.find(key);
        if (it != g_fileTokens.end()) snapshot = it->second;
    }

    auto diskTokens = CloudStorage::LoadFileTokens(accountId, appId);
    size_t mergedFromDisk = 0;
    for (auto& kv : diskTokens) {
        if (IsReservedBlobFilename(kv.first)) continue;
        if (snapshot.find(kv.first) == snapshot.end()) {
            snapshot.emplace(kv.first, kv.second);
            ++mergedFromDisk;
        }
    }
    if (mergedFromDisk > 0) {
        LOG("[NS-FT] PersistFileTokens account %u app %u: merged %zu extra entries from disk",
            accountId, appId, mergedFromDisk);
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        auto& mapRef = g_fileTokens[key];
        for (auto& kv : snapshot) {
            mapRef.emplace(kv.first, kv.second);
        }
        snapshot = mapRef;
    }
    if (!CloudStorage::SaveFileTokens(accountId, appId, snapshot)) {
        LOG("[RecordFileToken] file_tokens.dat local persist FAILED app %u -- in-memory mapping diverges from disk", appId);
    }
}

// Defer persistence to HandleCompleteBatch.
static void MarkFileTokensDirty(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_fileTokensDirtyMutex);
    g_fileTokensDirtyApps.insert(MakeAppAccountKey(accountId, appId));
}

static void ClearFileTokensDirty(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_fileTokensDirtyMutex);
    g_fileTokensDirtyApps.erase(MakeAppAccountKey(accountId, appId));
}

static std::string GetMachineName() {
#ifdef _WIN32
    char buf[MAX_COMPUTERNAME_LENGTH + 1];
    DWORD len = sizeof(buf);
    if (GetComputerNameA(buf, &len))
        return std::string(buf, len);
    return "UNKNOWN";
#else
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0)
        return std::string(buf);
    return "UNKNOWN";
#endif
}


// Returns the blob-store file list; Steam compares vs. remotecache.vdf.
RpcResult HandleGetChangelist(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    SetRpcCrashContext("GetChangelist:entry", "Cloud.GetAppFileChangelist#1", appId);
    auto* cnField = PB::FindField(reqBody, 2);
    uint64_t clientChangeNumber = cnField ? cnField->varintVal : 0;

    uint32_t accountId = 0;
    SetRpcCrashContext("GetChangelist:account", "Cloud.GetAppFileChangelist#1", appId);
    if (!RequireAccountId("GetAppFileChangelist", appId, accountId)) {
        // is_only_delta=1 prevents Steam queuing ClientDeleteFile.
        PB::Writer body;
        body.WriteVarint(1, 0);                    // current_change_number
        body.WriteVarint(3, 1);                    // is_only_delta = 1
        body.WriteString(5, GetMachineName());     // machine_names
        body.WriteVarint(6, 0);                    // app_buildid_hwm
        return body;
    }
    uint64_t appKey = MakeAppAccountKey(accountId, appId);

    // Inject quota before ComputeLastKnownSyncState can run.
    EnsureAppQuotaInjected(accountId, appId, nullptr);
    EnsureSaveFilesInjected(appId);

    // No client-side changelist fast-path. Real Steam (sub_13852FDC0 in
    // steamclient64) ALWAYS calls Cloud.GetAppFileChangelist and decides "already
    // synced" purely from the response's current_change_number vs its own CN. A
    // CloudRedirect-side "repeat call, empty delta" short-circuit has no Steam
    // equivalent and is unsafe: it asserts the client is synced without verifying
    // the client still holds the files on disk. If the client's disk diverged
    // (file deleted/missing), the empty delta makes Steam treat the missing files
    // as locally deleted and wipe the cloud copy. Always serve authoritative state
    // and let Steam's own CN comparison + reconciliation run, exactly like a real
    // cloud server.

    // Track whether we fetched fresh manifest from cloud this call
    CloudStorage::Manifest cloudManifest;
    std::unordered_map<std::string, CloudStorage::FileEntry> cloudFileEntries; // full per-file state from cloud
    bool haveCloudManifest = false;
    uint64_t cloudCN = 0;
    uint64_t appBuildIdHwm = 0;
    CloudStorage::CloudAppState fetchedState; // retained for quota caching
    bool haveFetchedState = false;
    bool cloudStateNotFound = false; // true ONLY on genuine NotFound (not fetch error)

    if (CloudStorage::IsCloudActive()) {
        SetRpcCrashContext("GetChangelist:fetch-cloud", "Cloud.GetAppFileChangelist#1", appId);
        auto stateResult = CloudStorage::FetchCloudState(accountId, appId);
        if (stateResult.status == CloudStorage::StateFetchStatus::Ok) {
            auto& state = stateResult.state;
            cloudCN = state.cn;
            appBuildIdHwm = state.appBuildId;
            for (const auto& [name, fe] : state.files) {
                CloudStorage::ManifestEntry me;
                me.sha = fe.sha;
                me.timestamp = fe.timestamp;
                me.size = fe.size;
                cloudManifest[name] = std::move(me);
                cloudFileEntries[name] = fe;
            }
            haveCloudManifest = true;
            fetchedState = state;
            haveFetchedState = true;

            uint64_t localCN = LocalStorage::GetChangeNumber(accountId, appId);
            if (state.cn > localCN) {
                LOG("[NS-CL] GetAppFileChangelist app=%u: cloud CN=%llu > local CN=%llu, syncing local",
                    appId, state.cn, localCN);
                CloudStorage::SaveManifestLocal(accountId, appId, cloudManifest);
                LocalStorage::SetChangeNumber(accountId, appId, state.cn);
            }

            LOG("[NS-CL] GetAppFileChangelist app=%u: cloud state CN=%llu (%zu files)",
                appId, cloudCN, cloudManifest.size());
        } else if (stateResult.status == CloudStorage::StateFetchStatus::NotFound) {
            cloudStateNotFound = true;
            LOG("[NS-CL] GetAppFileChangelist app=%u: no cloud state (new app), using local",
                appId);
        } else {
            LOG("[NS-CL] GetAppFileChangelist app=%u: cloud state fetch failed (status=%d), using local",
                appId, static_cast<int>(stateResult.status));
        }
    }

    // Inject quota (cached PICS values or fallback).
    EnsureAppQuotaInjected(accountId, appId,
                           haveFetchedState ? &fetchedState : nullptr);

    if (!haveCloudManifest) {
        SetRpcCrashContext("GetChangelist:local-fallback", "Cloud.GetAppFileChangelist#1", appId);
        uint64_t localCN = LocalStorage::GetChangeNumber(accountId, appId);

        auto localManifest = CloudStorage::LoadLocalManifest(accountId, appId);
        if (!localManifest.empty()) {
            cloudCN = localCN;
            for (const auto& [name, me] : localManifest) {
                cloudManifest[name] = me;
            }
            haveCloudManifest = true;
        } else {
            cloudCN = 0;
        }

        LOG("[NS-CL] GetAppFileChangelist app=%u: local fallback CN=%llu (%zu files)",
            appId, cloudCN, cloudManifest.size());
    }

    // Reconciliation safety net: if our local blob store contains non-reserved
    // files the REAL cloud manifest is missing, publish a MERGED manifest at a
    // non-rewinding CN. Native upload (CompleteBatch) is the primary publisher;
    // this catches the case where blobs exist locally but cloud state was lost
    // and no new upload has happened yet. Publishes at strictly-above-cloud CN so
    // it can never rewind (issue #53 was caused by the old AutoCloud bootstrap
    // publishing a stale CN; that component has been removed entirely).
    // Only reconcile when we KNOW the real cloud contents: either a successful
    // fetch (haveFetchedState) or a definitive NotFound (empty cloud). NEVER on a
    // transient fetch failure -- treating that as "empty" could republish over
    // good cloud state during a network blip.
    if (CloudStorage::IsCloudActive() && (haveFetchedState || cloudStateNotFound)) {
        SetRpcCrashContext("GetChangelist:reconcile-local", "Cloud.GetAppFileChangelist#1", appId);
        CloudStorage::Manifest localBlobs =
            CloudStorage::BuildManifestFromLocalBlobs(accountId, appId);

        // Compare against the REAL cloud manifest only. cloudManifest/cloudCN may
        // have been pre-filled from the local-fallback path above (when no cloud
        // state exists), which would mask missing files. cloudFileEntries is
        // populated ONLY by an actual cloud fetch (haveFetchedState), so use it as
        // the authoritative "what's really in the cloud" set. NotFound -> empty.
        static const std::unordered_map<std::string, CloudStorage::FileEntry> kEmptyCloudFiles;
        const auto& realCloudFiles = haveFetchedState ? cloudFileEntries : kEmptyCloudFiles;

        size_t missingFromCloud = 0;
        for (const auto& [name, entry] : localBlobs) {
            if (IsReservedBlobFilename(name)) continue;
            if (realCloudFiles.find(name) == realCloudFiles.end()) ++missingFromCloud;
        }

        if (missingFromCloud > 0) {
            // Merge: start from real cloud files, add/refresh from local blobs.
            CloudStorage::CloudAppState mergedState;
            for (const auto& [name, fe] : realCloudFiles) {
                if (IsReservedBlobFilename(name)) continue;
                mergedState.files[name] = fe;
            }
            for (const auto& [name, me] : localBlobs) {
                if (IsReservedBlobFilename(name)) continue;
                CloudStorage::FileEntry fe;
                fe.sha = me.sha;
                fe.timestamp = me.timestamp;
                fe.size = me.size;
                mergedState.files[name] = std::move(fe);
            }
            uint64_t baseCN = cloudCN > LocalStorage::GetChangeNumber(accountId, appId)
                                  ? cloudCN
                                  : LocalStorage::GetChangeNumber(accountId, appId);
            mergedState.cn = baseCN + 1; // strictly above any seen CN -> never rewinds
            mergedState.appBuildId = appBuildIdHwm;

            LOG("[NS-CL] Reconcile app %u: %zu local file(s) missing from cloud; "
                "publishing merged manifest (%zu files) at CN=%llu",
                appId, missingFromCloud, mergedState.files.size(),
                (unsigned long long)mergedState.cn);

            auto statePtr = std::make_shared<CloudStorage::CloudAppState>(std::move(mergedState));
            uint32_t asyncAcct = accountId;
            uint32_t asyncApp = appId;
            std::thread([statePtr, asyncAcct, asyncApp] {
                CloudStorage::InflightSyncScope guard;
                if (!guard.entered) return;
                auto syncMtx = CloudStorage::AcquireAppSyncMutex(asyncAcct, asyncApp);
                std::lock_guard<std::mutex> lock(*syncMtx);
                // Re-fetch under lock; recompute CN above the freshest cloud CN so
                // a concurrent native publish can't be rewound.
                auto existing = CloudStorage::FetchCloudState(asyncAcct, asyncApp);
                if (existing.status == CloudStorage::StateFetchStatus::Ok) {
                    if (existing.state.cn >= statePtr->cn) {
                        statePtr->cn = existing.state.cn + 1;
                    }
                    // Don't clobber an active session lock.
                    statePtr->session = existing.state.session;
                }
                CloudStorage::PublishCloudState(asyncAcct, asyncApp, *statePtr);
                LocalStorage::SetChangeNumber(asyncAcct, asyncApp, statePtr->cn);
            }).detach();
        }
    }

    // Build file list - either from cloud manifest (fast path) or local blobs
    std::vector<LocalStorage::FileEntry> files;
    uint64_t serverChangeNumber = 0;  // Initialize to prevent UB in edge cases
    bool responseIsDelta = true;

    if (haveCloudManifest && cloudManifest.empty() && cloudCN == 0) {
        // New app at CN=0 -- return empty authoritative inventory
        serverChangeNumber = cloudCN;
        responseIsDelta = false;
        LOG("[NS-CL] GetAppFileChangelist app=%u: cloud manifest is empty at CN=%llu, returning empty authoritative inventory",
            appId, cloudCN);
    } else if (haveCloudManifest && !cloudManifest.empty()) {
        SetRpcCrashContext("GetChangelist:manifest-delta", "Cloud.GetAppFileChangelist#1", appId);
        // Steam-faithful delta: compute diff between clientCN snapshot and current manifest.
        // Steam's server returns only changed files -- not the full inventory.
        auto delta = CloudStorage::ComputeManifestDelta(accountId, appId,
                                                         clientChangeNumber, cloudCN,
                                                         cloudManifest);
        if (!delta.files.empty()) {
            serverChangeNumber = delta.serverCN;
            responseIsDelta = true;
            for (auto& fc : delta.files) {
                if (IsReservedBlobFilename(fc.filename)) continue;
                LocalStorage::FileEntry fe;
                fe.filename = std::move(fc.filename);
                fe.sha = std::move(fc.sha);
                fe.timestamp = fc.timestamp;
                fe.rawSize = fc.size;
                fe.deleted = fc.deleted;
                files.push_back(std::move(fe));
            }
            LOG("[NS-CL] GetAppFileChangelist app=%u delta clientCN=%llu serverCN=%llu (%zu changed)",
                appId, clientChangeNumber, cloudCN, files.size());
        } else {
            // No per-CN delta to compute. Serve the authoritative full manifest on
            // EVERY call -- like a real cloud server. (Previously this short-circuited
            // to an empty delta on repeat calls within a session, which lied that the
            // client was synced and let Steam wipe the cloud copy when local files had
            // diverged.) Use cloud timestamps so repeated compares see a stable
            // remotetime and don't manufacture false conflicts.
            serverChangeNumber = cloudCN;
            responseIsDelta = false;

            for (const auto& [filename, entry] : cloudManifest) {
                if (IsReservedBlobFilename(filename)) continue;
                LocalStorage::FileEntry fe;
                fe.filename = filename;
                fe.sha = entry.sha;
                fe.timestamp = entry.timestamp;
                fe.rawSize = entry.size;
                fe.deleted = false;
                files.push_back(std::move(fe));
            }

            LOG("[NS-CL] GetAppFileChangelist app=%u: returning full manifest (%zu files) at CN=%llu (clientCN=%llu)",
                appId, files.size(), serverChangeNumber, clientChangeNumber);
        }
    } else {
        // No cloud manifest -- serve local files as delta (don't trigger reconcile-deletes)
        SetRpcCrashContext("GetChangelist:local-files", "Cloud.GetAppFileChangelist#1", appId);
        
        files = LocalStorage::GetFileList(accountId, appId);
        serverChangeNumber = LocalStorage::GetChangeNumber(accountId, appId);
        // cloud active but fetch failed -> delta (don't delete unverified); cloud inactive -> authoritative
        responseIsDelta = CloudStorage::IsCloudActive();

        files.erase(std::remove_if(files.begin(), files.end(),
            [](const LocalStorage::FileEntry& fe) {
                return IsReservedBlobFilename(fe.filename);
            }), files.end());
    }

    LOG("[NS-CL] GetAppFileChangelist app=%u clientCN=%llu serverCN=%llu files=%zu",
        appId, clientChangeNumber, serverChangeNumber, files.size());

    // build path_prefix table and file entries
    std::unordered_map<std::string, uint32_t> prefixMap;
    std::vector<std::string> prefixList;
    std::string machineName = GetMachineName();

    std::unordered_set<std::string> rootTokens;
    bool appHasUfsRules = true; // assume yes until proven otherwise
    {
        SetRpcCrashContext("GetChangelist:root-token-cache", "Cloud.GetAppFileChangelist#1", appId);
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        auto it = g_appRootTokens.find(appKey);
        if (it != g_appRootTokens.end()) {
            rootTokens = it->second;
        }
    }
    // Disk-only fallback (no cloud download yet). Tokens are persisted by the
    // upload path; safe to cache directly (no async ingest worker to straddle).
    if (rootTokens.empty()) {
        SetRpcCrashContext("GetChangelist:root-token-disk", "Cloud.GetAppFileChangelist#1", appId);
        rootTokens = LocalMetadataStore::LoadRootTokens(accountId, appId);
        if (!rootTokens.empty()) {
            std::lock_guard<std::mutex> lock(g_rootTokenMutex);
            auto it = g_appRootTokens.find(appKey);
            if (it != g_appRootTokens.end()) {
                rootTokens = it->second;
            } else {
                g_appRootTokens[appKey] = rootTokens;
            }
        }
    }
    // No disk tokens: skip cloud download for apps with no UFS rules.
    if (rootTokens.empty()) {
        std::string steamPath = CloudIntercept::GetSteamPath();
        if (!steamPath.empty()) {
            auto scanResult = AutoCloudScan::GetFileList(steamPath, accountId, appId);
            appHasUfsRules = scanResult.hasRules;
            if (appHasUfsRules) {
                // Has rules but no tokens on disk -- fall back to full cloud load.
                rootTokens = CloudStorage::LoadRootTokens(accountId, appId);
            }
            for (const auto& fe : scanResult.files) {
                if (!fe.rootToken.empty()) rootTokens.insert(fe.rootToken);
            }
            if (rootTokens.empty()) {
                rootTokens = std::move(scanResult.ruleRootTokens);
            }
        }
        if (rootTokens.empty()) {
            rootTokens.insert("");
        }
    }

    for (auto& t : rootTokens) {
        LOG("[NS-CL] Root token for app %u: '%s'", appId, t.c_str());
    }

    // Snapshot file -> root-token map; same straddle gate as root tokens.
    // No UFS rules: skip cloud download.
    std::unordered_map<std::string, std::string> fileTokenSnapshot;
    bool needsDiskLoad = false;
    {
        SetRpcCrashContext("GetChangelist:file-token-cache", "Cloud.GetAppFileChangelist#1", appId);
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        auto it = g_fileTokens.find(appKey);
        if (it != g_fileTokens.end()) {
            fileTokenSnapshot = it->second;
        } else {
            needsDiskLoad = true;
        }
    }
    if (needsDiskLoad) {
        SetRpcCrashContext("GetChangelist:file-token-disk", "Cloud.GetAppFileChangelist#1", appId);
        auto loaded = appHasUfsRules
            ? CloudStorage::LoadFileTokens(accountId, appId)
            : LocalMetadataStore::LoadFileTokens(accountId, appId);
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        auto it = g_fileTokens.find(appKey);
        if (it != g_fileTokens.end()) {
            fileTokenSnapshot = it->second;
        } else if (!loaded.empty()) {
            g_fileTokens[appKey] = std::move(loaded);
            LOG("[NS-CL] Loaded %zu file-token mappings for account %u app %u",
                g_fileTokens[appKey].size(), accountId, appId);
            fileTokenSnapshot = g_fileTokens[appKey];
        }
    }

    // Default token: prefer %GameInstall%, else lexicographically smallest.
    std::string defaultToken;
    if (!rootTokens.empty()) {
        if (rootTokens.count("%GameInstall%"))
            defaultToken = "%GameInstall%";
        else {
            std::vector<std::string> sorted(rootTokens.begin(), rootTokens.end());
            std::sort(sorted.begin(), sorted.end());
            defaultToken = sorted.front();
        }
    }


    struct PreparedFile {
        std::string leaf;
        uint32_t prefixIdx;
        const LocalStorage::FileEntry* entry;
    };
    std::vector<PreparedFile> prepared;

    std::vector<RemotecacheCandidate> remotecacheCandidates;
    remotecacheCandidates.reserve(files.size());

    SetRpcCrashContext("GetChangelist:prepare-files", "Cloud.GetAppFileChangelist#1", appId);
    for (auto& fe : files) {
        // split filename into directory prefix + leaf
        size_t lastSlash = fe.filename.rfind('/');
        std::string dirPrefix, leaf;
        if (lastSlash != std::string::npos) {
            dirPrefix = fe.filename.substr(0, lastSlash + 1);
            leaf = fe.filename.substr(lastSlash + 1);
        } else {
            leaf = fe.filename;
        }

        std::string fileToken;
        auto ftIt = fileTokenSnapshot.find(fe.filename);
        bool hasRecordedFileToken = ftIt != fileTokenSnapshot.end();
        if (hasRecordedFileToken) fileToken = ftIt->second;
        if (!hasRecordedFileToken) {
            fileToken = defaultToken;
            LOG("[NS-CL]   file: %s has no recorded token, using default '%s'",
                fe.filename.c_str(), fileToken.c_str());
        }

        std::string fullPrefix = fileToken + dirPrefix;

        uint32_t prefixIdx;
        auto it = prefixMap.find(fullPrefix);
        if (it != prefixMap.end()) {
            prefixIdx = it->second;
        } else {
            prefixIdx = (uint32_t)prefixList.size();
            prefixMap[fullPrefix] = prefixIdx;
            prefixList.push_back(fullPrefix);
        }

        prepared.push_back({leaf, prefixIdx, &fe});
        remotecacheCandidates.push_back(
            { fe.filename, fileToken, fe.sha, fe.timestamp, fe.rawSize });
        LOG("[NS-CL]   file: %s (prefix[%u]=%s, size=%llu, ts=%llu)",
            fe.filename.c_str(), prefixIdx, fullPrefix.c_str(), fe.rawSize, fe.timestamp);
    }

    // Don't pre-seed remotecache.vdf; let Steam manage it via GetChangelist diffs.
    // Pre-seeding caused conflicts (Steam interpreted it as "local changed").

    SetRpcCrashContext("GetChangelist:write-response", "Cloud.GetAppFileChangelist#1", appId);
    PB::Writer body;
    body.WriteVarint(1, serverChangeNumber);                     // current_change_number
    body.WriteVarint(3, responseIsDelta ? 1u : 0u);              // is_only_delta

    LOG("[NS-CL-WIRE] app=%u response: current_change_number=%llu is_only_delta=%u nfiles=%zu",
        appId, (unsigned long long)serverChangeNumber, responseIsDelta ? 1u : 0u, prepared.size());

    // file entries (field 2, repeated)
    for (auto& pf : prepared) {
        PB::Writer fileSub;
        fileSub.WriteString(1, pf.leaf);                        // file_name (leaf only)
        if (!pf.entry->sha.empty())
            fileSub.WriteBytes(2, pf.entry->sha.data(), pf.entry->sha.size()); // sha_file
        fileSub.WriteVarint(3, pf.entry->timestamp);            // time_stamp
        fileSub.WriteVarint(4, ClampFileSizeToUint32(pf.entry->rawSize,
                                                     "AppFileInfo.raw_file_size",
                                                     appId, pf.entry->filename));
        // persist_state: 0=Persisted, 2=Deleted; platforms_to_sync: 0xFFFFFFFF=all
        uint32_t persistState = pf.entry->deleted ? 2u : pf.entry->persistState;
        uint32_t platforms = pf.entry->platformsToSync;
        auto cfeIt = cloudFileEntries.find(pf.entry->filename);
        if (cfeIt != cloudFileEntries.end()) {
            if (!pf.entry->deleted) persistState = cfeIt->second.persistState;
            platforms = cfeIt->second.platformsToSync;
        }
        fileSub.WriteVarint(5, persistState);                    // persist_state
        fileSub.WriteVarint(6, platforms);                       // platforms_to_sync
        fileSub.WriteVarint(7, pf.prefixIdx);                    // path_prefix_index
        fileSub.WriteVarint(8, 0);                              // machine_name_index
        body.WriteSubmessage(2, fileSub);

        LOG("[NS-CL-WIRE]   file=%s deleted=%d persist_state=%u platforms=0x%X sha_len=%zu ts=%llu size=%llu prefix=%u%s",
            pf.entry->filename.c_str(), pf.entry->deleted ? 1 : 0, persistState, platforms,
            pf.entry->sha.size(), (unsigned long long)pf.entry->timestamp,
            (unsigned long long)pf.entry->rawSize, pf.prefixIdx,
            (cfeIt != cloudFileEntries.end()) ? " [from-cloud]" : " [no-cloud-entry]");
    }

    // path_prefixes (field 4, repeated)
    for (auto& p : prefixList) {
        body.WriteString(4, p);
    }

    // machine_names (field 5, repeated)
    body.WriteString(5, machineName);

    // app_buildid_hwm (field 6) - not critical, set to 0
    body.WriteVarint(6, appBuildIdHwm);

    LOG("[NS-CL] Response: %zu files, %zu prefixes, CN=%llu",
        prepared.size(), prefixList.size(), serverChangeNumber);


#ifdef DEBUG_HEX_DUMP
    {
        auto& ourData = body.Data();
        LOG("[NS-CL-HEX] Our changelist response: %zu bytes", ourData.size());
        for (size_t off = 0; off < ourData.size(); off += 32) {
            char hexLine[200];
            int pos = 0;
            size_t end = (off + 32 < ourData.size()) ? off + 32 : ourData.size();
            for (size_t i = off; i < end; i++) {
                pos += snprintf(hexLine + pos, sizeof(hexLine) - pos, "%02X ", ourData[i]);
            }
            LOG("[NS-CL-HEX] offset=%04X: %s", (unsigned)off, hexLine);
        }
    }
#endif

    return body;
}

// SignalAppLaunchIntent: return pending_remote_operations.
// Also pre-restores cloud files to game folders so Steam's sync finds them on disk.
RpcResult HandleLaunchIntent(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    LOG("[NS] SignalAppLaunchIntent app=%u", appId);

    uint32_t accountId = 0;
    if (!RequireAccountId("SignalAppLaunchIntent", appId, accountId)) {
        PB::Writer body;
        return body;
    }
    EnsureAppQuotaInjected(accountId, appId, nullptr);
    EnsureSaveFilesInjected(appId);

    // Fetch cloud state (SHAs for cache-first restore + session management).
    CloudStorage::StateFetchResult stateResult;
    if (CloudStorage::IsCloudActive()) {
        stateResult = CloudStorage::FetchCloudState(accountId, appId);
    }

    ConsumeConflictLocalChoice(appId);

    PendingOpsJournal::Entry currentSession;
    currentSession.machineName = GetMachineName();
    currentSession.timeLastUpdated = static_cast<uint32_t>(time(nullptr));
    bool ignorePendingOperations = false;
    for (const auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::Varint) currentSession.clientId = f.varintVal;
        if (f.fieldNum == 3 && f.wireType == PB::LengthDelimited) {
            currentSession.machineName.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
        }
        if (f.fieldNum == 4 && f.wireType == PB::Varint) ignorePendingOperations = f.varintVal != 0;
        if (f.fieldNum == 5 && f.wireType == PB::Varint) currentSession.osType = static_cast<uint32_t>(f.varintVal);
        if (f.fieldNum == 6 && f.wireType == PB::Varint) currentSession.deviceType = static_cast<uint32_t>(f.varintVal);
    }

    // Cloud session management -- reuse stateResult from above (already fetched).
    PB::Writer body;
    bool sessionConflict = false;
    if (stateResult.status == CloudStorage::StateFetchStatus::Ok) {
        // Sync mutex: serialize state RMW to prevent interleaved publishes.
        auto syncMtx = CloudStorage::AcquireAppSyncMutex(accountId, appId);
        std::lock_guard<std::mutex> syncLock(*syncMtx);

        auto& state = stateResult.state;
        uint64_t now = static_cast<uint64_t>(time(nullptr));

        if (state.hasActiveSession() &&
            state.session.clientId != currentSession.clientId) {
            // Another machine holds session lock; EResult=108 triggers conflict UI.
            LOG("[NS] LaunchIntent app=%u: another session active (machine=%s, client=%llu, age=%llus)",
                appId, state.session.machineName.c_str(),
                state.session.clientId,
                now - state.session.timeLastUpdated);
            PB::Writer op;
            op.WriteVarint(1, 1); // operation = AppSessionActive
            op.WriteString(2, state.session.machineName);
            op.WriteVarint(3, state.session.clientId);
            op.WriteVarint(4, static_cast<uint32_t>(state.session.timeLastUpdated));
            body.WriteSubmessage(1, op);

            if (!ignorePendingOperations) {
                // Steam expects EResult=108 to trigger the conflict UI.
                sessionConflict = true;
            } else {
                // "Play anyway" -- override stale session.
                state.session.clientId = currentSession.clientId;
                state.session.machineName = currentSession.machineName;
                state.session.timeLastUpdated = now;
                state.session.operation = "active";
                if (!CloudStorage::PublishCloudState(accountId, appId, state, stateResult.etag)) {
                    // Retry once without etag.
                    if (!CloudStorage::PublishCloudState(accountId, appId, state)) {
                        LOG("[NS] LaunchIntent app=%u: session override publish failed after retry", appId);
                    }
                }
                LOG("[NS] LaunchIntent app=%u: forced session override (machine=%s, client=%llu)",
                    appId, currentSession.machineName.c_str(), currentSession.clientId);
            }
        } else {
            state.session.clientId = currentSession.clientId;
            state.session.machineName = currentSession.machineName;
            state.session.timeLastUpdated = now;
            state.session.operation = "active";
            if (!CloudStorage::PublishCloudState(accountId, appId, state, stateResult.etag)) {
                // Retry without etag (stale from racing ReleaseCloudSession).
                LOG("[NS] LaunchIntent app=%u: session acquire publish failed, retrying without etag", appId);
                auto freshResult = CloudStorage::FetchCloudState(accountId, appId);
                if (freshResult.status == CloudStorage::StateFetchStatus::Ok) {
                    auto& freshState = freshResult.state;
                    if (freshState.hasActiveSession() &&
                        freshState.session.clientId != currentSession.clientId) {
                        // Another machine acquired the session in the window.
                        LOG("[NS] LaunchIntent app=%u: another machine acquired session during retry", appId);
                        sessionConflict = !ignorePendingOperations;
                    } else {
                        freshState.session = state.session;
                        if (!CloudStorage::PublishCloudState(accountId, appId, freshState)) {
                            LOG("[NS] LaunchIntent app=%u: session acquire retry also failed", appId);
                        }
                    }
                }
            }
            LOG("[NS] LaunchIntent app=%u: acquired session (machine=%s, client=%llu)",
                appId, currentSession.machineName.c_str(), currentSession.clientId);
        }
    }

    auto pending = PendingOpsJournal::RecordLaunchIntent(
        accountId, appId, currentSession, ignorePendingOperations);

    // Launch intent clears pending-upload state.
    PendingOpsJournal::ClearUploadPending(accountId, appId);

    for (const auto& entry : pending) {
        PB::Writer op;
        op.WriteVarint(1, static_cast<uint32_t>(entry.operation));
        if (!entry.machineName.empty()) op.WriteString(2, entry.machineName);
        if (entry.clientId != 0) op.WriteVarint(3, entry.clientId);
        if (entry.timeLastUpdated != 0) op.WriteVarint(4, entry.timeLastUpdated);
        if (entry.osType != 0) op.WriteVarint(5, entry.osType);
        if (entry.deviceType != 0) op.WriteVarint(6, entry.deviceType);
        body.WriteSubmessage(1, op);
    }
    return RpcResult(std::move(body), sessionConflict ? kEResultDisabled : kEResultOK);
}

RpcResult HandleSuspendSession(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    LOG("[NS] SuspendAppSession app=%u", appId);

    uint32_t accountId = 0;
    if (!RequireAccountId("SuspendAppSession", appId, accountId)) {
        return PB::Writer();
    }

    PendingOpsJournal::Entry session;
    session.operation = PendingOpsJournal::Operation::AppSessionSuspended;
    session.machineName = GetMachineName();
    session.timeLastUpdated = static_cast<uint32_t>(time(nullptr));
    bool cloudSyncCompleted = false;
    for (const auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::Varint) session.clientId = f.varintVal;
        if (f.fieldNum == 3 && f.wireType == PB::LengthDelimited) {
            session.machineName.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
        }
        if (f.fieldNum == 4 && f.wireType == PB::Varint) cloudSyncCompleted = f.varintVal != 0;
    }

    PendingOpsJournal::RecordSuspendState(accountId, appId, session, cloudSyncCompleted);
    return PB::Writer();
}

RpcResult HandleResumeSession(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    LOG("[NS] ResumeAppSession app=%u", appId);

    uint32_t accountId = 0;
    if (!RequireAccountId("ResumeAppSession", appId, accountId)) {
        return PB::Writer();
    }

    uint64_t clientId = 0;
    for (const auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::Varint) {
            clientId = f.varintVal;
        }
    }

    PendingOpsJournal::RecordResumeState(accountId, appId, clientId);
    return PB::Writer();
}

// ClientGetAppQuotaUsage
RpcResult HandleQuotaUsage(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    uint32_t accountId = 0;
    if (!RequireAccountId("ClientGetAppQuotaUsage", appId, accountId)) {
        PB::Writer body;
        body.WriteVarint(1, 0);
        body.WriteVarint(2, 0);
        body.WriteVarint(3, 10000);
        body.WriteVarint(4, 1073741824ULL);
        return body;
    }

    // count from manifest, not blob cache -- blobs may be orphaned/stale
    auto manifest = CloudStorage::LoadLocalManifest(accountId, appId);
    size_t fileCount = 0;
    uint64_t totalBytes = 0;
    for (const auto& [name, entry] : manifest) {
        if (IsReservedBlobFilename(name)) continue;
        ++fileCount;
        totalBytes += entry.size;
    }

    // Report quota from PICS KV injection.
    uint64_t maxBytes = kFallbackQuotaBytes;
    uint32_t maxFiles = kFallbackMaxFiles;
    uint64_t kvQuota = 0;
    uint32_t kvFiles = 0;
    if (SteamKvInjector::IsReady() &&
        SteamKvInjector::ReadAppQuota(appId, kvQuota, kvFiles) &&
        kvQuota > 0 && kvFiles > 0) {
        maxBytes = kvQuota;
        maxFiles = kvFiles;
    }

    PB::Writer body;
    body.WriteVarint(1, ClampFileSizeToUint32((uint64_t)fileCount,
                                              "QuotaUsage.existing_files",
                                              appId, std::string{}));  // existing_files
    body.WriteVarint(2, totalBytes);                 // existing_bytes
    body.WriteVarint(3, maxFiles);                   // max_num_files
    body.WriteVarint(4, maxBytes);                   // max_num_bytes

    LOG("[NS] QuotaUsage app=%u files=%zu bytes=%llu (max %u/%llu)",
        appId, fileCount, totalBytes, maxFiles, (unsigned long long)maxBytes);
    return body;
}

// BeginAppUploadBatch
RpcResult HandleBeginBatch(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    uint64_t batchId = BatchTracker_NextId();
    uint32_t accountId = 0;
    if (!RequireAccountId("BeginAppUploadBatch", appId, accountId)) {
        // Fail early: error skips CompleteBatch, preventing orphaned local blobs.
        return RpcResult(PB::Writer(), kEResultFail);
    }

    uint64_t currentCN = LocalStorage::GetChangeNumber(accountId, appId);
    uint64_t assignedCN = currentCN + 1;
    uint64_t appBuildId = 0;
    PrepareBatchCanonicalTokens(accountId, appId);
    PendingOpsJournal::RecordUploadBatchStart(accountId, appId);

    int uploadCount = 0, deleteCount = 0;
    for (auto& f : reqBody) {
        if (f.fieldNum == 3 && f.wireType == PB::LengthDelimited) {
            std::string name(reinterpret_cast<const char*>(f.data), f.dataLen);
            LOG("[NS-BATCH]   upload: %s", SanitizeForLog(name).c_str());
            TryCaptureRootToken(accountId, appId,
                CanonicalizeUploadRootToken(accountId, appId, StripRootToken(name), ExtractRootToken(name)));
            ++uploadCount;
        }
        if (f.fieldNum == 4 && f.wireType == PB::LengthDelimited) {
            std::string name(reinterpret_cast<const char*>(f.data), f.dataLen);
            LOG("[NS-BATCH]   delete: %s", SanitizeForLog(name).c_str());
            TryCaptureRootToken(accountId, appId,
                CanonicalizeUploadRootToken(accountId, appId, StripRootToken(name), ExtractRootToken(name)));
            ++deleteCount;
        }
        if (f.fieldNum == 6 && f.wireType == PB::Varint) appBuildId = f.varintVal;
    }

    BatchTracker_Begin(accountId, appId, batchId, assignedCN, appBuildId);

    PB::Writer body = CloudRpcUtils::BuildBeginBatchResponseBody(batchId, assignedCN);

    LOG("[NS] BeginBatch app=%u batchId=%llu assignedCN=%llu appBuildId=%llu uploads=%d deletes=%d",
        appId, batchId, assignedCN, (unsigned long long)appBuildId, uploadCount, deleteCount);
    return body;
}

// ClientBeginFileUpload
// Tell Steam to PUT the file to our local HTTP server.
RpcResult HandleBeginFileUpload(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    // extract request fields
    uint64_t fileSize = 0, rawFileSize = 0;
    std::string filename;
    std::vector<uint8_t> fileSha;
    uint64_t timestamp = 0;
    uint32_t platformsToSync = 0xFFFFFFFFu;

    for (auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::Varint) fileSize = f.varintVal;
        if (f.fieldNum == 3 && f.wireType == PB::Varint) rawFileSize = f.varintVal;
        if (f.fieldNum == 4 && f.wireType == PB::LengthDelimited)
            fileSha.assign(f.data, f.data + f.dataLen);
        if (f.fieldNum == 5 && f.wireType == PB::Varint) timestamp = f.varintVal;
        if (f.fieldNum == 6 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
        if (f.fieldNum == 7 && f.wireType == PB::Varint) platformsToSync = (uint32_t)f.varintVal;
    }

    uint16_t port = HttpServer::GetPort();
    uint32_t accountId = 0;
    if (!RequireAccountId("ClientBeginFileUpload", appId, accountId)) {
        return PB::Writer();
    }
    std::string urlHost = "127.0.0.1:" + std::to_string(port);
    std::string rootToken = ExtractRootToken(filename);
    std::string cleanName = StripRootToken(filename);
    if (cleanName.empty()) {
        LOG("[NS-UP] BeginFileUpload app=%u REJECTED: empty filename after token strip", appId);
        return PB::Writer();
    }
    PrepareBatchCanonicalTokens(accountId, appId);
    rootToken = CanonicalizeUploadRootToken(accountId, appId, cleanName, rootToken);

    std::string urlPath = "/upload/" + std::to_string(accountId) + "/" + std::to_string(appId)
        + "/" + HttpUtil::UrlEncode(cleanName, true);

    TryCaptureRootToken(accountId, appId, rootToken);
    BatchTracker_RecordFilePlatforms(accountId, appId, cleanName, platformsToSync);

    LOG("[NS-UP] BeginFileUpload app=%u file=%s (clean=%s) size=%llu rawSize=%llu platforms=0x%08X -> %s%s",
        appId, filename.c_str(), cleanName.c_str(), fileSize, rawFileSize, platformsToSync, urlHost.c_str(), urlPath.c_str());

    uint64_t blockLen = fileSize > 0 ? fileSize : rawFileSize;

    // build block request submessage (ClientCloudFileUploadBlockDetails)
    PB::Writer blockReq;
    blockReq.WriteString(1, urlHost);                // url_host
    blockReq.WriteString(2, urlPath);                // url_path
    blockReq.WriteVarint(3, 0);                      // use_https = false
    blockReq.WriteVarint(4, 4);                      // http_method = PUT (EHTTPMethod: 4)
    // no request_headers needed for our simple server
    blockReq.WriteVarint(6, 0);                      // block_offset = 0
    blockReq.WriteVarint(7, blockLen);               // block_length

    PB::Writer body;
    body.WriteVarint(1, 0);                          // encrypt_file = false
    body.WriteSubmessage(2, blockReq);               // block_requests (repeated, just 1)

    // hex dump response for debugging upload failures
#ifdef DEBUG_HEX_DUMP
    {
        auto& d = body.Data();
        std::string hex;
        for (size_t i = 0; i < d.size(); i++) {
            char tmp[4]; snprintf(tmp, sizeof(tmp), "%02X ", d[i]);
            hex += tmp;
        }
        LOG("[NS-UP] Response hex (%zu bytes): %s", d.size(), hex.c_str());
        auto& bd = blockReq.Data();
        std::string bhex;
        for (size_t i = 0; i < bd.size(); i++) {
            char tmp[4]; snprintf(tmp, sizeof(tmp), "%02X ", bd[i]);
            bhex += tmp;
        }
        LOG("[NS-UP] BlockReq hex (%zu bytes): %s", bd.size(), bhex.c_str());
    }
#endif

    return body;
}

// ClientCommitFileUpload
// The file has been PUT to our HTTP server. Update local metadata.
RpcResult HandleCommitFileUpload(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    bool transferSucceeded = false;
    std::string filename;

    for (auto& f : reqBody) {
        if (f.fieldNum == 1 && f.wireType == PB::Varint) transferSucceeded = (f.varintVal != 0);
        if (f.fieldNum == 4 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    LOG("[NS-UP] CommitFileUpload app=%u file=%s succeeded=%d",
        appId, filename.c_str(), transferSucceeded);

    std::string cleanName = StripRootToken(filename);

    if (cleanName.empty()) {
        LOG("[NS-UP] CommitFileUpload app=%u REJECTED: empty filename after token strip",
            appId);
        PB::Writer body;
        body.WriteVarint(1, 0);  // file_committed = false
        return body;
    }

    // Reject reserved blob names: internal metadata and any `.cloudredirect`
    // file/segment belong outside the per-app save namespace.
    if (IsReservedBlobFilename(cleanName)) {
        LOG("[NS-UP] CommitFileUpload app=%u REJECTED: '%s' is a reserved /blobs/ name",
            appId, cleanName.c_str());
        // Defer blob cleanup: accountId is not yet known at this point.
        // The second check after RequireAccountId below handles the actual
        // cleanup and rejection.
    }
    
    bool committed = false;
    uint32_t accountId = 0;
    if (!RequireAccountId("ClientCommitFileUpload", appId, accountId)) {
        PB::Writer body;
        body.WriteVarint(1, 0);
        return body;
    }

    if (IsReservedBlobFilename(cleanName)) {
        if (HttpServer::HasBlob(accountId, appId, cleanName)) {
            HttpServer::DeleteBlob(accountId, appId, cleanName);
        }
        PB::Writer body;
        body.WriteVarint(1, 0);  // file_committed = false
        return body;
    }

    std::string rootToken = ExtractRootToken(filename);
    PrepareBatchCanonicalTokens(accountId, appId);
    rootToken = CanonicalizeUploadRootToken(accountId, appId, cleanName, rootToken);

    if (transferSucceeded) {
        if (HttpServer::HasBlob(accountId, appId, cleanName)) {
            committed = true;

            // Trust the localhost PUT (Steam server doesn't re-verify SHA either).
            auto blobData = HttpServer::ReadBlob(accountId, appId, cleanName);
            LOG("[NS-UP]   committed: %s (%zu bytes)", cleanName.c_str(), blobData.size());

            {
                const uint8_t* blobPtr = blobData.empty() ? nullptr : blobData.data();
                uint64_t batchId = BatchTracker_ActiveId(accountId, appId);
                bool isStaged = (batchId != 0);
                bool stored = isStaged
                    ? CloudStorage::StoreBlobStaged(accountId, appId, batchId,
                        cleanName, blobPtr, blobData.size())
                    : CloudStorage::StoreBlob(accountId, appId, cleanName,
                        blobPtr, blobData.size());
                if (!stored) {
                    LOG("[NS-UP]   ERROR: failed to store blob for %s", cleanName.c_str());
                    committed = false;
                    HttpServer::DeleteBlob(accountId, appId, cleanName);
                } else if (!isStaged) {
                    auto entry = LocalStorage::GetFileEntry(accountId, appId, cleanName);
                    if (entry) {
                        CloudStorage::UpdateManifestEntry(accountId, appId, cleanName,
                            entry->sha, entry->timestamp, entry->rawSize);
                    }
                }
            }

            if (committed) {
                if (RecordFileToken(accountId, appId, cleanName, rootToken)) {
                    MarkFileTokensDirty(accountId, appId);
                }
                BatchTracker_RecordUpload(accountId, appId, cleanName);
            }
        } else {
            LOG("[NS-UP]   WARNING: blob not found after PUT for %s (clean=%s)", filename.c_str(), cleanName.c_str());
        }

    } else {
        // Don't delete blobs on transfer failure -- orphans are cleaned at batch completion.
        LOG("[NS-UP]   transfer failed for %s, skipping blob cleanup (concurrent PUT may have succeeded)",
            cleanName.c_str());
    }

    PB::Writer body;
    body.WriteVarint(1, committed ? 1 : 0);          // file_committed
    return body;
}

// CompleteAppUploadBatchBlocking
RpcResult HandleCompleteBatch(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    auto completeInfo = CloudRpcUtils::ParseCompleteBatchRequest(reqBody);

    // Increment CN once per batch; cloud publish detached.
    uint32_t accountId = 0;
    if (!RequireAccountId("CompleteAppUploadBatchBlocking", appId, accountId)) {
        return PB::Writer();
    }

    UploadBatchState batch = BatchTracker_Get(accountId, appId, completeInfo.batchId);
    if (batch.batchId == 0) {
        LOG("[NS] CompleteBatch app=%u requested batch %llu but no active batch exists",
            appId, (unsigned long long)completeInfo.batchId);
        PendingOpsJournal::RecordUploadBatchInterrupted(accountId, appId);
        ClearBatchCanonicalTokens(accountId, appId);
        return PB::Writer();
    }
    if (completeInfo.hasResult && completeInfo.result != 1) {
        LOG("[NS] CompleteBatch app=%u batch=%llu reported Steam upload result %u; refusing CN advance",
            appId, (unsigned long long)batch.batchId, completeInfo.result);
        PendingOpsJournal::RecordUploadBatchInterrupted(accountId, appId);
        BatchTracker_Clear(accountId, appId, batch.batchId);
        ClearBatchCanonicalTokens(accountId, appId);
        ClearFileTokensDirty(accountId, appId);
        return PB::Writer();
    }

    // Drain deferred file-token persists for this app only.
    {
        uint64_t key = MakeAppAccountKey(accountId, appId);
        bool wasDirty = false;
        {
            std::lock_guard<std::mutex> lock(g_fileTokensDirtyMutex);
            wasDirty = g_fileTokensDirtyApps.erase(key) > 0;
        }
        if (wasDirty) {
            PersistFileTokens(accountId, appId);
        }
    }
    std::vector<std::string> uploads(batch.uploads.begin(), batch.uploads.end());
    std::vector<std::string> deletes(batch.deletes.begin(), batch.deletes.end());
    if (!CloudStorage::PromoteStagedBatchForCommit(accountId, appId,
            batch.batchId, uploads, deletes)) {
        LOG("[NS] CompleteBatch app=%u refused CN advance: staged promotion failed",
            appId);
        PendingOpsJournal::RecordUploadBatchInterrupted(accountId, appId);
        BatchTracker_Clear(accountId, appId, batch.batchId);
        ClearFileTokensDirty(accountId, appId);
        ClearBatchCanonicalTokens(accountId, appId);
        return PB::Writer();
    }

    uint64_t newCN = batch.assignedCN;
    {
        // Sync mutex: serialize CN set + state publish.
        auto syncMtx = CloudStorage::AcquireAppSyncMutex(accountId, appId);
        std::lock_guard<std::mutex> syncLock(*syncMtx);

        // Fetch existing cloud state; fall back to local manifest.
        CloudStorage::CloudAppState state;
        bool haveCloudBase = false;
        if (CloudStorage::IsCloudActive()) {
            auto result = CloudStorage::FetchCloudState(accountId, appId);
            if (result.status == CloudStorage::StateFetchStatus::Ok) {
                state = std::move(result.state);
                haveCloudBase = true;
            }
        }
        // If cloud CN is behind local, rebuild file list from manifest (keep session/quota).
        uint64_t localCN = LocalStorage::GetChangeNumber(accountId, appId);
        if (!haveCloudBase || state.cn < localCN) {
            if (haveCloudBase && state.cn < localCN) {
                LOG("[NS] CompleteBatch app %u: cloud CN %llu < local CN %llu, rebuilding file list from local manifest",
                    appId, (unsigned long long)state.cn, (unsigned long long)localCN);
            }
            state.files.clear();
            auto localManifest = CloudStorage::LoadLocalManifest(accountId, appId);
            for (const auto& [name, me] : localManifest) {
                CloudStorage::FileEntry fe;
                fe.sha = me.sha;
                fe.timestamp = me.timestamp;
                fe.size = me.size;
                state.files[name] = std::move(fe);
            }
        }

        for (const auto& filename : deletes)
            state.files.erase(filename);

        for (const auto& filename : uploads) {
            if (IsReservedBlobFilename(filename)) continue;
            auto entry = LocalStorage::GetFileEntry(accountId, appId, filename);
            if (!entry.has_value()) continue;
            CloudStorage::FileEntry fe;
            fe.sha = entry->sha;
            fe.timestamp = entry->timestamp;
            fe.size = entry->rawSize;
            auto ptIt = batch.filePlatforms.find(filename);
            fe.platformsToSync = (ptIt != batch.filePlatforms.end())
                ? ptIt->second : 0xFFFFFFFFu;
            state.files[filename] = std::move(fe);
        }

        state.cn = newCN;
        state.appBuildId = batch.appBuildId;

        LocalStorage::SetChangeNumber(accountId, appId, newCN);
        CloudStorage::Manifest updatedManifest;
        for (const auto& [name, fe] : state.files) {
            CloudStorage::ManifestEntry me;
            me.sha = fe.sha;
            me.timestamp = fe.timestamp;
            me.size = fe.size;
            updatedManifest[name] = std::move(me);
        }
        CloudStorage::SaveManifestLocal(accountId, appId, updatedManifest);
        CloudStorage::SaveManifestSnapshot(accountId, appId, newCN);

        // Synchronous first attempt (must finish before ExitSyncDone).
        // Steam ignores CompleteBatch eresult, so retry async on failure.
        if (!CloudStorage::PublishCloudState(accountId, appId, state)) {
            LOG("[NS] CompleteBatch: state publish failed for app %u; scheduling async retry", appId);
            // Capture file data only; retry re-fetches live state.
            auto filesToMerge = std::make_shared<std::unordered_map<std::string, CloudStorage::FileEntry>>(state.files);
            uint64_t retryCN = state.cn;
            uint64_t retryBuildId = state.appBuildId;
            std::thread([filesToMerge, retryCN, retryBuildId, accountId, appId] {
                CloudStorage::InflightSyncScope guard;
                if (!guard.entered) return;
                constexpr int kMaxRetries = 3;
                constexpr int kBaseDelayMs = 2000;
                for (int attempt = 1; attempt <= kMaxRetries; ++attempt) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(kBaseDelayMs * attempt));
                    // Re-fetch live state under sync mutex to preserve session changes.
                    auto syncMtx = CloudStorage::AcquireAppSyncMutex(accountId, appId);
                    std::lock_guard<std::mutex> lock(*syncMtx);
                    auto result = CloudStorage::FetchCloudState(accountId, appId);
                    if (result.status != CloudStorage::StateFetchStatus::Ok) {
                        // Cloud fetch failed; skip to avoid erasing session lock.
                        LOG("[NS] CompleteBatch: retry %d/%d skipped for app %u: cloud fetch failed",
                            attempt, kMaxRetries, appId);
                        continue;
                    }
                    CloudStorage::CloudAppState retryState = std::move(result.state);
                    // Abort if a newer CN already committed.
                    if (retryState.cn > retryCN) {
                        LOG("[NS] CompleteBatch: retry aborted for app %u: cloud CN %llu > batch CN %llu",
                            appId, retryState.cn, retryCN);
                        return;
                    }
                    retryState.files = *filesToMerge;
                    retryState.cn = retryCN;
                    retryState.appBuildId = retryBuildId;
                    if (CloudStorage::PublishCloudState(accountId, appId, retryState)) {
                        LOG("[NS] CompleteBatch: async retry %d/%d succeeded for app %u",
                            attempt, kMaxRetries, appId);
                        return;
                    }
                    LOG("[NS] CompleteBatch: async retry %d/%d failed for app %u",
                        attempt, kMaxRetries, appId);
                }
                LOG("[NS] CompleteBatch: all retries exhausted for app %u; "
                    "remote state stale until next sync", appId);
            }).detach();
        }
    }

    BatchTracker_Clear(accountId, appId, batch.batchId);
    PendingOpsJournal::RecordUploadBatchEnd(accountId, appId);
    LOG("[NS] CompleteBatch app=%u CN=%llu (state published atomically)", appId, newCN);

    ClearBatchCanonicalTokens(accountId, appId);

    // Fire-and-forget GC after successful commit.
    std::thread([accountId, appId]() {
        CloudStorage::InflightSyncScope guard;
        if (!guard.entered) return;
        CloudStorage::GarbageCollectBlobs(accountId, appId);
    }).detach();

    PB::Writer body; // empty response
    return body;
}

// ClientFileDownload
// Tell Steam to GET the file from our local HTTP server.
RpcResult HandleFileDownload(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    std::string filename;
    for (auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    uint32_t accountId = 0;
    if (!RequireAccountId("ClientFileDownload", appId, accountId)) {
        return PB::Writer();
    }
    uint16_t port = HttpServer::GetPort();
    std::string urlHost = "127.0.0.1:" + std::to_string(port);
    std::string cleanName = StripRootToken(filename);
    if (cleanName.empty()) {
        LOG("[NS-DL] FileDownload app=%u REJECTED: empty filename after token strip", appId);
        return PB::Writer();
    }
    std::string urlPath = "/download/" + std::to_string(accountId) + "/" + std::to_string(appId)
        + "/" + HttpUtil::UrlEncode(cleanName, true);

    uint64_t fileSize = 0;    uint64_t timestamp = 0;
    std::vector<uint8_t> sha;

    // Check local manifest FIRST - this is saved from cloud during GetChangelist
    // and represents what we told Steam the file looks like. Must match what we serve.
    auto manifest = CloudStorage::LoadLocalManifest(accountId, appId);
    auto it = manifest.find(cleanName);
    if (it != manifest.end()) {
        fileSize = it->second.size;
        timestamp = it->second.timestamp;
        sha = it->second.sha;
    } else {
        // Fallback: check local storage (for files uploaded locally but not yet in manifest)
        auto entry = LocalStorage::GetFileEntry(accountId, appId, cleanName);
        if (entry) {
            fileSize = entry->rawSize;
            timestamp = entry->timestamp;
            sha = entry->sha;
        } else {
            // Last resort: check blob size on disk
            fileSize = HttpServer::GetBlobSize(accountId, appId, cleanName);
        }
    }

    LOG("[NS-DL] FileDownload app=%u file=%s (clean=%s) size=%llu -> %s%s",
        appId, filename.c_str(), cleanName.c_str(), fileSize, urlHost.c_str(), urlPath.c_str());

    uint32_t clampedSize = ClampFileSizeToUint32(fileSize,
                                                 "FileDownload_Response.file_size",
                                                 appId, filename);
    PB::Writer body;
    body.WriteVarint(1, appId);                      // appid
    body.WriteVarint(2, clampedSize);                // file_size (compressed = same)
    body.WriteVarint(3, clampedSize);                // raw_file_size
    if (!sha.empty())
        body.WriteBytes(4, sha.data(), sha.size());  // sha_file
    body.WriteVarint(5, timestamp);                  // time_stamp
    body.WriteVarint(6, 0);                          // is_explicit_delete = false
    body.WriteString(7, urlHost);                    // url_host
    body.WriteString(8, urlPath);                    // url_path
    body.WriteVarint(9, 0);                          // use_https = false
    // no request_headers (field 10)
    body.WriteVarint(11, 0);                         // encrypted = false

    return body;
}

// ClientDeleteFile
RpcResult HandleDeleteFile(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    std::string filename;
    for (auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    std::string cleanName = StripRootToken(filename);
    LOG("[NS] DeleteFile app=%u file=%s (clean=%s)", appId, filename.c_str(), cleanName.c_str());

    if (cleanName.empty()) {
        LOG("[NS] DeleteFile app=%u REJECTED: empty filename", appId);
        return PB::Writer();
    }

    if (IsReservedBlobFilename(cleanName)) {
        LOG("[NS] DeleteFile app=%u ignored for reserved /blobs/ name %s", appId, cleanName.c_str());
        return PB::Writer();
    }

    uint32_t accountId = 0;
    if (!RequireAccountId("ClientDeleteFile", appId, accountId)) {
        return PB::Writer();
    }

    HttpServer::DeleteBlob(accountId, appId, cleanName);
    uint64_t batchId = BatchTracker_ActiveId(accountId, appId);
    bool deleted = batchId == 0
        ? CloudStorage::DeleteBlob(accountId, appId, cleanName)
        : CloudStorage::DeleteBlobStaged(accountId, appId, cleanName);

    if (RemoveFileToken(accountId, appId, cleanName)) {
        MarkFileTokensDirty(accountId, appId);
    }
    if (deleted) BatchTracker_RecordDelete(accountId, appId, cleanName);
    
    // Staged batches publish a full manifest at CompleteBatch, so only live
    // deletes update committed manifest state immediately.
    if (batchId == 0) {
        CloudStorage::RemoveManifestEntry(accountId, appId, cleanName);
    }

    PB::Writer body; // empty response
    return body;
}

} // namespace CloudIntercept
