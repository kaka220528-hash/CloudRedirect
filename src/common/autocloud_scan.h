#pragma once
// AutoCloud scan - parses appinfo.vdf AutoCloud rules and produces
// a list of matching save files on disk.

#include <string>
#include <vector>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include "autocloud_util.h"

namespace AutoCloudScan {

// Result of scanning AutoCloud rules for a single app.
struct FileEntry {
    std::string relativePath;   // Path relative to app root (e.g., "save/slot1.dat")
    std::string fullPath;       // Absolute filesystem path
    uint64_t size = 0;
    uint64_t modifiedTime = 0;  // Unix timestamp
    std::vector<uint8_t> sha;   // SHA1 hash (20 bytes)
    std::string rootToken;      // Cloud root token (e.g., "%WinAppDataLocal%")
    uint32_t rootId = 0;        // Steam ERemoteStorageFileRoot enum value
    std::vector<uint8_t> content; // bytes read while hashing; empty if not retained
};

struct ScanResult {
    std::vector<FileEntry> files;
    std::unordered_set<std::string> ruleRootTokens;  // cloud root tokens from parsed rules (even if 0 files matched)
    bool complete = false;          // true if scan completed without truncation or collision
    bool hasRules = false;          // true if app has AutoCloud rules in appinfo.vdf
    bool hasRootCollision = false;  // true if two rules resolved to same path under different roots

    // Per-rule double-count accounting for the mixed-root quota multiplier.
    //
    // Steam's CAutoCloudManager::YldOnAppExit walks the savefiles rules and counts
    // each matching file once PER RULE -- the native per-rule dedup (sub_1384D1DA0
    // @ 0x1384d221a) is dead on the exit path because YldOnAppExit seeds it with a
    // null "previous path" (it is only live on the staging/save path). So when two
    // effective-platform rules resolve to the same directory (via rootoverrides),
    // every file there is counted twice against maxnumfiles -> false over-quota ->
    // cloud wipe. `files` below is the UNIQUE set (cross-rule deduped at scan time);
    // these two fields capture what the native exit loop actually counts so the
    // multiplier can size the budget to the real worst case rather than a blunt
    // fileCount*ruleCount.

    // Total file-claims summed across all effective rules (counts a file once for
    // each rule, including siblings, whose resolved scan dir + pattern match it).
    // This equals the instance count YldOnAppExit charges against maxnumfiles.
    size_t countedInstances = 0;
    // Largest number of effective rules that resolve to (and claim files in) the
    // same physical directory. 1 = no collision (native dedup irrelevant, no wipe
    // risk); >1 = the per-file multiplication factor on the exit path.
    size_t maxCollisionFactor = 0;
    // Sum of (1 + siblingCount) over rules that participate in a collision; mirrors
    // the extra budget YldOnAppExit's loop consumes per file beyond the raw count.
    size_t collisionSiblingHeadroom = 0;
};

// Scan AutoCloud rules for an app and return matching files from disk.
ScanResult GetFileList(const std::string& steamPath,
                       uint32_t accountId, uint32_t appId);

// Check if an app is installed in any Steam library folder.
// Returns true if appmanifest_<appId>.acf exists in any library.
bool IsAppInstalled(const std::string& steamPath, uint32_t appId);

// Parse AutoCloud savefiles rules from appinfo.vdf for KV injection.
std::vector<AutoCloudUtil::AutoCloudRuleNative> GetRules(
    const std::string& steamPath, uint32_t appId, uint32_t accountId = 0);

// Get raw rootoverrides for an app from appinfo.vdf.
// Returns empty vector if app has no rootoverrides or appinfo can't be parsed.
std::vector<AutoCloudUtil::AutoCloudRootOverrideNative> GetRootOverrides(
    const std::string& steamPath, uint32_t appId);

// Build root-token -> directory map for restoring blobs to game save folders.
std::unordered_map<std::string, std::string> GetRootTokenDirectories(
    const std::string& steamPath, uint32_t appId, uint32_t accountId = 0);

// Look up a game's display name from Steam's appinfo.vdf cache.
// Returns empty string if not found.
std::string GetAppName(const std::string& steamPath, uint32_t appId);

#ifdef CLOUDREDIRECT_TESTING
// Test seam: read a file once, returning its SHA1 and (in outBytes) the exact
// bytes read. Underpins the scan->commit race fix (bytes are captured during
// hashing so the commit never re-reads). Returns empty SHA on error.
std::vector<uint8_t> TestReadAndHashFile(const std::string& path,
                                         std::vector<uint8_t>& outBytes);
#endif

} // namespace AutoCloudScan
