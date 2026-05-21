#pragma once
// Inject ufs.quota/maxnumfiles and savefiles rules into Steam's in-memory KV
// for namespace apps missing PICS data.

#include <cstdint>
#include <string>
#include <vector>

namespace SteamKvInjector {

bool Init();

bool IsReady();

// Read current ufs.quota/maxnumfiles from KV. False if injector not ready.
bool ReadAppQuota(uint32_t appId, uint64_t& outQuotaBytes, uint32_t& outMaxNumFiles);

// Trigger PICS fetch and poll for results. False on timeout.
bool TriggerPicsAndWait(uint32_t appId,
                        uint64_t& outQuotaBytes,
                        uint32_t& outMaxNumFiles,
                        int timeoutMs = 500);

// Write quota/maxnumfiles into KV. Won't clobber existing non-zero values.
bool InjectAppQuota(uint32_t appId, uint64_t quotaBytes, uint32_t maxNumFiles);

// A single AutoCloud save-file rule for KV injection.
struct SaveFileRule {
    std::string root;       // e.g. "WinAppDataLocal"
    std::string path;       // e.g. "Katana_ZERO"
    std::string pattern;    // e.g. "*.zero"
    bool recursive = false;
    uint32_t platforms = 0xFFFFFFFFu;  // bitmask: Win=1, Mac=2, Linux=8; -1=all
};

// Inject savefiles rules into UFS KV. Won't clobber existing children.
bool InjectSaveFiles(uint32_t appId, const std::vector<SaveFileRule>& rules);

} // namespace SteamKvInjector
