#pragma once
#include "protobuf.h"
#include "rpc_handlers.h"
#include <vector>
#include <cstdint>
#include <optional>
#include <functional>

namespace StatsHandlers {

// Service RPC method names
inline constexpr const char* RPC_GET_USER_STATS = "Player.GetUserStats#1";
inline constexpr const char* RPC_GET_LAST_PLAYED = "Player.ClientGetLastPlayedTimes#1";

// Namespace-app predicate. The platform layer installs this so playtime
// session tracking (and any persistence) is restricted to namespace/lua apps
// only -- real owned games must NOT have their playtime tracked or synced.
// Returns true iff appId is a namespace app we own.
using NamespacePredicate = std::function<bool(uint32_t appId)>;
void SetNamespacePredicate(NamespacePredicate pred);

// Legacy EMsg numbers
inline constexpr uint32_t EMSG_CLIENT_GET_USER_STATS      = 818;
inline constexpr uint32_t EMSG_CLIENT_GET_USER_STATS_RESP = 819;
inline constexpr uint32_t EMSG_CLIENT_STORE_USER_STATS2   = 820;
inline constexpr uint32_t EMSG_CLIENT_STORE_USER_STATS_RESP = 821;
inline constexpr uint32_t EMSG_CLIENT_GAMES_PLAYED        = 5410;

// Initialize stats system (call after StatsStore::Init)
void Init();

// Service RPC handler for Player.GetUserStats#1
CloudIntercept::RpcResult HandleGetUserStats(uint32_t appId, const std::vector<PB::Field>& reqBody);

// Service RPC handler for Player.ClientGetLastPlayedTimes#1
CloudIntercept::RpcResult HandleGetLastPlayedTimes(const std::vector<PB::Field>& reqBody);

// Legacy EMsg handlers - return response body bytes
// Returns nullopt if this EMsg should pass through to real server
std::optional<std::vector<uint8_t>> HandleLegacyGetUserStats(
    const uint8_t* body, size_t bodyLen, uint64_t steamId);

std::optional<std::vector<uint8_t>> HandleLegacyStoreUserStats2(
    const uint8_t* body, size_t bodyLen, uint64_t steamId);

// Called when we see CMsgClientGamesPlayed (EMsg 5410) pass through.
// We don't intercept it - just observe it to track playtime.
void ObserveGamesPlayed(const uint8_t* body, size_t bodyLen);

// Shutdown - flush and cleanup
void Shutdown();

} // namespace StatsHandlers
