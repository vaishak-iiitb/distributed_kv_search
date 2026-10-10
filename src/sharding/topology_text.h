#pragma once

// Text formats used by the process tooling (kv_metadata_server, kv_admin).
// Pure parsing/formatting: no sockets, no Raft, no files. That is deliberate,
// so every rule below is unit-testable in milliseconds.

#include <cstdint>
#include <string>
#include <vector>

#include "sharding/raft_group_client.h"
#include "sharding/shard_map.h"

namespace sharding {

// ---- Cluster spec: how a metadata-group process learns its group ----------
//
//   "0=127.0.0.1:7000/127.0.0.1:7100,1=127.0.0.1:7001/127.0.0.1:7101,..."
//
// Each entry is  <node id>=<raft host:port>/<client host:port>.
// Ids must be unique; they become the Raft NodeIds as written.
struct ClusterNode {
    std::uint32_t id = 0;
    Endpoint raft;    // Raft-to-Raft address
    Endpoint client;  // client-facing (ClientServer) address
};

// Returns false and fills *err (naming the offending entry) on any problem.
bool ParseClusterSpec(const std::string& text, std::vector<ClusterNode>* out, std::string* err);

// ---- Map file: the human-written description of a ShardMap -----------------
//
//   # comments start with '#', blank lines are ignored
//   version 3
//   vnodes 128            (optional, default 128)
//   # shard node raft_addr client_addr
//   0 0 127.0.0.1:7000 127.0.0.1:7100
//   0 1 127.0.0.1:7001 127.0.0.1:7101
//   1 0 127.0.0.1:7010 127.0.0.1:7110
//
// Node indexes within a shard must be exactly 0..n-1 (no gaps, no repeats):
// the index is the node's identity (it becomes the Raft NodeId), so a file
// that skips or repeats one is rejected rather than silently renumbered.
// Lines may appear in any order; the resulting ShardMap lists shards by
// ascending id and each shard's nodes by ascending index.
//
// Structural checks only. Callers still run ShardMap::Validate before Encode.
bool ParseMapFile(const std::string& text, ShardMap* out, std::string* err);

// Inverse of ParseMapFile (output parses back to an equal map).
std::string FormatMapFile(const ShardMap& map);

}  // namespace sharding