// The 12-process integration test (3 metadata nodes + 2 shards x 3 nodes,
// plus this test process acting as the client): the real binaries
// kv_metadata_server and kv_shard_server, real sockets, real Raft, real disk.
//
// Phases:
//   1  boot         start metadata group, start shard nodes BEFORE any map
//                   exists (they must wait), publish the map, wait until
//                   every shard can commit a write
//   2  routing      200 keys through ClientRouter; each is readable from its
//                   owning shard directly, and a non-owning shard answers
//                   WRONG_SHARD <map version> for it
//   3  shard crash  for EACH shard: find its current LEADER (via the LEADER
//                   probe) and SIGKILL it. The shard keeps serving every old
//                   key and accepts new writes (2 of 3 is a majority);
//                   restart the node: it rejoins and the shard keeps serving.
//                   Then, kWriteKillRounds times per shard: acknowledged Put,
//                   SIGKILL the leader IMMEDIATELY (no pause), read the key
//                   back. This is the narrow window where followers may not
//                   yet have learned the commit index (heartbeats are 50 ms
//                   apart), so a new leader must not serve a stale read
//   4  meta crash   find the metadata group's LEADER and SIGKILL it: a FRESH
//                   router can still fetch the map and route
//   5  shutdown     SIGTERM everything left: every process must exit 0
//                   (this exercises the stop-ClientServer-before-RaftNode
//                   ordering in both launchers)
//
// Usage: multi_shard_test [bin_dir]
//   bin_dir defaults to the directory containing this executable; it must
//   hold kv_metadata_server and kv_shard_server.
// Exit codes: 0 all passed | 1 a check failed | 2 environment problem
//   (binary missing, port busy, cannot create work dir). Logs of every
//   process are kept in the printed work dir when the test fails.

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "net/line_client.h"
#include "process_harness.h"
#include "sharding/client_router.h"
#include "sharding/metadata_client.h"
#include "sharding/raft_group_client.h"
#include "sharding/shard_map.h"

namespace {

using sharding::ClientRouter;
using sharding::Endpoint;
using sharding::RaftGroupClient;
using sharding::RaftGroupClientOptions;
using sharding::ShardId;
using std::chrono::milliseconds;
namespace fs = std::filesystem;

int g_failures = 0;

#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                                 \
        }                                                                                 \
    } while (0)

// Like CHECK but also prints why (for operations returning a Result).
#define CHECK_OK(result_expr)                                                                   \
    do {                                                                                        \
        const auto r_ = (result_expr);                                                          \
        if (r_.status != RaftGroupClient::Status::kOk) {                                        \
            std::fprintf(stderr, "%s:%d: not OK: %s -> status %d (%s)\n", __FILE__, __LINE__,   \
                         #result_expr, static_cast<int>(r_.status), r_.detail.c_str());         \
            ++g_failures;                                                                       \
        }                                                                                       \
    } while (0)

// ---- fixed topology: ports are constants so a stuck run is easy to diagnose
constexpr int kNumShards = 2;
constexpr int kNodesPerShard = 3;
constexpr int kMetaRaftBase = 21000;
constexpr int kMetaClientBase = 21010;
constexpr int kShardRaftBase = 21100;    // + shard * 10 + node
constexpr int kShardClientBase = 21200;  // + shard * 10 + node
constexpr std::uint64_t kMapVersion = 1;
constexpr int kNumKeys = 200;
constexpr int kWriteKillRounds = 5;  // write-then-kill rounds per shard

int ShardRaftPort(int shard, int node) { return kShardRaftBase + shard * 10 + node; }
int ShardClientPort(int shard, int node) { return kShardClientBase + shard * 10 + node; }

Endpoint Local(int port) { return Endpoint{"127.0.0.1", port}; }
std::string Addr(int port) { return "127.0.0.1:" + std::to_string(port); }

struct Cluster {
    std::string bin_dir;
    std::string work_dir;

    sharding::ShardMap map;
    std::shared_ptr<const sharding::HashRing> ring;
    std::vector<Endpoint> meta_endpoints;  // client-facing
    std::vector<std::vector<Endpoint>> shard_endpoints;  // [shard][node], client-facing

    std::vector<std::unique_ptr<harness::Process>> meta;                   // [node id]
    std::vector<std::vector<std::unique_ptr<harness::Process>>> shards;    // [shard][node]
};

std::string KeyName(int i) { return "user-" + std::to_string(i); }
std::string ValueFor(int i) { return "val-" + std::to_string(i); }

ShardId OwnerOf(const Cluster& c, const std::string& key) {
    return *c.ring->Lookup(key);  // ring is never empty here
}

// A key the ring assigns to `shard` (for readiness probes).
std::string KeyOwnedBy(const Cluster& c, ShardId shard, const std::string& prefix) {
    for (int i = 0; i < 100000; ++i) {
        const std::string key = prefix + std::to_string(i);
        if (OwnerOf(c, key) == shard) return key;
    }
    return "";
}

bool ReadyProbe(const Endpoint& ep) {
    const auto reply = net::LineClient::RoundTrip(ep.host, ep.port, "LEADER", 500);
    return reply.has_value();  // any answer (LEADER / NOT_LEADER ...) = serving
}

// Index (into `endpoints`) of the node whose client port currently answers
// "LEADER", or -1 if none does within the timeout. Polls, because an election
// may still be settling. Node order in `endpoints` equals node id.
int FindLeader(const std::vector<Endpoint>& endpoints, std::chrono::milliseconds timeout) {
    int found = -1;
    harness::WaitUntil(
        [&] {
            for (std::size_t i = 0; i < endpoints.size(); ++i) {
                const auto reply = net::LineClient::RoundTrip(endpoints[i].host,
                                                              endpoints[i].port, "LEADER", 300);
                if (reply.has_value() && *reply == "LEADER") {
                    found = static_cast<int>(i);
                    return true;
                }
            }
            return false;
        },
        timeout, milliseconds(100));
    return found;
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

bool PreflightPorts() {
    std::vector<int> ports;
    for (int i = 0; i < 3; ++i) {
        ports.push_back(kMetaRaftBase + i);
        ports.push_back(kMetaClientBase + i);
    }
    for (int s = 0; s < kNumShards; ++s) {
        for (int n = 0; n < kNodesPerShard; ++n) {
            ports.push_back(ShardRaftPort(s, n));
            ports.push_back(ShardClientPort(s, n));
        }
    }
    bool ok = true;
    for (int port : ports) {
        if (!harness::PortIsFree(port)) {
            std::cerr << "port " << port << " is already in use (a previous run still alive? "
                      << "try: pkill kv_metadata_server; pkill kv_shard_server)\n";
            ok = false;
        }
    }
    return ok;
}

bool BuildTopology(Cluster* c) {
    c->map.version = kMapVersion;
    c->map.vnodes = 64;
    for (int s = 0; s < kNumShards; ++s) {
        sharding::ShardInfo info;
        info.id = static_cast<ShardId>(s);
        std::vector<Endpoint> endpoints;
        for (int n = 0; n < kNodesPerShard; ++n) {
            info.nodes.push_back(
                sharding::NodeAddr{Addr(ShardRaftPort(s, n)), Addr(ShardClientPort(s, n))});
            endpoints.push_back(Local(ShardClientPort(s, n)));
        }
        c->map.shards.push_back(info);
        c->shard_endpoints.push_back(endpoints);
    }
    std::string err;
    if (!c->map.Validate(&err)) {
        std::cerr << "test map is not valid: " << err << "\n";
        return false;
    }
    c->ring = c->map.BuildRing();
    for (int i = 0; i < 3; ++i) c->meta_endpoints.push_back(Local(kMetaClientBase + i));
    return true;
}

void SpawnProcesses(Cluster* c) {
    std::string cluster_spec, meta_list;
    for (int i = 0; i < 3; ++i) {
        if (i > 0) {
            cluster_spec += ",";
            meta_list += ",";
        }
        cluster_spec += std::to_string(i) + "=" + Addr(kMetaRaftBase + i) + "/" +
                        Addr(kMetaClientBase + i);
        meta_list += Addr(kMetaClientBase + i);
    }

    const std::string logs = c->work_dir + "/logs";
    for (int i = 0; i < 3; ++i) {
        c->meta.push_back(std::make_unique<harness::Process>(
            "meta" + std::to_string(i), c->bin_dir + "/kv_metadata_server",
            std::vector<std::string>{"--id", std::to_string(i), "--cluster", cluster_spec,
                                     "--data-dir", c->work_dir + "/meta"},
            logs + "/meta" + std::to_string(i) + ".log"));
    }
    for (int s = 0; s < kNumShards; ++s) {
        c->shards.emplace_back();
        for (int n = 0; n < kNodesPerShard; ++n) {
            c->shards[s].push_back(std::make_unique<harness::Process>(
                "shard" + std::to_string(s) + "_node" + std::to_string(n),
                c->bin_dir + "/kv_shard_server",
                std::vector<std::string>{"--shard", std::to_string(s), "--node", std::to_string(n),
                                         "--metadata", meta_list, "--data-dir",
                                         c->work_dir + "/shard"},
                logs + "/shard" + std::to_string(s) + "_node" + std::to_string(n) + ".log"));
        }
    }
}

RaftGroupClientOptions QuickOptions() {
    RaftGroupClientOptions o;
    o.attempt_timeout_ms = 1000;
    o.overall_timeout_ms = 3000;
    o.retry_backoff_ms = 50;
    return o;
}

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

bool PhaseBoot(Cluster& c) {
    std::cout << "[1/5] boot: metadata group, shard nodes waiting, publish, elections\n";

    for (auto& p : c.meta) CHECK(p->Start());
    // Shard nodes start BEFORE any map is published: they must sit in their
    // bootstrap loop (kv_shard_server's default is to wait forever).
    for (auto& shard : c.shards) {
        for (auto& p : shard) CHECK(p->Start());
    }
    std::this_thread::sleep_for(milliseconds(500));
    for (auto& p : c.meta) CHECK(p->Running());
    for (auto& shard : c.shards) {
        for (auto& p : shard) CHECK(p->Running());  // still waiting, not crashed
    }
    if (g_failures != 0) return false;

    sharding::MetadataClient metadata(c.meta_endpoints, QuickOptions());
    const std::string encoded = c.map.Encode();
    bool published = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto r = metadata.Publish(encoded);
        if (r.status == sharding::MetadataClient::PublishStatus::kOk) {
            published = true;
            break;
        }
        if (r.status == sharding::MetadataClient::PublishStatus::kStaleVersion) {
            std::cerr << "unexpected: map already published (work dir not fresh?)\n";
            return false;
        }
        std::this_thread::sleep_for(milliseconds(250));
    }
    CHECK(published);
    if (!published) return false;

    // Every shard must be able to COMMIT a write (leader elected, quorum up).
    ClientRouter router(c.meta_endpoints);
    for (int s = 0; s < kNumShards; ++s) {
        const std::string canary = KeyOwnedBy(c, static_cast<ShardId>(s), "canary-");
        bool ready = false;
        const auto ready_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < ready_deadline) {
            if (router.Put(canary, "ready").status == RaftGroupClient::Status::kOk) {
                ready = true;
                break;
            }
            std::this_thread::sleep_for(milliseconds(250));
        }
        if (!ready) std::cerr << "shard " << s << " never became writable\n";
        CHECK(ready);
        if (!ready) return false;
    }
    return true;
}

bool PhaseRouting(Cluster& c, ClientRouter& router) {
    std::cout << "[2/5] routing: " << kNumKeys << " keys, ownership enforced on the wire\n";

    std::vector<int> per_shard(kNumShards, 0);
    int put_failures = 0;
    for (int i = 0; i < kNumKeys && put_failures < 3; ++i) {
        const auto r = router.Put(KeyName(i), ValueFor(i));
        if (r.status != RaftGroupClient::Status::kOk) {
            ++put_failures;
            std::fprintf(stderr, "Put %s failed: status %d (%s)\n", KeyName(i).c_str(),
                         static_cast<int>(r.status), r.detail.c_str());
        }
        ++per_shard[OwnerOf(c, KeyName(i))];
    }
    CHECK(put_failures == 0);
    if (put_failures != 0) return false;

    // Sanity: the ring really spread the keys (loose bound; this is not a
    // statistics test, just "not everything on one shard").
    for (int s = 0; s < kNumShards; ++s) CHECK(per_shard[s] >= kNumKeys / 10);

    // Read everything back through the router.
    int get_failures = 0;
    for (int i = 0; i < kNumKeys && get_failures < 3; ++i) {
        const auto r = router.Get(KeyName(i));
        if (r.status != RaftGroupClient::Status::kOk || r.value != ValueFor(i)) {
            ++get_failures;
            std::fprintf(stderr, "Get %s: status %d value '%s' (%s)\n", KeyName(i).c_str(),
                         static_cast<int>(r.status), r.value.c_str(), r.detail.c_str());
        }
    }
    CHECK(get_failures == 0);

    // Talk to each shard's group directly: the owner has the value, the other
    // shard refuses with the map version.
    std::vector<std::unique_ptr<RaftGroupClient>> direct;
    for (int s = 0; s < kNumShards; ++s) {
        direct.push_back(std::make_unique<RaftGroupClient>(c.shard_endpoints[s]));
    }
    int direct_failures = 0;
    for (int i = 0; i < kNumKeys && direct_failures < 3; ++i) {
        const std::string key = KeyName(i);
        const int owner = static_cast<int>(OwnerOf(c, key));
        const auto own = direct[owner]->Get(key);
        if (own.status != RaftGroupClient::Status::kOk || own.value != ValueFor(i)) {
            ++direct_failures;
            std::fprintf(stderr, "direct owner Get %s: status %d (%s)\n", key.c_str(),
                         static_cast<int>(own.status), own.detail.c_str());
        }
        if (i % 5 == 0) {  // sample: the foreign path is slower to fail loudly
            const int other = (owner + 1) % kNumShards;
            const auto foreign = direct[other]->Get(key);
            if (foreign.status != RaftGroupClient::Status::kWrongShard ||
                foreign.map_version != kMapVersion) {
                ++direct_failures;
                std::fprintf(stderr, "foreign Get %s: status %d version %llu (%s)\n", key.c_str(),
                             static_cast<int>(foreign.status),
                             static_cast<unsigned long long>(foreign.map_version),
                             foreign.detail.c_str());
            }
        }
    }
    CHECK(direct_failures == 0);
    return g_failures == 0;
}

bool PhaseShardCrash(Cluster& c, ClientRouter& router, int shard) {
    const int leader = FindLeader(c.shard_endpoints[shard], std::chrono::seconds(10));
    CHECK(leader >= 0);
    if (leader < 0) {
        std::cerr << "shard " << shard << ": no node answered LEADER\n";
        return false;
    }
    std::cout << "      shard " << shard << ": SIGKILL its leader (node " << leader << ")\n";

    harness::Process& victim = *c.shards[shard][leader];
    victim.Kill();
    CHECK(!victim.Running());

    // Every key (both shards) still readable straight away: a NEW leader has
    // to serve them. New writes to the damaged shard succeed.
    int failures = 0;
    for (int i = 0; i < kNumKeys && failures < 3; ++i) {
        const auto r = router.Get(KeyName(i));
        if (r.status != RaftGroupClient::Status::kOk || r.value != ValueFor(i)) {
            ++failures;
            std::fprintf(stderr, "after killing shard %d leader: Get %s status %d value '%s' (%s)\n",
                         shard, KeyName(i).c_str(), static_cast<int>(r.status), r.value.c_str(),
                         r.detail.c_str());
            if (r.status == RaftGroupClient::Status::kNotFound) {
                std::fprintf(stderr,
                             "  ^ NOT_FOUND for a key that was committed before the crash. The "
                             "killed node WAS its shard's leader, so the NEW leader served a read "
                             "before applying committed entries (known flag #2: no read barrier / "
                             "leader no-op on election). That is a server-side finding, not a "
                             "harness bug.\n");
            }
        }
    }
    CHECK(failures == 0);

    int written = 0;
    for (int i = 0; written < 10 && i < 100000; ++i) {
        const std::string key = "after-crash-" + std::to_string(shard) + "-" + std::to_string(i);
        if (OwnerOf(c, key) != static_cast<ShardId>(shard)) continue;  // aim at the damaged shard
        CHECK_OK(router.Put(key, "v-" + key));
        const auto got = router.Get(key);
        CHECK(got.status == RaftGroupClient::Status::kOk && got.value == "v-" + key);
        ++written;
    }

    // Restart: same data dir, so it recovers its persisted Raft state.
    CHECK(victim.Start());
    const Endpoint restarted = c.shard_endpoints[shard][leader];
    CHECK(harness::WaitUntil([&] { return ReadyProbe(restarted); }, std::chrono::seconds(15)));
    CHECK(victim.Running());

    // The shard as a whole is still healthy afterwards.
    CHECK_OK(router.Put(KeyOwnedBy(c, static_cast<ShardId>(shard), "after-restart-"), "ok"));
    return g_failures == 0;
}

// The narrow window: an acknowledged write followed IMMEDIATELY by the death
// of the leader. A Put is acknowledged once a majority has the entry, but the
// followers only learn it is committed from the leader's next AppendEntries
// (up to one heartbeat later). If the leader dies in that gap, the new leader
// has the entry in its log yet may not have applied it. Reading the key right
// after must still return the value.
bool PhaseWriteThenKill(Cluster& c, ClientRouter& router, int shard) {
    for (int round = 0; round < kWriteKillRounds; ++round) {
        const int leader = FindLeader(c.shard_endpoints[shard], std::chrono::seconds(10));
        CHECK(leader >= 0);
        if (leader < 0) return false;

        const std::string key = KeyOwnedBy(
            c, static_cast<ShardId>(shard),
            "wtk-" + std::to_string(shard) + "-" + std::to_string(round) + "-");
        const std::string value = "v-" + key;

        CHECK_OK(router.Put(key, value));  // acknowledged
        harness::Process& victim = *c.shards[shard][leader];
        victim.Kill();                      // immediately: no sleep in between
        CHECK(!victim.Running());

        const auto got = router.Get(key);
        if (got.status != RaftGroupClient::Status::kOk || got.value != value) {
            std::fprintf(stderr,
                         "shard %d round %d: key %s was ACKNOWLEDGED, then its leader (node %d) was "
                         "killed at once; Get -> status %d value '%s' (%s)\n",
                         shard, round, key.c_str(), leader, static_cast<int>(got.status),
                         got.value.c_str(), got.detail.c_str());
            if (got.status == RaftGroupClient::Status::kNotFound ||
                (got.status == RaftGroupClient::Status::kOk && got.value != value)) {
                std::fprintf(stderr,
                             "  ^ a client that was told OK cannot read its own write after a "
                             "leader change: the new leader answered from state that had not yet "
                             "applied a committed entry (known flag #2: no read barrier / leader "
                             "no-op on election). Server-side finding, not a harness bug.\n");
            }
            ++g_failures;
        }

        CHECK(victim.Start());
        CHECK(harness::WaitUntil([&] { return ReadyProbe(c.shard_endpoints[shard][leader]); },
                                 std::chrono::seconds(15)));
        // Let the restarted node catch up before the next round kills another.
        std::this_thread::sleep_for(milliseconds(500));
        if (g_failures != 0) return false;
    }
    return true;
}

bool PhaseShardCrashes(Cluster& c, ClientRouter& router) {
    std::cout << "[3/5] shard crash: SIGKILL each shard's LEADER in turn, keep serving, restart\n";
    for (int shard = 0; shard < kNumShards; ++shard) {
        if (!PhaseShardCrash(c, router, shard)) return false;
        std::cout << "      shard " << shard << ": " << kWriteKillRounds
                  << " rounds of acknowledged write, immediate leader kill, read back\n";
        if (!PhaseWriteThenKill(c, router, shard)) return false;
    }
    return true;
}

bool PhaseMetadataCrash(Cluster& c, ClientRouter& old_router) {
    std::cout << "[4/5] metadata crash: SIGKILL the metadata LEADER, fresh router still works\n";

    const int meta_leader = FindLeader(c.meta_endpoints, std::chrono::seconds(10));
    CHECK(meta_leader >= 0);
    if (meta_leader < 0) return false;
    std::cout << "      metadata: SIGKILL its leader (node " << meta_leader << ")\n";
    c.meta[meta_leader]->Kill();
    CHECK(!c.meta[meta_leader]->Running());

    // The map is cached in the old router; a fresh one must fetch it from the
    // two surviving metadata nodes (2 of 3 = majority).
    RaftGroupClientOptions opts = QuickOptions();
    opts.overall_timeout_ms = 15000;  // allow for a leader election
    sharding::ClientRouterOptions router_opts;
    router_opts.metadata_options = opts;
    ClientRouter fresh(c.meta_endpoints, router_opts);

    const auto refreshed = fresh.Refresh();
    CHECK(refreshed.status == ClientRouter::RefreshStatus::kOk);
    CHECK(fresh.map_version() == kMapVersion);

    const auto got = fresh.Get(KeyName(0));
    CHECK(got.status == RaftGroupClient::Status::kOk && got.value == ValueFor(0));
    CHECK_OK(fresh.Put("after-meta-crash", "ok"));

    // The router created earlier is unaffected (it never needed metadata again).
    const auto got_old = old_router.Get(KeyName(1));
    CHECK(got_old.status == RaftGroupClient::Status::kOk && got_old.value == ValueFor(1));
    return true;
}

void PhaseShutdown(Cluster& c) {
    std::cout << "[5/5] shutdown: SIGTERM every remaining process, expect exit code 0\n";
    for (auto& p : c.meta) {
        if (!p->Running()) continue;  // the metadata leader was SIGKILLed in phase 4
        const int code = p->Terminate(5000);
        if (code != 0) std::fprintf(stderr, "%s: exit code %d (expected 0)\n", p->name().c_str(), code);
        CHECK(code == 0);
    }
    for (auto& shard : c.shards) {
        for (auto& p : shard) {
            if (!p->Running()) continue;
            const int code = p->Terminate(5000);
            if (code != 0) std::fprintf(stderr, "%s: exit code %d (expected 0)\n", p->name().c_str(), code);
            CHECK(code == 0);
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    Cluster c;
    c.bin_dir = argc > 1 ? argv[1] : fs::weakly_canonical(fs::path(argv[0])).parent_path().string();

    for (const char* name : {"kv_metadata_server", "kv_shard_server"}) {
        if (!fs::exists(fs::path(c.bin_dir) / name)) {
            std::cerr << "missing " << c.bin_dir << "/" << name
                      << " (build it, or pass the build directory as argv[1])\n";
            return 2;
        }
    }
    if (!PreflightPorts()) return 2;
    if (!BuildTopology(&c)) return 2;

    c.work_dir = (fs::temp_directory_path() /
                  ("kv_multi_shard_test_" + std::to_string(::getpid())))
                     .string();
    std::error_code ec;
    fs::remove_all(c.work_dir, ec);
    fs::create_directories(c.work_dir + "/logs", ec);
    if (ec) {
        std::cerr << "cannot create work dir " << c.work_dir << ": " << ec.message() << "\n";
        return 2;
    }
    std::cout << "work dir (logs kept on failure): " << c.work_dir << "\n";

    SpawnProcesses(&c);

    bool ok = PhaseBoot(c);
    if (ok) {
        ClientRouter router(c.meta_endpoints);
        ok = PhaseRouting(c, router) && PhaseShardCrashes(c, router) && PhaseMetadataCrash(c, router);
    }
    PhaseShutdown(c);  // always: also the cleanup path when a phase bailed out

    if (g_failures == 0) {
        fs::remove_all(c.work_dir, ec);
        std::cout << "multi_shard_test: all checks passed\n";
        return 0;
    }
    std::cerr << "multi_shard_test: " << g_failures << " check(s) failed; logs in " << c.work_dir
              << "/logs\n";
    return 1;
}