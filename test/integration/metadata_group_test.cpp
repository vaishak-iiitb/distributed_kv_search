// The metadata group is an ORDINARY 3-node Raft group running the ordinary
// ClientServer; MetadataClient talks to it exactly as it would to any shard.
// These tests prove that claim with real Raft, real sockets, real disk.

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "raft/client_server.h"
#include "raft/raft_node.h"
#include "sharding/metadata_client.h"

namespace {

int g_failures = 0;

#define CHECK(cond)                                                                    \
    do {                                                                               \
        if (!(cond)) {                                                                 \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                              \
        }                                                                              \
    } while (0)

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout,
               std::chrono::milliseconds poll = std::chrono::milliseconds(20)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(poll);
    }
    return predicate();
}

// A plausible encoded map; only its version matters to MetadataClient.
std::string FakeMap(std::uint64_t version) {
    return "SM1;v=" + std::to_string(version) + ";vn=64;s=0@127.0.0.1:1/127.0.0.1:2;ck=00";
}

// One 3-node Raft group, each node with its own ClientServer.
class Group {
public:
    Group(const std::string& prefix, int raft_base_port, int client_base_port)
        : ids_({0, 1, 2}),
          prefix_(prefix),
          raft_base_(raft_base_port),
          client_base_(client_base_port) {
        Clean();
        for (raft::NodeId id : ids_) {
            nodes_.push_back(std::make_unique<raft::RaftNode>(MakeConfig(id)));
            alive_.push_back(nodes_.back()->Start());

            std::map<raft::NodeId, raft::PeerAddress> client_peers;
            for (raft::NodeId peer : ids_) {
                if (peer == id) continue;
                client_peers[peer] =
                    raft::PeerAddress{"127.0.0.1", client_base_ + static_cast<int>(peer)};
            }
            servers_.push_back(std::make_unique<raft::ClientServer>(
                client_base_ + static_cast<int>(id), nodes_.back().get(), std::move(client_peers)));
            alive_.back() = alive_.back() && servers_.back()->Start();
        }
    }

    ~Group() {
        for (auto& s : servers_) s->Stop();
        for (auto& n : nodes_) n->Stop();
        Clean();
    }

    bool AllStarted() const {
        for (bool a : alive_) if (!a) return false;
        return true;
    }

    std::optional<std::size_t> Leader() {
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            if (alive_[i] && nodes_[i]->CurrentRole() == raft::Role::kLeader) return i;
        }
        return std::nullopt;
    }

    bool WaitForLeader() {
        return WaitUntil([&] { return Leader().has_value(); }, std::chrono::milliseconds(3000));
    }

    // "Crash" a node: its client port and its Raft participation both stop.
    void Kill(std::size_t i) {
        servers_[i]->Stop();
        nodes_[i]->Stop();
        alive_[i] = false;
    }

    sharding::Endpoint EndpointOf(std::size_t i) const {
        return sharding::Endpoint{"127.0.0.1", client_base_ + static_cast<int>(i)};
    }

    // All three endpoints, starting from `first` (to force a given contact order).
    std::vector<sharding::Endpoint> EndpointsStartingAt(std::size_t first) const {
        std::vector<sharding::Endpoint> out;
        for (std::size_t k = 0; k < ids_.size(); ++k) out.push_back(EndpointOf((first + k) % ids_.size()));
        return out;
    }

    raft::RaftNode& Node(std::size_t i) { return *nodes_[i]; }

private:
    raft::RaftNodeConfig MakeConfig(raft::NodeId id) const {
        raft::RaftNodeConfig cfg;
        cfg.self_id = id;
        cfg.listen_port = raft_base_ + static_cast<int>(id);
        for (raft::NodeId peer : ids_) {
            if (peer == id) continue;
            cfg.peers[peer] = raft::PeerAddress{"127.0.0.1", raft_base_ + static_cast<int>(peer)};
        }
        cfg.state_path = prefix_ + "_n" + std::to_string(id) + ".state";
        cfg.log_path = prefix_ + "_n" + std::to_string(id) + ".log";
        cfg.kv_data_dir = prefix_ + "_n" + std::to_string(id) + ".kv";
        cfg.election_timeout_min = raft::Duration(100);
        cfg.election_timeout_max = raft::Duration(200);
        cfg.heartbeat_interval = raft::Duration(30);
        cfg.election_timer_seed = 3000 + id;
        return cfg;
    }

    // Removes the .kv directory too: a leftover one from a previous run would
    // make "nothing has been published yet" false.
    void Clean() const {
        namespace fs = std::filesystem;
        std::error_code ec;
        for (raft::NodeId id : ids_) {
            const std::string base = prefix_ + "_n" + std::to_string(id);
            fs::remove(base + ".state", ec);
            fs::remove(base + ".log", ec);
            fs::remove_all(base + ".kv", ec);
        }
    }

    std::vector<raft::NodeId> ids_;
    std::string prefix_;
    int raft_base_;
    int client_base_;
    std::vector<bool> alive_;
    // Declared nodes-then-servers so servers are destroyed FIRST.
    std::vector<std::unique_ptr<raft::RaftNode>> nodes_;
    std::vector<std::unique_ptr<raft::ClientServer>> servers_;
};

using sharding::MetadataClient;

// ---------------------------------------------------------------------------

void test_publish_fetch_and_replication() {
    Group group("meta_test_basic", 19000, 19100);
    CHECK(group.AllStarted());
    CHECK(group.WaitForLeader());

    MetadataClient client(group.EndpointsStartingAt(0));

    CHECK(client.Fetch().status == MetadataClient::FetchStatus::kEmpty);

    CHECK(client.Publish(FakeMap(1)).status == MetadataClient::PublishStatus::kOk);
    const auto f1 = client.Fetch();
    CHECK(f1.status == MetadataClient::FetchStatus::kOk && f1.version == 1);
    CHECK(f1.encoded_map == FakeMap(1));

    CHECK(client.Publish(FakeMap(2)).status == MetadataClient::PublishStatus::kOk);

    // Version discipline holds through real Raft too.
    const auto stale = client.Publish(FakeMap(1));
    CHECK(stale.status == MetadataClient::PublishStatus::kStaleVersion);
    CHECK(stale.current_version == 2);
    CHECK(client.Publish(FakeMap(2)).status == MetadataClient::PublishStatus::kStaleVersion);

    // The published map is replicated: every node eventually holds it
    // locally (followers learn the commit on the next heartbeat).
    for (std::size_t i = 0; i < 3; ++i) {
        std::string value;
        CHECK(WaitUntil(
            [&] { return group.Node(i).Get(sharding::kShardMapKey, &value) && value == FakeMap(2); },
            std::chrono::milliseconds(3000)));
    }
}

void test_client_that_contacts_a_follower_first_still_succeeds() {
    Group group("meta_test_redirect", 19200, 19300);
    CHECK(group.AllStarted());
    CHECK(group.WaitForLeader());
    const auto leader = group.Leader();
    CHECK(leader.has_value());
    if (!leader) return;

    const std::size_t follower = (*leader + 1) % 3;
    MetadataClient client(group.EndpointsStartingAt(follower));  // follower is tried first

    CHECK(client.Publish(FakeMap(10)).status == MetadataClient::PublishStatus::kOk);
    const auto fetched = client.Fetch();
    CHECK(fetched.status == MetadataClient::FetchStatus::kOk && fetched.version == 10);
}

// The point of putting the shard map in a Raft group at all: it survives
// losing a node — including the leader — and stays writable.
void test_publish_survives_leader_loss() {
    Group group("meta_test_failover", 19400, 19500);
    CHECK(group.AllStarted());
    CHECK(group.WaitForLeader());
    const auto first_leader = group.Leader();
    CHECK(first_leader.has_value());
    if (!first_leader) return;

    MetadataClient client(group.EndpointsStartingAt(*first_leader));
    CHECK(client.Publish(FakeMap(1)).status == MetadataClient::PublishStatus::kOk);

    group.Kill(*first_leader);

    // No explicit wait for the election: the client's own retry loop must
    // absorb it (connection refused -> next endpoint -> NOT_LEADER -> ...).
    const auto start = std::chrono::steady_clock::now();
    CHECK(client.Publish(FakeMap(2)).status == MetadataClient::PublishStatus::kOk);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    CHECK(elapsed.count() < 8000);

    // A brand-new client whose FIRST contact is the dead node still works.
    MetadataClient fresh(group.EndpointsStartingAt(*first_leader));
    const auto fetched = fresh.Fetch();
    CHECK(fetched.status == MetadataClient::FetchStatus::kOk);
    CHECK(fetched.version == 2);
    CHECK(fetched.encoded_map == FakeMap(2));

    // Both survivors hold the latest map.
    for (std::size_t i = 0; i < 3; ++i) {
        if (i == *first_leader) continue;
        std::string value;
        CHECK(WaitUntil(
            [&] { return group.Node(i).Get(sharding::kShardMapKey, &value) && value == FakeMap(2); },
            std::chrono::milliseconds(3000)));
    }
}

}  // namespace

int main() {
    test_publish_fetch_and_replication();
    test_client_that_contacts_a_follower_first_still_succeeds();
    test_publish_survives_leader_loss();

    if (g_failures == 0) {
        std::printf("metadata_group_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "metadata_group_test: %d check(s) failed\n", g_failures);
    return 1;
}