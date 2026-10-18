#include "raft/client_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstring>
#include <sstream>

namespace raft {
namespace {

bool write_all(int fd, const char* data, std::size_t len) {
    std::size_t sent = 0;
    while (sent < len) {
        const ssize_t n = ::write(fd, data + sent, len - sent);
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

}  // namespace

ClientServer::ClientServer(int port, RaftNode* node,
                           std::map<NodeId, PeerAddress> client_peer_addresses,
                           Duration propose_timeout, std::size_t thread_pool_size)
    : port_(port),
      node_(node),
      client_peer_addresses_(std::move(client_peer_addresses)),
      propose_timeout_(propose_timeout),
      pool_(thread_pool_size) {}

ClientServer::~ClientServer() { Stop(); }

bool ClientServer::Start() {
    // See Step 8a design notes §4 — writing to a socket whose peer just
    // closed can raise SIGPIPE, whose default disposition would kill
    // this whole process. Safe to call repeatedly.
    std::signal(SIGPIPE, SIG_IGN);

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    if (::listen(listen_fd_, 16) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread(&ClientServer::AcceptLoop, this);
    return true;
}

void ClientServer::Stop() {
    if (!running_.exchange(false)) return;

    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (accept_thread_.joinable()) accept_thread_.join();

    {
        std::lock_guard<std::mutex> lock(inbound_mutex_);
        for (int fd : inbound_fds_) {
            ::shutdown(fd, SHUT_RDWR);
        }
    }
    // pool_'s destructor (run via this object's own destructor, after
    // Stop() returns) joins every worker — see design notes §3.2.
}

void ClientServer::AcceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        const int client_fd =
            ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_) break;
            continue;
        }
        pool_.Enqueue([this, client_fd] { HandleClient(client_fd); });
    }
}

void ClientServer::HandleClient(int fd) {
    {
        std::lock_guard<std::mutex> lock(inbound_mutex_);
        inbound_fds_.push_back(fd);
    }

    std::string buffer;
    char chunk[4096];
    bool connection_ok = true;

    while (connection_ok && running_) {
        const ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n <= 0) break;
        buffer.append(chunk, static_cast<std::size_t>(n));

        std::size_t newline;
        while ((newline = buffer.find('\n')) != std::string::npos) {
            std::string line = buffer.substr(0, newline);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            buffer.erase(0, newline + 1);

            const std::string response = HandleLine(line);
            if (!write_all(fd, response.data(), response.size())) {
                connection_ok = false;
                break;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(inbound_mutex_);
        inbound_fds_.erase(std::remove(inbound_fds_.begin(), inbound_fds_.end(), fd),
                           inbound_fds_.end());
    }
    ::close(fd);
}

std::string ClientServer::FormatProposeOutcome(const RaftNode::ProposeWaitResult& result) {
    if (result.committed) return "OK\n";

    if (!result.durable.persisted) {
        return "ERROR local persistence failure\n";
    }
        if (!result.durable.result.accepted) {
        return NotLeaderResponse();
    }
    return "TIMEOUT\n";
}

std::string ClientServer::NotLeaderResponse() {
    // Return a "NOT_LEADER" response based on the client_peer_addresses_ map.
    // If a leader is known and has a client-facing address, include that.
    // Otherwise, return just "NOT_LEADER".
    const auto hint = node_->KnownLeaderHint();
    if (hint.leader_id.has_value()) {
        const auto it = client_peer_addresses_.find(*hint.leader_id);
        if (it != client_peer_addresses_.end()) {
            return "NOT_LEADER " + it->second.host + ":" +
                   std::to_string(it->second.port) + "\n";
        }
    }
    return "NOT_LEADER\n";
}
void ClientServer::SetKeyGuard(KeyGuard guard) {
    std::shared_ptr<const KeyGuard> shared;
    if (guard) shared = std::make_shared<const KeyGuard>(std::move(guard));
    std::lock_guard<std::mutex> lock(guard_mutex_);
    key_guard_ = std::move(shared);
}

std::string ClientServer::RejectIfNotOwned(const std::string& key) {
    // Copy the pointer under the lock, call the guard outside it: the guard
    // may be slow-ish (a ring lookup) and must not serialize all requests.
    std::shared_ptr<const KeyGuard> guard;
    {
        std::lock_guard<std::mutex> lock(guard_mutex_);
        guard = key_guard_;
    }
    if (!guard) return "";
    return (*guard)(key);
}

std::string ClientServer::HandleLine(const std::string& line) {
    std::istringstream iss(line);
    std::string verb;
    iss >> verb;
    // Leadership probe for clients that need to read from the leader
    // (RaftGroupClient::Get). Same redirect answer as a rejected write.
    if (verb == "LEADER") {
        if (node_->CurrentRole() == Role::kLeader) return "LEADER\n";
        return NotLeaderResponse();
    }
    
    if (verb == "GET") {
        std::string key;
        iss >> key;
        if (key.empty()) return "ERROR missing key\n";
        if (const std::string rejection = RejectIfNotOwned(key); !rejection.empty()) {
            return rejection;
        }

        // Flag #2 fix: the leader serves reads only after its election no-op
        // has committed and applied (so a new leader cannot answer from state
        // that is behind entries it already acknowledged). A node that is not
        // the leader keeps the previous behaviour (local read); RaftGroupClient
        // only ever asks the node that answered LEADER.
        const RaftNode::ReadResult read = node_->GetFromLeader(key, propose_timeout_);
        switch (read.status) {
            case RaftNode::ReadStatus::kFound:
                return "VALUE " + read.value + "\n";
            case RaftNode::ReadStatus::kNotFound:
                return "NOT_FOUND\n";
            case RaftNode::ReadStatus::kNotReady:
                return "TIMEOUT\n";  // leader, but its barrier did not clear in time
            case RaftNode::ReadStatus::kNotLeader:
                break;  // fall through to the local read below
        }
        std::string value;
        if (node_->Get(key, &value)) {
            return "VALUE " + value + "\n";
        }
        return "NOT_FOUND\n";
    }

    if (verb == "PUT") {
        ClientId client_id = 0;
        SequenceNum sequence_num = 0;
        std::string key;
        if (!(iss >> client_id >> sequence_num >> key)) {
            return "ERROR malformed PUT\n";
        }
        if (const std::string rejection = RejectIfNotOwned(key); !rejection.empty()) {
            return rejection;
        }
        std::string value;
        std::getline(iss, value);
        if (!value.empty() && value.front() == ' ') value.erase(0, 1);

        const auto result =
            node_->ProposePutAndWait(client_id, sequence_num, key, value, propose_timeout_);
        return FormatProposeOutcome(result);
    }

    if (verb == "DELETE") {
        ClientId client_id = 0;
        SequenceNum sequence_num = 0;
        std::string key;
        if (!(iss >> client_id >> sequence_num >> key)) {
            return "ERROR malformed DELETE\n";
        }
        if (const std::string rejection = RejectIfNotOwned(key); !rejection.empty()) {
            return rejection;
        }

        const auto result =
            node_->ProposeDeleteAndWait(client_id, sequence_num, key, propose_timeout_);
        return FormatProposeOutcome(result);
    }

    return "ERROR unknown command\n";
}

}  // namespace raft