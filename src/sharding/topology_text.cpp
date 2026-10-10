#include "sharding/topology_text.h"

#include <map>
#include <set>
#include <sstream>

namespace sharding {
namespace {

bool ParseUnsigned(const std::string& text, std::uint64_t* out) {
    if (text.empty() || text.size() > 18) return false;
    std::uint64_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') return false;
        value = value * 10 + static_cast<std::uint64_t>(c - '0');
    }
    *out = value;
    return true;
}

std::string Trim(const std::string& s) {
    const char* ws = " \t\r\n";
    const std::size_t first = s.find_first_not_of(ws);
    if (first == std::string::npos) return "";
    const std::size_t last = s.find_last_not_of(ws);
    return s.substr(first, last - first + 1);
}

std::vector<std::string> SplitWhitespace(const std::string& s) {
    std::istringstream in(s);
    std::vector<std::string> tokens;
    std::string token;
    while (in >> token) tokens.push_back(token);
    return tokens;
}

constexpr std::uint64_t kMaxNodeIndex = 1023;
constexpr std::uint64_t kMaxVnodes = 100000;

}  // namespace

bool ParseClusterSpec(const std::string& text, std::vector<ClusterNode>* out, std::string* err) {
    out->clear();
    if (text.empty()) {
        *err = "empty cluster spec";
        return false;
    }

    std::set<std::uint32_t> seen;
    std::size_t start = 0;
    while (true) {
        std::size_t comma = text.find(',', start);
        if (comma == std::string::npos) comma = text.size();
        const std::string entry = text.substr(start, comma - start);

        const std::size_t eq = entry.find('=');
        const std::size_t slash = entry.find('/', eq == std::string::npos ? 0 : eq);
        if (entry.empty() || eq == std::string::npos || slash == std::string::npos) {
            *err = "bad cluster entry '" + entry + "' (expected id=raft_host:port/client_host:port)";
            return false;
        }

        ClusterNode node;
        std::uint64_t id = 0;
        if (!ParseUnsigned(entry.substr(0, eq), &id) || id > 0xFFFFFFFFull) {
            *err = "bad node id in cluster entry '" + entry + "'";
            return false;
        }
        node.id = static_cast<std::uint32_t>(id);
        if (!ParseEndpoint(entry.substr(eq + 1, slash - eq - 1), &node.raft)) {
            *err = "bad raft address in cluster entry '" + entry + "'";
            return false;
        }
        if (!ParseEndpoint(entry.substr(slash + 1), &node.client)) {
            *err = "bad client address in cluster entry '" + entry + "'";
            return false;
        }
        if (!seen.insert(node.id).second) {
            *err = "duplicate node id " + std::to_string(node.id) + " in cluster spec";
            return false;
        }
        out->push_back(node);

        if (comma == text.size()) break;
        start = comma + 1;
    }
    return true;
}

bool ParseMapFile(const std::string& text, ShardMap* out, std::string* err) {
    bool have_version = false, have_vnodes = false;
    std::uint64_t version = 0, vnodes = 128;
    std::map<ShardId, std::map<std::size_t, NodeAddr>> shards;

    std::istringstream in(text);
    std::string raw;
    int line_no = 0;
    while (std::getline(in, raw)) {
        ++line_no;
        const std::string where = "line " + std::to_string(line_no) + ": ";
        const std::size_t hash = raw.find('#');
        if (hash != std::string::npos) raw.resize(hash);
        const std::vector<std::string> tok = SplitWhitespace(Trim(raw));
        if (tok.empty()) continue;

        if (tok[0] == "version") {
            if (tok.size() != 2 || !ParseUnsigned(tok[1], &version) || version < 1) {
                *err = where + "expected 'version <n>' with n >= 1";
                return false;
            }
            if (have_version) {
                *err = where + "'version' given twice";
                return false;
            }
            have_version = true;
        } else if (tok[0] == "vnodes") {
            if (tok.size() != 2 || !ParseUnsigned(tok[1], &vnodes) || vnodes < 1 || vnodes > kMaxVnodes) {
                *err = where + "expected 'vnodes <n>' with 1 <= n <= " + std::to_string(kMaxVnodes);
                return false;
            }
            if (have_vnodes) {
                *err = where + "'vnodes' given twice";
                return false;
            }
            have_vnodes = true;
        } else if (tok.size() == 4) {
            std::uint64_t shard = 0, node = 0;
            if (!ParseUnsigned(tok[0], &shard) || shard > 0xFFFFFFFFull) {
                *err = where + "bad shard id '" + tok[0] + "'";
                return false;
            }
            if (!ParseUnsigned(tok[1], &node) || node > kMaxNodeIndex) {
                *err = where + "bad node index '" + tok[1] + "'";
                return false;
            }
            Endpoint raft_ep, client_ep;
            if (!ParseEndpoint(tok[2], &raft_ep)) {
                *err = where + "bad raft address '" + tok[2] + "' (expected host:port)";
                return false;
            }
            if (!ParseEndpoint(tok[3], &client_ep)) {
                *err = where + "bad client address '" + tok[3] + "' (expected host:port)";
                return false;
            }
            auto& nodes = shards[static_cast<ShardId>(shard)];
            if (!nodes.emplace(static_cast<std::size_t>(node), NodeAddr{tok[2], tok[3]}).second) {
                *err = where + "shard " + tok[0] + " node " + tok[1] + " listed twice";
                return false;
            }
        } else {
            *err = where + "expected 'version <n>', 'vnodes <n>' or "
                           "'<shard> <node> <raft_addr> <client_addr>'";
            return false;
        }
    }

    if (!have_version) {
        *err = "missing 'version <n>' line";
        return false;
    }
    if (shards.empty()) {
        *err = "no node lines: a map needs at least one shard";
        return false;
    }

    ShardMap map;
    map.version = version;
    map.vnodes = static_cast<std::uint32_t>(vnodes);
    for (const auto& [shard_id, nodes] : shards) {
        ShardInfo info;
        info.id = shard_id;
        std::size_t expected = 0;
        for (const auto& [index, addr] : nodes) {
            if (index != expected) {
                *err = "shard " + std::to_string(shard_id) + ": node indexes must be exactly 0.." +
                       std::to_string(nodes.size() - 1) + " (missing node " +
                       std::to_string(expected) + ")";
                return false;
            }
            info.nodes.push_back(addr);
            ++expected;
        }
        map.shards.push_back(std::move(info));
    }
    *out = std::move(map);
    return true;
}

std::string FormatMapFile(const ShardMap& map) {
    std::ostringstream out;
    out << "version " << map.version << "\n";
    out << "vnodes " << map.vnodes << "\n";
    out << "# shard node raft_addr client_addr\n";
    for (const ShardInfo& shard : map.shards) {
        for (std::size_t i = 0; i < shard.nodes.size(); ++i) {
            out << shard.id << " " << i << " " << shard.nodes[i].raft_addr << " "
                << shard.nodes[i].client_addr << "\n";
        }
    }
    return out.str();
}

}  // namespace sharding