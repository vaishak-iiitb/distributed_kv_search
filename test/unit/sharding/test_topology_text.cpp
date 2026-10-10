// Unit tests for the tooling text formats (cluster spec + map file).
// Pure parsing: no sockets, no Raft, no disk.

#include <cstdio>
#include <string>
#include <vector>

#include "sharding/topology_text.h"

using namespace sharding;

static int g_failures = 0;
#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                                 \
        }                                                                                 \
    } while (0)

namespace {

bool Contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

void TestClusterSpecOk() {
    std::vector<ClusterNode> nodes;
    std::string err;
    CHECK(ParseClusterSpec("0=127.0.0.1:7000/127.0.0.1:7100,1=h2:7001/h2:7101,2=h3:7002/h3:7102",
                           &nodes, &err));
    CHECK(nodes.size() == 3);
    CHECK(nodes[0].id == 0 && nodes[0].raft.port == 7000 && nodes[0].client.port == 7100);
    CHECK(nodes[1].raft.host == "h2" && nodes[1].client.host == "h2");
    CHECK(nodes[2].id == 2);

    CHECK(ParseClusterSpec("5=a:1/b:2", &nodes, &err));  // single node, non-zero id
    CHECK(nodes.size() == 1 && nodes[0].id == 5);
}

void TestClusterSpecErrors() {
    std::vector<ClusterNode> nodes;
    std::string err;
    CHECK(!ParseClusterSpec("", &nodes, &err));
    CHECK(!ParseClusterSpec("0=a:1/b:2,", &nodes, &err));           // trailing comma
    CHECK(!ParseClusterSpec("0=a:1/b:2,,1=c:3/d:4", &nodes, &err));  // empty entry
    CHECK(!ParseClusterSpec("0=a:1", &nodes, &err));                 // no client half
    CHECK(!ParseClusterSpec("a:1/b:2", &nodes, &err));               // no id
    CHECK(!ParseClusterSpec("x=a:1/b:2", &nodes, &err));             // non-numeric id
    CHECK(!ParseClusterSpec("0=a:0/b:2", &nodes, &err));             // port 0
    CHECK(!ParseClusterSpec("0=a:1/b:70000", &nodes, &err));         // port too big
    CHECK(!ParseClusterSpec("0=a/b:2", &nodes, &err));               // no port
    CHECK(!ParseClusterSpec("0=a:1/b:2,0=c:3/d:4", &nodes, &err));   // duplicate id
    CHECK(Contains(err, "duplicate node id 0"));
    CHECK(!ParseClusterSpec("99999999999=a:1/b:2", &nodes, &err));   // id overflow uint32
}

const char* kGoodMap =
    "# two shards\n"
    "version 4\n"
    "vnodes 64   # trailing comment\n"
    "\n"
    "1 0 h:7010 h:7110\n"
    "0 1 h:7001 h:7101\n"  // out of order on purpose
    "0 0 h:7000 h:7100\n"
    "1 1 h:7011 h:7111\r\n";  // CRLF line ending

void TestMapFileOk() {
    ShardMap map;
    std::string err;
    CHECK(ParseMapFile(kGoodMap, &map, &err));
    CHECK(map.version == 4 && map.vnodes == 64);
    CHECK(map.shards.size() == 2);
    CHECK(map.shards[0].id == 0 && map.shards[1].id == 1);  // ascending shard id
    CHECK(map.shards[0].nodes.size() == 2);
    CHECK(map.shards[0].nodes[0].raft_addr == "h:7000");     // ascending node index
    CHECK(map.shards[0].nodes[1].client_addr == "h:7101");
    CHECK(map.shards[1].nodes[1].client_addr == "h:7111");   // CR stripped
}

void TestMapFileDefaultsAndRoundTrip() {
    ShardMap map;
    std::string err;
    CHECK(ParseMapFile("version 1\n0 0 a:1 b:2\n", &map, &err));
    CHECK(map.vnodes == 128);  // default

    ShardMap original;
    CHECK(ParseMapFile(kGoodMap, &original, &err));
    ShardMap again;
    CHECK(ParseMapFile(FormatMapFile(original), &again, &err));
    CHECK(again.version == original.version && again.vnodes == original.vnodes);
    CHECK(again.shards.size() == original.shards.size());
    for (std::size_t s = 0; s < original.shards.size(); ++s) {
        CHECK(again.shards[s].id == original.shards[s].id);
        CHECK(again.shards[s].nodes.size() == original.shards[s].nodes.size());
        for (std::size_t n = 0; n < original.shards[s].nodes.size(); ++n) {
            CHECK(again.shards[s].nodes[n].raft_addr == original.shards[s].nodes[n].raft_addr);
            CHECK(again.shards[s].nodes[n].client_addr == original.shards[s].nodes[n].client_addr);
        }
    }
}

void ExpectMapError(const std::string& text, const std::string& needle) {
    ShardMap map;
    std::string err;
    const bool ok = ParseMapFile(text, &map, &err);
    if (ok || !Contains(err, needle)) {
        std::fprintf(stderr, "  wanted error containing '%s', got ok=%d err='%s'\n", needle.c_str(),
                     ok, err.c_str());
    }
    CHECK(!ok);
    CHECK(Contains(err, needle));
}

void TestMapFileErrors() {
    ExpectMapError("0 0 a:1 b:2\n", "missing 'version");
    ExpectMapError("version 1\n", "no node lines");
    ExpectMapError("version 0\n0 0 a:1 b:2\n", "version <n>");
    ExpectMapError("version x\n0 0 a:1 b:2\n", "version <n>");
    ExpectMapError("version 1\nversion 2\n0 0 a:1 b:2\n", "twice");
    ExpectMapError("version 1\nvnodes 0\n0 0 a:1 b:2\n", "vnodes");
    ExpectMapError("version 1\nvnodes 5\nvnodes 6\n0 0 a:1 b:2\n", "twice");
    ExpectMapError("version 1\n0 0 a:1\n", "line 2");                 // 3 tokens
    ExpectMapError("version 1\n0 0 a:1 b:2 extra\n", "line 2");      // 5 tokens
    ExpectMapError("version 1\nnonsense here\n", "line 2");          // 2 tokens, not a directive
    ExpectMapError("version 1\nz 0 a:1 b:2\n", "bad shard id");
    ExpectMapError("version 1\n0 z a:1 b:2\n", "bad node index");
    ExpectMapError("version 1\n0 5000 a:1 b:2\n", "bad node index");
    ExpectMapError("version 1\n0 0 nocolon b:2\n", "bad raft address");
    ExpectMapError("version 1\n0 0 a:1 b:99999\n", "bad client address");
    ExpectMapError("version 1\n0 0 a:1 b:2\n0 0 c:3 d:4\n", "listed twice");
    ExpectMapError("version 1\n0 0 a:1 b:2\n0 2 c:3 d:4\n", "missing node 1");  // gap
    ExpectMapError("version 1\n0 1 a:1 b:2\n", "missing node 0");               // starts at 1
    ExpectMapError("version 1\n# 0 0 a:1 b:2\n", "no node lines");              // commented out
}

}  // namespace

int main() {
    TestClusterSpecOk();
    TestClusterSpecErrors();
    TestMapFileOk();
    TestMapFileDefaultsAndRoundTrip();
    TestMapFileErrors();
    if (g_failures == 0) {
        std::printf("test_topology_text: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "test_topology_text: %d check(s) failed\n", g_failures);
    return 1;
}