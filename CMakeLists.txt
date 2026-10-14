cmake_minimum_required(VERSION 3.16)
project(distributed_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

find_package(Threads REQUIRED)
enable_testing()

add_library(storage
src/storage/skiplist.cpp
src/storage/wal.cpp
src/storage/memtable.cpp
src/storage/bloom_filter.cpp
src/storage/sstable.cpp
src/storage/compaction.cpp
src/storage/kv_engine.cpp
)
target_include_directories(storage PUBLIC src)
target_link_libraries(storage PUBLIC Threads::Threads)

add_library(net
src/net/thread_pool.cpp
src/net/tcp_server.cpp
src/net/line_client.cpp

)
target_include_directories(net PUBLIC src)
target_link_libraries(net PUBLIC storage Threads::Threads)

add_library(raft
src/raft/rpc.cpp
src/raft/log.cpp
src/raft/state_machine.cpp
src/raft/fake_cluster.cpp
src/raft/election_timer.cpp
src/raft/persistence.cpp
src/raft/persistent_node.cpp
src/raft/command.cpp
src/raft/raft_transport.cpp
src/raft/raft_node.cpp
src/raft/client_server.cpp
)
target_include_directories(raft PUBLIC src)
target_link_libraries(raft PUBLIC net storage Threads::Threads)

# --- storage unit tests ---
add_executable(test_skiplist test/unit/storage/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE storage)
add_test(NAME test_skiplist COMMAND test_skiplist)

add_executable(test_wal test/unit/storage/test_wal.cpp)
target_link_libraries(test_wal PRIVATE storage)
add_test(NAME test_wal COMMAND test_wal)
add_executable(test_memtable test/unit/storage/test_memtable.cpp)
target_link_libraries(test_memtable PRIVATE storage)
add_test(NAME test_memtable COMMAND test_memtable)

add_executable(test_bloom_filter test/unit/storage/test_bloom_filter.cpp)
target_link_libraries(test_bloom_filter PRIVATE storage)
add_test(NAME test_bloom_filter COMMAND test_bloom_filter)

add_executable(test_sstable test/unit/storage/test_sstable.cpp)
target_link_libraries(test_sstable PRIVATE storage)
add_test(NAME test_sstable COMMAND test_sstable)

add_executable(test_compaction test/unit/storage/test_compaction.cpp)
target_link_libraries(test_compaction PRIVATE storage)
add_test(NAME test_compaction COMMAND test_compaction)

# --- raft unit tests ---
add_executable(rpc_test test/unit/raft/rpc_test.cpp)
target_link_libraries(rpc_test PRIVATE raft)
add_test(NAME rpc_test COMMAND rpc_test)

add_executable(test_state_machine test/unit/raft/test_state_machine.cpp)
target_link_libraries(test_state_machine PRIVATE raft)
add_test(NAME test_state_machine COMMAND test_state_machine)

add_executable(fake_cluster_test test/unit/raft/fake_cluster_test.cpp)
target_link_libraries(fake_cluster_test PRIVATE raft)
add_test(NAME fake_cluster_test COMMAND fake_cluster_test)

add_executable(election_timer_test test/unit/raft/election_timer_test.cpp)
target_link_libraries(election_timer_test PRIVATE raft)
add_test(NAME election_timer_test COMMAND election_timer_test)

add_executable(log_test test/unit/raft/log_test.cpp)
target_link_libraries(log_test PRIVATE raft)
add_test(NAME log_test COMMAND log_test)

add_executable(replication_test test/unit/raft/replication_test.cpp)
target_link_libraries(replication_test PRIVATE raft)
add_test(NAME replication_test COMMAND replication_test)

add_executable(persistence_test test/unit/raft/persistence_test.cpp)
target_link_libraries(persistence_test PRIVATE raft)
add_test(NAME persistence_test COMMAND persistence_test)

add_executable(persistent_node_test test/unit/raft/persistent_node_test.cpp)
target_link_libraries(persistent_node_test PRIVATE raft)
add_test(NAME persistent_node_test COMMAND persistent_node_test)

add_executable(backtrack_test test/unit/raft/backtrack_test.cpp)
target_link_libraries(backtrack_test PRIVATE raft)
add_test(NAME backtrack_test COMMAND backtrack_test)

add_executable(commit_test test/unit/raft/commit_test.cpp)
target_link_libraries(commit_test PRIVATE raft)
add_test(NAME commit_test COMMAND commit_test)

add_executable(command_test test/unit/raft/command_test.cpp)
target_link_libraries(command_test PRIVATE raft)
add_test(NAME command_test COMMAND command_test)

add_executable(apply_test test/unit/raft/apply_test.cpp)
target_link_libraries(apply_test PRIVATE raft)
add_test(NAME apply_test COMMAND apply_test)

add_executable(raft_transport_test test/unit/raft/raft_transport_test.cpp)
target_link_libraries(raft_transport_test PRIVATE raft)
add_test(NAME raft_transport_test COMMAND raft_transport_test)

# --- integration tests (real sockets, real disk) ---
add_executable(test_tcp_server test/integration/tcp_server_test.cpp)
target_link_libraries(test_tcp_server PRIVATE net storage)
add_test(NAME test_tcp_server COMMAND test_tcp_server)

add_executable(raft_node_cluster_test test/integration/raft_node_cluster_test.cpp)
target_link_libraries(raft_node_cluster_test PRIVATE raft)
add_test(NAME raft_node_cluster_test COMMAND raft_node_cluster_test)

add_executable(client_server_test test/integration/client_server_test.cpp)
target_link_libraries(client_server_test PRIVATE raft)
add_test(NAME client_server_test COMMAND client_server_test)

# --- server binary + manual CLI client ---
add_executable(kv_server src/server_main.cpp)
target_link_libraries(kv_server PRIVATE net storage)

add_executable(cli tools/cli.cpp)

# ---- Phase 3: sharding ----
add_library(sharding
  src/sharding/hash_ring.cpp
  src/sharding/shard_map.cpp
  src/sharding/raft_group_client.cpp
  src/sharding/metadata_client.cpp
  src/sharding/client_router.cpp
  src/sharding/topology_text.cpp)
target_include_directories(sharding PUBLIC src)
target_link_libraries(sharding PUBLIC net Threads::Threads)

add_executable(test_hash_ring test/unit/sharding/test_hash_ring.cpp)
target_link_libraries(test_hash_ring sharding)

add_executable(test_shard_map test/unit/sharding/test_shard_map.cpp)
target_link_libraries(test_shard_map sharding)

add_executable(test_raft_group_client test/unit/sharding/test_raft_group_client.cpp)
target_link_libraries(test_raft_group_client sharding)
add_test(NAME test_raft_group_client COMMAND test_raft_group_client)

add_executable(test_metadata_client test/unit/sharding/test_metadata_client.cpp)
target_link_libraries(test_metadata_client sharding)
add_test(NAME test_metadata_client COMMAND test_metadata_client)

add_executable(test_client_router test/unit/sharding/test_client_router.cpp)
target_link_libraries(test_client_router sharding)
add_test(NAME test_client_router COMMAND test_client_router)

add_executable(test_topology_text test/unit/sharding/test_topology_text.cpp)
target_link_libraries(test_topology_text sharding)
add_test(NAME test_topology_text COMMAND test_topology_text)

add_executable(metadata_group_test test/integration/metadata_group_test.cpp)
target_link_libraries(metadata_group_test PRIVATE raft sharding)
add_test(NAME metadata_group_test COMMAND metadata_group_test)

# ---- Phase 3 Step 4: shard node ----
add_library(shard_node src/sharding/shard_node.cpp)
target_include_directories(shard_node PUBLIC src)
target_link_libraries(shard_node PUBLIC raft sharding)

add_executable(kv_shard_server src/shard_server_main.cpp)
target_link_libraries(kv_shard_server PRIVATE shard_node)

add_executable(shard_node_test test/integration/shard_node_test.cpp)
target_link_libraries(shard_node_test PRIVATE shard_node)
add_test(NAME shard_node_test COMMAND shard_node_test)

# ---- Phase 3 Step 6: process tooling ----
add_executable(kv_metadata_server src/metadata_server_main.cpp)
target_link_libraries(kv_metadata_server PRIVATE raft sharding)

add_executable(kv_admin tools/kv_admin.cpp)
target_link_libraries(kv_admin PRIVATE sharding)

# ---- Phase 3 Step 7: 12-process integration test (real binaries) ----
add_executable(multi_shard_test test/integration/multi_shard_test.cpp)
target_link_libraries(multi_shard_test PRIVATE sharding)
add_dependencies(multi_shard_test kv_metadata_server kv_shard_server)
add_test(NAME multi_shard_test COMMAND multi_shard_test)

add_executable(process_harness_test test/integration/process_harness_test.cpp)
add_test(NAME process_harness_test COMMAND process_harness_test)