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
)
target_include_directories(net PUBLIC src)
target_link_libraries(net PUBLIC storage Threads::Threads)

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

# --- integration test (real sockets, real disk) ---
add_executable(test_tcp_server test/integration/tcp_server_test.cpp)
target_link_libraries(test_tcp_server PRIVATE net storage)
add_test(NAME test_tcp_server COMMAND test_tcp_server)

# --- server binary + manual CLI client ---
add_executable(kv_server src/server_main.cpp)
target_link_libraries(kv_server PRIVATE net storage)

add_executable(cli tools/cli.cpp)