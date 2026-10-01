#include "raft/persistence.h"

#include <cstdio>
#include <fstream>
#include <vector>

namespace {

int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            ++g_failures;                                                    \
        }                                                                    \
    } while (0)

void remove_if_exists(const std::string& path) { std::remove(path.c_str()); }

// ---------------------------------------------------------------------------
// TermVoteStore
// ---------------------------------------------------------------------------

void test_term_vote_missing_file_returns_nullopt() {
    const std::string path = "persistence_test_state_missing.bin";
    remove_if_exists(path);

    raft::TermVoteStore store(path);
    CHECK(!store.Load().has_value());
}

void test_term_vote_round_trip_with_vote() {
    const std::string path = "persistence_test_state_1.bin";
    remove_if_exists(path);

    raft::TermVoteStore store(path);
    raft::PersistedTermVote saved;
    saved.current_term = 7;
    saved.voted_for = 3;
    CHECK(store.Save(saved));

    const auto loaded = store.Load();
    CHECK(loaded.has_value());
    CHECK(loaded->current_term == 7);
    CHECK(loaded->voted_for.has_value() && *loaded->voted_for == 3);

    remove_if_exists(path);
}

void test_term_vote_round_trip_no_vote() {
    const std::string path = "persistence_test_state_2.bin";
    remove_if_exists(path);

    raft::TermVoteStore store(path);
    raft::PersistedTermVote saved;
    saved.current_term = 4;
    saved.voted_for = std::nullopt;
    CHECK(store.Save(saved));

    const auto loaded = store.Load();
    CHECK(loaded.has_value());
    CHECK(loaded->current_term == 4);
    CHECK(!loaded->voted_for.has_value());

    remove_if_exists(path);
}

void test_term_vote_overwrite_replaces_previous_value() {
    const std::string path = "persistence_test_state_3.bin";
    remove_if_exists(path);

    raft::TermVoteStore store(path);
    CHECK(store.Save({5, 1}));
    CHECK(store.Save({9, std::nullopt}));  // overwrite, not append

    const auto loaded = store.Load();
    CHECK(loaded.has_value());
    CHECK(loaded->current_term == 9);      // the SECOND save, not the first
    CHECK(!loaded->voted_for.has_value());

    remove_if_exists(path);
}

void test_term_vote_no_stale_temp_file_left_behind() {
    const std::string path = "persistence_test_state_4.bin";
    const std::string tmp_path = path + ".tmp";
    remove_if_exists(path);
    remove_if_exists(tmp_path);

    raft::TermVoteStore store(path);
    CHECK(store.Save({1, std::nullopt}));

    std::ifstream tmp_check(tmp_path);
    CHECK(!tmp_check.is_open());  // rename() should have consumed the temp file

    remove_if_exists(path);
}

// ---------------------------------------------------------------------------
// PersistedLogStore
// ---------------------------------------------------------------------------

void test_log_store_replay_on_missing_file_returns_zero() {
    const std::string path = "persistence_test_log_missing.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    const std::size_t count = store.Replay([](raft::Term, const std::string&) {});
    CHECK(count == 0);
}

void test_log_store_round_trip_multiple_entries() {
    const std::string path = "persistence_test_log_1.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    CHECK(store.Append(1, "PUT a 1"));
    CHECK(store.Append(1, "PUT b 2"));
    CHECK(store.Append(2, "DEL a"));

    std::vector<std::pair<raft::Term, std::string>> replayed;
    const std::size_t count = store.Replay(
        [&](raft::Term t, const std::string& c) { replayed.emplace_back(t, c); });

    CHECK(count == 3);
    CHECK(replayed.size() == 3);
    CHECK(replayed[0].first == 1 && replayed[0].second == "PUT a 1");
    CHECK(replayed[1].first == 1 && replayed[1].second == "PUT b 2");
    CHECK(replayed[2].first == 2 && replayed[2].second == "DEL a");

    remove_if_exists(path);
}

void test_log_store_binary_and_empty_commands_round_trip() {
    const std::string path = "persistence_test_log_2.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    CHECK(store.Append(1, std::string("a\0b", 3)));  // embedded NUL
    CHECK(store.Append(1, ""));                        // empty command

    std::vector<std::string> replayed;
    store.Replay([&](raft::Term, const std::string& c) { replayed.push_back(c); });

    CHECK(replayed.size() == 2);
    CHECK(replayed[0].size() == 3);
    CHECK(replayed[1].empty());

    remove_if_exists(path);
}

// Simulates a crash mid-write of a third record: two full, valid entries,
// then a handful of raw bytes representing an incomplete header. Replay
// must recover exactly the two complete entries and stop — not error out
// (design notes §2.3).
void test_log_store_replay_stops_at_truncated_tail() {
    const std::string path = "persistence_test_log_3.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    CHECK(store.Append(1, "cmd1"));
    CHECK(store.Append(1, "cmd2"));

    {
        std::ofstream raw(path, std::ios::binary | std::ios::app);
        // 5 raw bytes: less than a full checksum+term header (12 bytes),
        // simulating a write cut off partway through record 3.
        const char garbage[5] = {1, 2, 3, 4, 5};
        raw.write(garbage, sizeof(garbage));
    }

    std::vector<std::string> replayed;
    const std::size_t count =
        store.Replay([&](raft::Term, const std::string& c) { replayed.push_back(c); });

    CHECK(count == 2);
    CHECK(replayed.size() == 2);
    CHECK(replayed[0] == "cmd1");
    CHECK(replayed[1] == "cmd2");

    remove_if_exists(path);
}

// Corrupts record 2's command bytes directly on disk (not via Append) —
// record 1 must still replay successfully; replay must stop AT record 2,
// not skip past it and resync on record 3. Proves the checksum is
// actually load-bearing, not just written and never checked.
void test_log_store_replay_stops_at_corrupted_record() {
    const std::string path = "persistence_test_log_4.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    CHECK(store.Append(1, "cmd1"));
    CHECK(store.Append(1, "cmd2"));
    CHECK(store.Append(1, "cmd3"));

    // Record 1 is exactly 4(checksum) + 8(term) + 4(len) + 4("cmd1") = 20
    // bytes. Flip one byte inside record 2's command area (offset 20 +
    // 12 = 32, i.e. the first byte of "cmd2"'s text).
    {
        std::fstream raw(path, std::ios::binary | std::ios::in | std::ios::out);
        raw.seekp(32);
        raw.put(static_cast<char>(0xFF));
    }

    std::vector<std::string> replayed;
    const std::size_t count =
        store.Replay([&](raft::Term, const std::string& c) { replayed.push_back(c); });

    CHECK(count == 1);              // only record 1 survives
    CHECK(replayed.size() == 1);
    CHECK(replayed[0] == "cmd1");   // record 3's valid bytes are never
                                    // reached — replay stops, doesn't resync

    remove_if_exists(path);
}
void test_log_store_rewrite_replaces_contents() {
    const std::string path = "persistence_test_log_rewrite.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    store.Append(1, "old1");
    store.Append(1, "old2");
    store.Append(1, "old3");

    raft::Log replacement;
    replacement.append(2, "new1");
    replacement.append(2, "new2");

    CHECK(store.Rewrite(replacement));

    std::vector<std::string> replayed;
    const std::size_t count =
        store.Replay([&](raft::Term, const std::string& c) { replayed.push_back(c); });

    CHECK(count == 2);  // old3 entry is gone, not just the diff
    CHECK(replayed.size() == 2);
    CHECK(replayed[0] == "new1");
    CHECK(replayed[1] == "new2");

    remove_if_exists(path);
}

void test_log_store_rewrite_of_empty_log_empties_file() {
    const std::string path = "persistence_test_log_rewrite_empty.bin";
    remove_if_exists(path);

    raft::PersistedLogStore store(path);
    store.Append(1, "will_be_removed");

    raft::Log empty_log;
    CHECK(store.Rewrite(empty_log));

    const std::size_t count = store.Replay([](raft::Term, const std::string&) {});
    CHECK(count == 0);

    remove_if_exists(path);
}

}  // namespace

int main() {
    test_term_vote_missing_file_returns_nullopt();
    test_term_vote_round_trip_with_vote();
    test_term_vote_round_trip_no_vote();
    test_term_vote_overwrite_replaces_previous_value();
    test_term_vote_no_stale_temp_file_left_behind();

    test_log_store_replay_on_missing_file_returns_zero();
    test_log_store_round_trip_multiple_entries();
    test_log_store_binary_and_empty_commands_round_trip();
    test_log_store_replay_stops_at_truncated_tail();
    test_log_store_replay_stops_at_corrupted_record();
    test_log_store_rewrite_replaces_contents();
    test_log_store_rewrite_of_empty_log_empties_file();

    if (g_failures == 0) {
        std::printf("persistence_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "persistence_test: %d check(s) failed\n", g_failures);
    return 1;
}