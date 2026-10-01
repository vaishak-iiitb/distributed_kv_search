#include "raft/log.h"

#include <cstdio>

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

void test_empty_log() {
    raft::Log log;
    CHECK(log.size() == 0);
    CHECK(log.last_index() == 0);
    CHECK(log.last_term() == 0);
    CHECK(!log.at(0).has_value());
    CHECK(!log.at(1).has_value());
    CHECK(log.term_at(1) == 0);
}

void test_append_returns_sequential_one_based_indices() {
    raft::Log log;
    CHECK(log.append(1, "a") == 1);
    CHECK(log.append(1, "b") == 2);
    CHECK(log.append(2, "c") == 3);
    CHECK(log.last_index() == 3);
    CHECK(log.last_term() == 2);
}

void test_at_returns_correct_content_in_order() {
    raft::Log log;
    log.append(1, "first");
    log.append(1, "second");
    log.append(3, "third");

    const auto e1 = log.at(1);
    const auto e2 = log.at(2);
    const auto e3 = log.at(3);
    CHECK(e1.has_value() && e1->term == 1 && e1->command == "first");
    CHECK(e2.has_value() && e2->term == 1 && e2->command == "second");
    CHECK(e3.has_value() && e3->term == 3 && e3->command == "third");
}

void test_at_index_zero_is_always_nullopt() {
    raft::Log log;
    log.append(1, "x");
    CHECK(!log.at(0).has_value());
}

void test_at_out_of_range_is_nullopt() {
    raft::Log log;
    log.append(1, "x");
    CHECK(!log.at(2).has_value());
    CHECK(!log.at(1000).has_value());
}

void test_term_at_matches_at() {
    raft::Log log;
    log.append(5, "x");
    log.append(7, "y");
    CHECK(log.term_at(1) == 5);
    CHECK(log.term_at(2) == 7);
    CHECK(log.term_at(0) == 0);
    CHECK(log.term_at(3) == 0);
}

void test_binary_and_empty_commands_preserved() {
    raft::Log log;
    log.append(1, std::string("a\0b", 3));
    log.append(1, "");
    CHECK(log.at(1)->command.size() == 3);
    CHECK(log.at(2)->command.empty());
}

// --- Step 6: truncate_from ---------------------------------------------

void test_truncate_from_removes_suffix() {
    raft::Log log;
    log.append(1, "a");
    log.append(1, "b");
    log.append(1, "c");
    log.append(1, "d");

    log.truncate_from(3);  // remove entries 3, 4 — keep 1, 2

    CHECK(log.last_index() == 2);
    CHECK(log.at(1)->command == "a");
    CHECK(log.at(2)->command == "b");
    CHECK(!log.at(3).has_value());
    CHECK(!log.at(4).has_value());
}

void test_truncate_from_one_empties_the_log() {
    raft::Log log;
    log.append(1, "a");
    log.append(1, "b");

    log.truncate_from(1);

    CHECK(log.last_index() == 0);
    CHECK(log.last_term() == 0);
}

void test_truncate_from_zero_is_a_no_op() {
    raft::Log log;
    log.append(1, "a");
    log.append(1, "b");

    log.truncate_from(0);  // 0 is the sentinel, never a real index

    CHECK(log.last_index() == 2);  // unchanged
}

void test_truncate_from_beyond_end_is_a_no_op() {
    raft::Log log;
    log.append(1, "a");

    log.truncate_from(50);

    CHECK(log.last_index() == 1);  // unchanged
    CHECK(log.at(1)->command == "a");
}

// After truncation, appending new entries must resume at the correct
// (now-shorter) index — proves truncate_from actually shrinks the
// underlying storage, not just masks it.
void test_append_after_truncate_resumes_at_correct_index() {
    raft::Log log;
    log.append(1, "a");
    log.append(1, "b");
    log.append(1, "c");

    log.truncate_from(2);  // keep only "a"
    CHECK(log.last_index() == 1);

    const raft::LogIndex new_index = log.append(2, "replacement");
    CHECK(new_index == 2);  // reuses index 2, doesn't skip to 4
    CHECK(log.at(2)->term == 2);
    CHECK(log.at(2)->command == "replacement");
    CHECK(log.last_index() == 2);
}

}  // namespace

int main() {
    test_empty_log();
    test_append_returns_sequential_one_based_indices();
    test_at_returns_correct_content_in_order();
    test_at_index_zero_is_always_nullopt();
    test_at_out_of_range_is_nullopt();
    test_term_at_matches_at();
    test_binary_and_empty_commands_preserved();

    test_truncate_from_removes_suffix();
    test_truncate_from_one_empties_the_log();
    test_truncate_from_zero_is_a_no_op();
    test_truncate_from_beyond_end_is_a_no_op();
    test_append_after_truncate_resumes_at_correct_index();

    if (g_failures == 0) {
        std::printf("log_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "log_test: %d check(s) failed\n", g_failures);
    return 1;
}