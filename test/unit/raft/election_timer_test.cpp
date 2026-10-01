#include "raft/election_timer.h"

#include <algorithm>
#include <cstdio>
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

using raft::Duration;
using raft::TimePoint;

TimePoint epoch() { return TimePoint(); }

// ---------------------------------------------------------------------------
// Basic deadline placement.
// ---------------------------------------------------------------------------

void test_deadline_lands_within_configured_range() {
    raft::ElectionTimer timer(Duration(150), Duration(300), /*seed=*/1);
    for (int i = 0; i < 1000; ++i) {
        timer.reset(epoch());
        const auto elapsed = timer.deadline() - epoch();
        CHECK(elapsed >= Duration(150));
        CHECK(elapsed < Duration(300));  // exclusive upper bound
    }
}

void test_has_expired_boundary() {
    raft::ElectionTimer timer(Duration(150), Duration(151), /*seed=*/2);
    timer.reset(epoch());
    const TimePoint deadline = timer.deadline();

    CHECK(!timer.has_expired(deadline - Duration(1)));  // just before: no
    CHECK(timer.has_expired(deadline));                 // exactly at: yes
    CHECK(timer.has_expired(deadline + Duration(1)));   // after: yes
}

void test_reset_from_nonzero_now_offsets_correctly() {
    raft::ElectionTimer timer(Duration(150), Duration(300), /*seed=*/3);
    const TimePoint later = epoch() + Duration(500000);  // arbitrary offset
    timer.reset(later);
    const auto elapsed = timer.deadline() - later;
    CHECK(elapsed >= Duration(150));
    CHECK(elapsed < Duration(300));
}

// ---------------------------------------------------------------------------
// Randomization actually varies across resets — the property §2.2's design
// notes are built on.
// ---------------------------------------------------------------------------

void test_repeated_resets_produce_varying_deadlines() {
    raft::ElectionTimer timer(Duration(150), Duration(300), /*seed=*/4);
    std::vector<Duration> draws;
    for (int i = 0; i < 50; ++i) {
        timer.reset(epoch());
        draws.push_back(std::chrono::duration_cast<Duration>(timer.deadline() - epoch()));
    }
    const bool all_identical =
        std::all_of(draws.begin(), draws.end(),
                    [&](Duration d) { return d == draws.front(); });
    CHECK(!all_identical);  // near-impossible with a real RNG across 50 draws
}

// ---------------------------------------------------------------------------
// Statistical: does a good range make simultaneous-firing collisions rare
// across a simulated 3-node cluster, repeated over many independent runs?
// This is the direct, testable version of the §2.3 worked example.
// ---------------------------------------------------------------------------

void test_good_range_rarely_produces_near_simultaneous_deadlines() {
    constexpr int kTrials = 2000;
    constexpr Duration kCollisionWindow(5);  // "near-simultaneous" = within 5ms
    int collisions = 0;

    for (int trial = 0; trial < kTrials; ++trial) {
        std::vector<Duration> deadlines;
        for (std::uint64_t node = 0; node < 3; ++node) {
            raft::ElectionTimer timer(Duration(150), Duration(300),
                                      /*seed=*/1000 * trial + node);
            timer.reset(epoch());
            deadlines.push_back(
                std::chrono::duration_cast<Duration>(timer.deadline() - epoch()));
        }
        std::sort(deadlines.begin(), deadlines.end());
        if (deadlines[1] - deadlines[0] < kCollisionWindow) ++collisions;
    }

    // With a 150ms-wide range and a 5ms window, collisions should be rare —
    // well under 10% across 2000 trials. This threshold is deliberately
    // loose (a statistical smoke test, not a tight bound) to avoid a flaky
    // test while still catching a badly narrowed range (see the next test).
    CHECK(collisions < kTrials / 10);
}

// The mirror image: prove the failure mode is real by reproducing it with a
// deliberately bad (narrow) range, per §2.3 Config B.
void test_narrow_range_produces_frequent_near_simultaneous_deadlines() {
    constexpr int kTrials = 2000;
    constexpr Duration kCollisionWindow(1);  // 1ms — matches the 1ms-wide range
    int collisions = 0;

    for (int trial = 0; trial < kTrials; ++trial) {
        std::vector<Duration> deadlines;
        for (std::uint64_t node = 0; node < 3; ++node) {
            raft::ElectionTimer timer(Duration(150), Duration(151),
                                      /*seed=*/1000 * trial + node);
            timer.reset(epoch());
            deadlines.push_back(
                std::chrono::duration_cast<Duration>(timer.deadline() - epoch()));
        }
        std::sort(deadlines.begin(), deadlines.end());
        if (deadlines[1] - deadlines[0] <= kCollisionWindow) ++collisions;
    }

    // A 1ms-wide range with 3 nodes should collide (land within 1ms of each
    // other) FAR more often than the good-range case above — demonstrating
    // the livelock risk is real, not just asserted.
    CHECK(collisions > kTrials / 4);
}

// ---------------------------------------------------------------------------
// validate_timing_config
// ---------------------------------------------------------------------------

void test_validate_timing_config_accepts_design_doc_example() {
    CHECK(raft::validate_timing_config(Duration(50), Duration(150), Duration(300)));
}

void test_validate_timing_config_rejects_heartbeat_too_close_to_min() {
    // heartbeat 100ms, min 150ms: 100*2=200 > 150 -> rejected
    CHECK(!raft::validate_timing_config(Duration(100), Duration(150), Duration(300)));
}

void test_validate_timing_config_rejects_inverted_range() {
    CHECK(!raft::validate_timing_config(Duration(50), Duration(300), Duration(150)));
}

void test_validate_timing_config_rejects_equal_min_max() {
    CHECK(!raft::validate_timing_config(Duration(50), Duration(200), Duration(200)));
}

void test_validate_timing_config_accepts_exact_2x_margin() {
    // heartbeat 75ms, min 150ms: exactly 2x, boundary should pass (<=).
    CHECK(raft::validate_timing_config(Duration(75), Duration(150), Duration(300)));
}

}  // namespace

int main() {
    test_deadline_lands_within_configured_range();
    test_has_expired_boundary();
    test_reset_from_nonzero_now_offsets_correctly();

    test_repeated_resets_produce_varying_deadlines();

    test_good_range_rarely_produces_near_simultaneous_deadlines();
    test_narrow_range_produces_frequent_near_simultaneous_deadlines();

    test_validate_timing_config_accepts_design_doc_example();
    test_validate_timing_config_rejects_heartbeat_too_close_to_min();
    test_validate_timing_config_rejects_inverted_range();
    test_validate_timing_config_rejects_equal_min_max();
    test_validate_timing_config_accepts_exact_2x_margin();

    if (g_failures == 0) {
        std::printf("election_timer_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "election_timer_test: %d check(s) failed\n", g_failures);
    return 1;
}