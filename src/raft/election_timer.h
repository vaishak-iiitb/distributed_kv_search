#pragma once

#include <chrono>
#include <cstdint>
#include <random>

namespace raft {

using Clock = std::chrono::steady_clock;
using Duration = std::chrono::milliseconds;
using TimePoint = Clock::time_point;

// A randomized election timeout (design doc §4.1). Pure logic: never reads
// the real clock, never sleeps, never spawns a thread. The caller supplies
// "now" on every call — see design notes §2.1/§3.1 for why. A real driving
// loop (Step 8) will call reset()/has_expired() with Clock::now() inside an
// actual event loop.
class ElectionTimer {
public:
    // Requires min < max (checked with assert in debug builds — a
    // degenerate range removes randomization; see design notes §2.3/§3.4
    // for why that's a correctness bug, not a style nit).
    ElectionTimer(Duration min, Duration max, std::uint64_t seed);

    // Draws a NEW random deadline in [min, max) starting from `now`. Must
    // be called on startup and on every valid heartbeat/vote grant — a
    // reset that doesn't draw fresh randomness degenerates into a fixed
    // timeout after the first call (design notes §2.2).
    void reset(TimePoint now);

    // True iff `now` is at or past the current deadline.
    bool has_expired(TimePoint now) const;

    TimePoint deadline() const { return deadline_; }
    Duration min() const { return min_; }
    Duration max() const { return max_; }

private:
    Duration min_;
    Duration max_;
    std::mt19937_64 rng_;
    TimePoint deadline_;
};

// Checks a timing configuration against the rule design doc §4.1 calls out
// explicitly: the heartbeat interval must be well below the MINIMUM
// election timeout. Returns false rather than throwing/asserting, so the
// caller decides how to surface a bad config (see design notes §3.3).
bool validate_timing_config(Duration heartbeat_interval, Duration min_timeout,
                            Duration max_timeout);

}  // namespace raft