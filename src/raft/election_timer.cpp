#include "raft/election_timer.h"

#include <cassert>

namespace raft {

ElectionTimer::ElectionTimer(Duration min, Duration max, std::uint64_t seed)
    : min_(min), max_(max), rng_(seed), deadline_(TimePoint::min()) {
    assert(min_ < max_ &&
           "ElectionTimer requires min < max: a degenerate range removes "
           "randomization and reintroduces the split-vote livelock "
           "(design doc §4.1)");
}

void ElectionTimer::reset(TimePoint now) {
    // Upper bound is exclusive: draws land in [min, max).
    std::uniform_int_distribution<Duration::rep> pick(min_.count(),
                                                       max_.count() - 1);
    deadline_ = now + Duration(pick(rng_));
}

bool ElectionTimer::has_expired(TimePoint now) const {
    return now >= deadline_;
}

bool validate_timing_config(Duration heartbeat_interval, Duration min_timeout,
                            Duration max_timeout) {
    if (min_timeout >= max_timeout) return false;
    // "Well below": require at least a 2x margin — see design notes §3.4
    // for why this specific factor and what it does and doesn't guarantee.
    return heartbeat_interval * 2 <= min_timeout;
}

}  // namespace raft