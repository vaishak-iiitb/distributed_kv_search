#include "raft/state_machine.h"

#include <cstdio>
#include <cstdint>

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

constexpr raft::NodeId kIgnoredFrom = 0;  // unused by events that don't need it

// ---------------------------------------------------------------------------
// Step 2a coverage, re-verified through the public API (not duplicated in
// depth — see phase2-step2a's test file for the exhaustive term-rule matrix;
// these are smoke checks that the extended handle() still respects it).
// ---------------------------------------------------------------------------

void test_higher_term_still_steps_down_leader() {
    raft::RaftState s;
    s.role = raft::Role::kLeader;
    s.current_term = 5;
    s.voted_for = 1;

    raft::AppendEntriesRequest req;
    req.term = 6;
    req.leader_id = 2;

    raft::handle(s, kIgnoredFrom, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    CHECK(s.role == raft::Role::kFollower);
    CHECK(s.current_term == 6);
    CHECK(!s.voted_for.has_value());
}

// ---------------------------------------------------------------------------
// candidate_log_is_at_least_as_up_to_date — the election restriction itself.
// ---------------------------------------------------------------------------

void test_log_comparison_higher_term_wins_even_with_shorter_log() {
    // Candidate's last entry is term 5 index 2; ours is term 4 index 100.
    // Candidate should still be considered at least as up-to-date, because
    // term is compared first.
    CHECK(raft::candidate_log_is_at_least_as_up_to_date(5, 2, 4, 100));
}

void test_log_comparison_lower_term_loses_even_with_longer_log() {
    CHECK(!raft::candidate_log_is_at_least_as_up_to_date(3, 100, 4, 1));
}

void test_log_comparison_equal_term_compares_index() {
    CHECK(raft::candidate_log_is_at_least_as_up_to_date(4, 10, 4, 5));
    CHECK(!raft::candidate_log_is_at_least_as_up_to_date(4, 3, 4, 5));
}

void test_log_comparison_exact_tie_is_up_to_date() {
    // ">= " not ">": an exactly equal log must count as up-to-date.
    CHECK(raft::candidate_log_is_at_least_as_up_to_date(4, 5, 4, 5));
}

// ---------------------------------------------------------------------------
// ElectionTimeout
// ---------------------------------------------------------------------------

void test_election_timeout_from_follower_becomes_candidate() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kFollower;
    s.current_term = 3;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::ElectionTimeout{}, 3);

    CHECK(s.role == raft::Role::kCandidate);
    CHECK(s.current_term == 4);
    CHECK(s.voted_for.has_value() && *s.voted_for == 1);
    CHECK(s.votes_received.count(1) == 1);
    CHECK(s.votes_received.size() == 1);

    CHECK(r.messages.size() == 1);
    CHECK(r.messages[0].to == raft::kBroadcast);
    const auto* req = std::get_if<raft::RequestVoteRequest>(&r.messages[0].message);
    CHECK(req != nullptr);
    if (req) {
        CHECK(req->term == 4);
        CHECK(req->candidate_id == 1);
    }
}

void test_election_timeout_increments_by_exactly_one() {
    raft::RaftState s;
    s.current_term = 41;
    raft::handle(s, kIgnoredFrom, raft::ElectionTimeout{}, 3);
    CHECK(s.current_term == 42);
}

void test_election_timeout_ignored_by_leader() {
    raft::RaftState s;
    s.role = raft::Role::kLeader;
    s.current_term = 5;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::ElectionTimeout{}, 3);

    CHECK(s.role == raft::Role::kLeader);  // unchanged
    CHECK(s.current_term == 5);            // unchanged
    CHECK(r.messages.empty());
}

// ---------------------------------------------------------------------------
// RequestVoteRequest handling
// ---------------------------------------------------------------------------

void test_grants_vote_when_eligible() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kFollower;
    s.current_term = 5;

    raft::RequestVoteRequest req;
    req.term = 5;
    req.candidate_id = 2;
    req.last_log_term = 0;
    req.last_log_index = 0;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::RequestVoteRequest>{req}, 3);

    CHECK(s.voted_for.has_value() && *s.voted_for == 2);
    CHECK(r.messages.size() == 1);
    CHECK(r.messages[0].to == 2);
    const auto* resp = std::get_if<raft::RequestVoteResponse>(&r.messages[0].message);
    CHECK(resp != nullptr);
    if (resp) {
        CHECK(resp->vote_granted);
        CHECK(resp->term == 5);
    }
}

void test_rejects_second_candidate_same_term() {
    raft::RaftState s;
    s.role = raft::Role::kFollower;
    s.current_term = 5;
    s.voted_for = 3;  // already voted for node 3 this term

    raft::RequestVoteRequest req;
    req.term = 5;
    req.candidate_id = 2;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::RequestVoteRequest>{req}, 3);

    CHECK(s.voted_for.has_value() && *s.voted_for == 3);  // unchanged
    const auto* resp = std::get_if<raft::RequestVoteResponse>(&r.messages[0].message);
    CHECK(resp != nullptr && !resp->vote_granted);
}

// Re-requesting from the SAME candidate in the same term must be granted
// again (idempotent retry), not rejected as "already voted for someone
// else" — condition 2 explicitly allows voted_for == candidate_id.
void test_regrants_vote_to_same_candidate_on_retry() {
    raft::RaftState s;
    s.role = raft::Role::kFollower;
    s.current_term = 5;
    s.voted_for = 2;

    raft::RequestVoteRequest req;
    req.term = 5;
    req.candidate_id = 2;  // same candidate we already voted for

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::RequestVoteRequest>{req}, 3);

    const auto* resp = std::get_if<raft::RequestVoteResponse>(&r.messages[0].message);
    CHECK(resp != nullptr && resp->vote_granted);
}

void test_rejects_stale_term_request_and_replies_with_current_term() {
    raft::RaftState s;
    s.role = raft::Role::kLeader;
    s.current_term = 8;

    raft::RequestVoteRequest req;
    req.term = 6;  // stale
    req.candidate_id = 9;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::RequestVoteRequest>{req}, 3);

    CHECK(s.role == raft::Role::kLeader);  // untouched, term 6 < 8
    CHECK(s.current_term == 8);
    const auto* resp = std::get_if<raft::RequestVoteResponse>(&r.messages[0].message);
    CHECK(resp != nullptr);
    if (resp) {
        CHECK(!resp->vote_granted);
        CHECK(resp->term == 8);  // stale sender learns the real term
    }
}

// A higher term arriving on a RequestVoteRequest must clear a previous
// vote from the OLD term before condition 2 is evaluated — otherwise a
// legitimate new-term candidate could be wrongly rejected.
void test_higher_term_request_clears_old_vote_before_granting() {
    raft::RaftState s;
    s.role = raft::Role::kFollower;
    s.current_term = 5;
    s.voted_for = 3;  // voted for node 3 back in term 5

    raft::RequestVoteRequest req;
    req.term = 6;  // higher term
    req.candidate_id = 2;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::RequestVoteRequest>{req}, 3);

    CHECK(s.current_term == 6);
    CHECK(s.voted_for.has_value() && *s.voted_for == 2);
    const auto* resp = std::get_if<raft::RequestVoteResponse>(&r.messages[0].message);
    CHECK(resp != nullptr && resp->vote_granted);
}

// ---------------------------------------------------------------------------
// RequestVoteResponse handling — winning an election
// ---------------------------------------------------------------------------

void test_wins_election_on_majority_in_three_node_cluster() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kCandidate;
    s.current_term = 4;
    s.votes_received = {1};  // voted for self already

    raft::RequestVoteResponse resp;
    resp.term = 4;
    resp.vote_granted = true;

    const raft::HandleResult r = raft::handle(
        s, /*from=*/2, raft::Received<raft::RequestVoteResponse>{resp}, /*cluster_size=*/3);

    CHECK(s.role == raft::Role::kLeader);
    CHECK(r.became_leader);
    CHECK(r.messages.size() == 1);
    CHECK(r.messages[0].to == raft::kBroadcast);
    CHECK(std::holds_alternative<raft::AppendEntriesRequest>(r.messages[0].message));
}

// Duplicate responses from the same voter must not double-count toward
// the majority (design notes §2.4's idempotency reasoning).
void test_duplicate_vote_from_same_node_does_not_double_count() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kCandidate;
    s.current_term = 4;
    s.votes_received = {1};

    raft::RequestVoteResponse resp;
    resp.term = 4;
    resp.vote_granted = true;

    // In a 5-node cluster, majority is 3. One real vote from node 2,
    // delivered twice, must NOT reach majority (would need node 1, 2, and
    // one more distinct voter).
    raft::handle(s, /*from=*/2, raft::Received<raft::RequestVoteResponse>{resp}, 5);
    const raft::HandleResult r2 =
        raft::handle(s, /*from=*/2, raft::Received<raft::RequestVoteResponse>{resp}, 5);

    CHECK(s.votes_received.size() == 2);  // {1, 2}, not 3
    CHECK(s.role == raft::Role::kCandidate);
    CHECK(!r2.became_leader);
}

void test_five_node_cluster_needs_three_votes() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kCandidate;
    s.current_term = 4;
    s.votes_received = {1};

    raft::RequestVoteResponse granted;
    granted.term = 4;
    granted.vote_granted = true;

    const raft::HandleResult after_second =
        raft::handle(s, 2, raft::Received<raft::RequestVoteResponse>{granted}, 5);
    CHECK(s.role == raft::Role::kCandidate);  // 2 votes, need 3
    CHECK(!after_second.became_leader);

    const raft::HandleResult after_third =
        raft::handle(s, 3, raft::Received<raft::RequestVoteResponse>{granted}, 5);
    CHECK(s.role == raft::Role::kLeader);  // 3 votes reaches majority(5)=3
    CHECK(after_third.became_leader);
}

// A response carrying a higher term than ours must make us step down, and
// must NOT be counted as a vote even if vote_granted happens to be true
// (which shouldn't occur in a correct peer, but the state machine must not
// trust that).
void test_response_with_higher_term_steps_down_not_counted() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kCandidate;
    s.current_term = 4;
    s.voted_for = 1;
    s.votes_received = {1};

    raft::RequestVoteResponse resp;
    resp.term = 9;  // higher
    resp.vote_granted = true;

    const raft::HandleResult r =
        raft::handle(s, 2, raft::Received<raft::RequestVoteResponse>{resp}, 3);

    CHECK(s.role == raft::Role::kFollower);
    CHECK(s.current_term == 9);
    CHECK(!s.voted_for.has_value());
    CHECK(!r.became_leader);
    CHECK(r.messages.empty());
}

// A response is irrelevant if we're no longer a Candidate at all (e.g. we
// already won, or already stepped down for an unrelated reason).
void test_response_ignored_when_not_a_candidate() {
    raft::RaftState s;
    s.role = raft::Role::kLeader;  // already won
    s.current_term = 4;

    raft::RequestVoteResponse resp;
    resp.term = 4;
    resp.vote_granted = true;

    const raft::HandleResult r =
        raft::handle(s, 2, raft::Received<raft::RequestVoteResponse>{resp}, 3);

    CHECK(s.role == raft::Role::kLeader);
    CHECK(!r.became_leader);  // no new transition happened
    CHECK(r.messages.empty());
}

// A response for a term we've already moved past (but which wasn't itself
// higher, so maybe_update_term left state alone) must not be counted.
void test_stale_term_response_not_counted() {
    raft::RaftState s;
    s.self_id = 1;
    s.role = raft::Role::kCandidate;
    s.current_term = 6;  // we're already in term 6
    s.votes_received = {1};

    raft::RequestVoteResponse resp;
    resp.term = 5;  // response from an old election we started
    resp.vote_granted = true;

    const raft::HandleResult r =
        raft::handle(s, 2, raft::Received<raft::RequestVoteResponse>{resp}, 3);

    CHECK(s.votes_received.size() == 1);  // not incremented
    CHECK(s.role == raft::Role::kCandidate);
    CHECK(!r.became_leader);
}

// ---------------------------------------------------------------------------
// AppendEntriesRequest — Candidate stepping down on a legitimate leader
// ---------------------------------------------------------------------------

void test_candidate_steps_down_on_append_entries_same_term() {
    raft::RaftState s;
    s.self_id = 2;
    s.role = raft::Role::kCandidate;
    s.current_term = 4;
    s.voted_for = 2;
    s.votes_received = {2};

    raft::AppendEntriesRequest req;
    req.term = 4;  // SAME term, not higher — maybe_update_term alone
                   // would not have demoted us (Step 2a's equal-term rule)
    req.leader_id = 1;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    CHECK(s.role == raft::Role::kFollower);
    CHECK(s.current_term == 4);  // term itself didn't change
    const auto* resp = std::get_if<raft::AppendEntriesResponse>(&r.messages[0].message);
    CHECK(resp != nullptr && resp->success);
}

void test_append_entries_stale_term_rejected() {
    raft::RaftState s;
    s.role = raft::Role::kLeader;
    s.current_term = 10;

    raft::AppendEntriesRequest req;
    req.term = 7;
    req.leader_id = 3;

    const raft::HandleResult r =
        raft::handle(s, kIgnoredFrom, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    CHECK(s.role == raft::Role::kLeader);  // untouched
    const auto* resp = std::get_if<raft::AppendEntriesResponse>(&r.messages[0].message);
    CHECK(resp != nullptr);
    if (resp) {
        CHECK(!resp->success);
        CHECK(resp->term == 10);
    }
}

// ---------------------------------------------------------------------------
// AppendEntriesResponse — term bookkeeping only at this step
// ---------------------------------------------------------------------------

void test_append_entries_response_higher_term_steps_down_leader() {
    raft::RaftState s;
    s.role = raft::Role::kLeader;
    s.current_term = 4;

    raft::AppendEntriesResponse resp;
    resp.term = 5;
    resp.success = false;

    raft::handle(s, kIgnoredFrom, raft::Received<raft::AppendEntriesResponse>{resp}, 3);

    CHECK(s.role == raft::Role::kFollower);
    CHECK(s.current_term == 5);
}

}  // namespace

int main() {
    test_higher_term_still_steps_down_leader();

    test_log_comparison_higher_term_wins_even_with_shorter_log();
    test_log_comparison_lower_term_loses_even_with_longer_log();
    test_log_comparison_equal_term_compares_index();
    test_log_comparison_exact_tie_is_up_to_date();

    test_election_timeout_from_follower_becomes_candidate();
    test_election_timeout_increments_by_exactly_one();
    test_election_timeout_ignored_by_leader();

    test_grants_vote_when_eligible();
    test_rejects_second_candidate_same_term();
    test_regrants_vote_to_same_candidate_on_retry();
    test_rejects_stale_term_request_and_replies_with_current_term();
    test_higher_term_request_clears_old_vote_before_granting();

    test_wins_election_on_majority_in_three_node_cluster();
    test_duplicate_vote_from_same_node_does_not_double_count();
    test_five_node_cluster_needs_three_votes();
    test_response_with_higher_term_steps_down_not_counted();
    test_response_ignored_when_not_a_candidate();
    test_stale_term_response_not_counted();

    test_candidate_steps_down_on_append_entries_same_term();
    test_append_entries_stale_term_rejected();

    test_append_entries_response_higher_term_steps_down_leader();

    if (g_failures == 0) {
        std::printf("test_state_machine: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "test_state_machine: %d check(s) failed\n", g_failures);
    return 1;
}