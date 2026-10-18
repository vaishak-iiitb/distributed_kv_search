// The leader's new-term no-op entry (flag #2 fix) must never be mistaken for,
// or collide with, a real client command.

#include <cstdio>
#include <string>

#include "raft/command.h"

static int g_failures = 0;
#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                                 \
        }                                                                                 \
    } while (0)

int main() {
    using namespace raft;

    CHECK(is_noop_command(kNoopCommand));
    CHECK(!is_noop_command(""));
    CHECK(!is_noop_command("NOOP "));
    CHECK(!is_noop_command("noop"));

    // The no-op is not a decodable command: it can never be applied as a Put/Delete.
    CHECK(!decode_command(kNoopCommand).has_value());

    // No real command encodes to the no-op bytes.
    Command put;
    put.type = CommandType::kPut;
    put.client_id = 1;
    put.sequence_num = 1;
    put.key = "NOOP";
    put.value = "NOOP";
    CHECK(!is_noop_command(encode_command(put)));
    Command del;
    del.type = CommandType::kDelete;
    del.client_id = 1;
    del.sequence_num = 2;
    del.key = "NOOP";
    CHECK(!is_noop_command(encode_command(del)));

    // Real commands still round-trip.
    const auto back = decode_command(encode_command(put));
    CHECK(back.has_value() && back->key == "NOOP" && back->value == "NOOP");

    if (g_failures == 0) {
        std::printf("noop_command_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "noop_command_test: %d check(s) failed\n", g_failures);
    return 1;
}