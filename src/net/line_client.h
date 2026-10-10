#pragma once

#include <optional>
#include <string>

namespace net {

// One-shot, deadline-bounded client for the project's newline-terminated
// text protocols (ClientServer's PUT/GET/DELETE/LEADER lines, and the
// Phase 1 TCPServer). Connect, send ONE line, read ONE line, close.
//
// Why one-shot and not a persistent connection: after a leader change a
// persistent connection points at the wrong node and has to be torn down
// anyway. A fresh connection per request makes "retry somewhere else" the
// same code path as "first try", at the cost of a TCP handshake per request
// (negligible on localhost, and fine for control-plane traffic).
class LineClient {
public:
    // Returns the response line WITHOUT its trailing "\n" (and without a
    // trailing "\r" if present). Returns std::nullopt on ANY failure:
    // unparseable host, connect refused/timeout, send failure, peer closed
    // before a full line arrived, or `timeout_ms` elapsed in total.
    //
    // `timeout_ms` bounds the WHOLE exchange (connect + send + receive), not
    // each step. Only IPv4 dotted-quad hosts and "localhost" are supported
    // (no DNS lookup — a resolver call can block far past any deadline).
    static std::optional<std::string> RoundTrip(const std::string& host, int port,
                                                const std::string& request_line,
                                                int timeout_ms);
};

}  // namespace net