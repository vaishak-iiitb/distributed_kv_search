#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "net/tcp_server.h"
#include "storage/kv_engine.h"

namespace {

std::string SendCommand(int port, const std::string& command) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return "CONNECT_FAILED";
  }

  std::string full_command = command + "\n";
  send(sock, full_command.data(), full_command.size(), 0);

  char buf[4096];
  ssize_t n = recv(sock, buf, sizeof(buf), 0);
  close(sock);
  return (n > 0) ? std::string(buf, static_cast<size_t>(n)) : "";
}

}  // namespace

int main() {
  const std::string kDataDir = "test_tcp_server_data";
  std::filesystem::remove_all(kDataDir);

  const int kPort = 9191;
  KVEngine engine(kDataDir);
  TCPServer server(kPort, &engine, /*thread_pool_size=*/4);
  assert(server.Start());

  // Give the accept loop a moment to actually be listening before the
  // first client connects.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // --- Basic protocol correctness ---
  assert(SendCommand(kPort, "PUT apple red") == "OK\n");
  assert(SendCommand(kPort, "GET apple") == "VALUE red\n");
  assert(SendCommand(kPort, "GET banana") == "NOT_FOUND\n");
  assert(SendCommand(kPort, "DELETE apple") == "OK\n");
  assert(SendCommand(kPort, "GET apple") == "NOT_FOUND\n");
  assert(SendCommand(kPort, "BOGUS foo") == "ERROR unknown_command\n");
  assert(SendCommand(kPort, "PUT keyonly") == "ERROR bad_request\n");

  // --- Concurrency smoke test: several clients through the pool at once ---
  std::vector<std::thread> clients;
  for (int i = 0; i < 10; i++) {
    clients.emplace_back([kPort, i] {
      std::string key = "concurrent" + std::to_string(i);
      std::string expected_value = "value" + std::to_string(i);
      assert(SendCommand(kPort, "PUT " + key + " " + expected_value) ==
             "OK\n");
      assert(SendCommand(kPort, "GET " + key) ==
             "VALUE " + expected_value + "\n");
    });
  }
  for (auto& t : clients) t.join();

  server.Stop();
  std::filesystem::remove_all(kDataDir);

  std::cout << "All TCP server tests passed.\n";
  return 0;
}