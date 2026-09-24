#include <csignal>
#include <cstdlib>
#include <chrono>
#include <iostream>
#include <thread>

#include "net/tcp_server.h"
#include "storage/kv_engine.h"

namespace {
TCPServer* g_server = nullptr;

void HandleSigint(int) {
  if (g_server != nullptr) {
    g_server->Stop();
  }
  std::exit(0);
}
}  // namespace

int main(int argc, char** argv) {
  int port = 9090;
  std::string data_dir = "kv_data";
  if (argc > 1) port = std::atoi(argv[1]);
  if (argc > 2) data_dir = argv[2];

  KVEngine engine(data_dir);
  TCPServer server(port, &engine);
  g_server = &server;
  std::signal(SIGINT, HandleSigint);

  if (!server.Start()) {
    std::cerr << "Failed to start server on port " << port << "\n";
    return 1;
  }

  std::cout << "KV store listening on port " << port
            << " (data dir: " << data_dir << ")\n";
  std::cout << "Press Ctrl+C to stop.\n";

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(3600));
  }
  return 0;
}