#pragma once

#include <atomic>
#include <string>
#include <thread>

#include "net/thread_pool.h"
#include "storage/kv_engine.h"

// Thread-pool TCP server exposing KVEngine over a simple line-based text
// protocol. See section 6 of the accompanying doc for the protocol and
// its deliberate limitations.
class TCPServer {
 public:
  TCPServer(int port, KVEngine* engine, size_t thread_pool_size = 4);
  ~TCPServer();

  TCPServer(const TCPServer&) = delete;
  TCPServer& operator=(const TCPServer&) = delete;

  // Binds, listens, and starts accepting connections on a background
  // thread. Returns false on any socket setup failure.
  bool Start();

  // Stops accepting new connections and joins the accept thread. Safe to
  // call multiple times; safe to skip if Start() was never called or
  // failed. The destructor calls this too.
  void Stop();

 private:
  void AcceptLoop();
  void HandleClient(int client_fd);
  std::string HandleCommand(const std::string& line);

  int port_;
  KVEngine* engine_;
  ThreadPool pool_;
  int listen_fd_;
  std::atomic<bool> running_;
  std::thread accept_thread_;
};