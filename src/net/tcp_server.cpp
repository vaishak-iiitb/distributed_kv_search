#include "net/tcp_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cstring>
#include <sstream>

TCPServer::TCPServer(int port, KVEngine* engine, size_t thread_pool_size)
    : port_(port),
      engine_(engine),
      pool_(thread_pool_size),
      listen_fd_(-1),
      running_(false) {}

TCPServer::~TCPServer() { Stop(); }

bool TCPServer::Start() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return false;
  }

  int opt = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port_));

  if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(listen_fd_);
    return false;
  }
  if (listen(listen_fd_, /*backlog=*/16) < 0) {
    close(listen_fd_);
    return false;
  }

  running_ = true;
  accept_thread_ = std::thread(&TCPServer::AcceptLoop, this);
  return true;
}

void TCPServer::AcceptLoop() {
  while (running_) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr),
                            &client_len);
    if (client_fd < 0) {
      // Stop() closes listen_fd_, which unblocks accept() with an error —
      // that's the expected shutdown path, not a real failure.
      if (!running_) break;
      continue;
    }
    pool_.Enqueue([this, client_fd] { HandleClient(client_fd); });
  }
}

void TCPServer::HandleClient(int client_fd) {
  std::string buffer;
  char chunk[4096];

  while (running_) {
    ssize_t n = recv(client_fd, chunk, sizeof(chunk), 0);
    if (n <= 0) break;  // 0 = client closed; <0 = socket error

    buffer.append(chunk, static_cast<size_t>(n));

    size_t newline_pos;
    while ((newline_pos = buffer.find('\n')) != std::string::npos) {
      std::string line = buffer.substr(0, newline_pos);
      buffer.erase(0, newline_pos + 1);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();  // tolerate CRLF from tools like telnet
      }

      std::string response = HandleCommand(line);
      send(client_fd, response.data(), response.size(), 0);
    }
  }

  close(client_fd);
}

std::string TCPServer::HandleCommand(const std::string& line) {
  std::istringstream iss(line);
  std::string cmd;
  iss >> cmd;
  for (auto& c : cmd) c = static_cast<char>(std::toupper(c));

  if (cmd == "PUT") {
    std::string key, value;
    iss >> key >> value;
    if (key.empty() || value.empty()) return "ERROR bad_request\n";
    return engine_->Put(key, value) ? "OK\n" : "ERROR write_failed\n";
  }

  if (cmd == "GET") {
    std::string key;
    iss >> key;
    if (key.empty()) return "ERROR bad_request\n";
    std::string value;
    if (engine_->Get(key, &value)) return "VALUE " + value + "\n";
    return "NOT_FOUND\n";
  }

  if (cmd == "DELETE") {
    std::string key;
    iss >> key;
    if (key.empty()) return "ERROR bad_request\n";
    return engine_->Delete(key) ? "OK\n" : "ERROR write_failed\n";
  }

  return "ERROR unknown_command\n";
}

void TCPServer::Stop() {
  if (!running_) return;
  running_ = false;
  shutdown(listen_fd_, SHUT_RDWR);
  close(listen_fd_);
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
}