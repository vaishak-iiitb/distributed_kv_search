#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <host> <port> <PUT key value | GET key | DELETE key>\n";
    return 1;
  }

  std::string host = argv[1];
  int port = std::atoi(argv[2]);

  std::ostringstream command;
  for (int i = 3; i < argc; i++) {
    command << argv[i];
    if (i + 1 < argc) command << " ";
  }
  command << "\n";

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "Could not connect to " << host << ":" << port << "\n";
    return 1;
  }

  std::string cmd_str = command.str();
  send(sock, cmd_str.data(), cmd_str.size(), 0);

  char buf[4096];
  ssize_t n = recv(sock, buf, sizeof(buf), 0);
  if (n > 0) {
    std::cout.write(buf, n);
  }

  close(sock);
  return 0;
}