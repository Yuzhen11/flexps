#pragma once

#include <glog/logging.h>
#include <zmq.h>

inline void zmq_send_common(zmq::socket_t* socket, const void* data, const size_t& len, int flag = !ZMQ_NOBLOCK) {
  CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
  CHECK(data != nullptr || len == 0) << "data and len are not matched!";
  while (true)
    try {
      size_t bytes = socket->send(data, len, flag);
      CHECK(bytes == len) << "zmq::send error!";
      break;
    } catch (zmq::error_t e) {
      switch (e.num()) {
      case EHOSTUNREACH:
      case EINTR:
        continue;
      default:
        LOG(ERROR) << "Invalid type of zmq::error!";
      }
    }
}

inline void zmq_recv_common(zmq::socket_t* socket, zmq::message_t* msg, int flag = !ZMQ_NOBLOCK) {
  CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
  CHECK(msg != nullptr) << "data and len are not matched!";
  while (true)
    try {
      bool successful = socket->recv(msg, flag);
      CHECK(successful) << "zmq::receive error!";
      break;
    } catch (zmq::error_t e) {
      if (e.num() == EINTR)
        continue;
      LOG(ERROR) << "Invalid type of zmq::error!";
    }
}
