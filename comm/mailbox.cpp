#include "comm/mailbox.hpp"

#include "glog/logging.h"

namespace flexps {

void Mailbox::Start(const std::vector<Node>& nodes) {
  context_ = zmq_ctx_new();
  CHECK(context_ != nullptr) << "create zmq context failed";
  zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);

  Bind(node_);
  for (const auto& node : nodes) {
    Connect(node);
  }

  receiver_thread_ = std::thread(&Mailbox::Receiving, this);
}

void Mailbox::Stop() {
  receiver_thread_.join();
}

void Mailbox::Connect(const Node& node) {
  auto it = senders_.find(node.id);
  if (it != senders_.end()) {
    zmq_close(it->second);
  }
  void* sender = zmq_socket(context_, ZMQ_DEALER);
  // CHECK(sender != nullptr) << zmq_strerror(errno);
  CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
  std::string my_id = "ps" + std::to_string(node_.id);
  zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
  std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(FATAL) << "connect to " + addr + " failed: " << zmq_strerror(errno);
  }
  senders_[node.id] = sender;
}

void Mailbox::Bind(const Node& node) {
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != nullptr) << "create receiver socket failed: " << zmq_strerror(errno);
  std::string address = "tcp://*:" + std::to_string(node.port);
  if (zmq_bind(receiver_, address.c_str()) != 0) {
    LOG(FATAL) << "bind to " + address + " failed: " << zmq_strerror(errno);
  }
}

void Mailbox::RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) {
  CHECK(queue_map_.find(queue_id) != queue_map_.end());
  queue_map_.insert({queue_id, queue});
}

void Mailbox::Receiving() {

  LOG(INFO) << "Starting receiving";
  while (true) {
    Message msg;
    int recv_bytes = Recv(&msg);
    // For debug, show received message
    LOG(INFO) << "Received message " << msg.DebugString();

    CHECK(queue_map_.find(msg.meta.recver) != queue_map_.end());
    queue_map_[msg.meta.recver]->Push(msg);
  }
}

// \brief Free the recved data
inline void MyFree(void *data, void *hint) {
  if (hint == NULL) {
    delete [] static_cast<char*>(data);
  } else {
    delete static_cast<third_party::SArray<char>*>(hint);
  }
}

/* Mailbox::PackMeta(const Meta& meta, char** meta_buf, int* buf_size) {
  // convert into protobuf
  PBMeta pb;
  pb.set_sender(meta.sender);
  pb.set_recver(meta.recver);
  pb.set_model_id(meta.model_id);
  pb.set_flag(meta.flag);

  // to string
  *buf_size = pb.ByteSize();
  *meta_buf = new char[*buf_size + 1];
  CHECK(pb.SerializeToArray(*meta_buf, *buf_size))
      << "failed to serialize protbuf";
}

void Mailbox::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) {
  // to protobuf
  PBMeta pb;
  CHECK(pb.ParseFromArray(meta_buf, buf_size))
      << "failed to parse string into protobuf";

  // to meta
  meta->sender = pb.sender();
  meta->recver = pb.recver();
  meta->model_id = pb.model_id()
  meta->flag = pb.flag();
}

// \brief return the node id given the received identity
// \return -1 if not find
int GetNodeID(const char* buf, size_t size) {
  if (size > 6 && buf[0] == 'f' && buf[1] == 'l' && buf[0] == 'e' && buf[1] == 'x' && buf[0] == 'p' && buf[1] == 's') {
    int id = 0;
    size_t i = 6;
    for (; i < size; ++i) {
      if (buf[i] >= '0' && buf[i] <= '9') {
        id = id * 10 + buf[i] - '0';
      } else {
        break;
      }
    }
    if (i == size) return id;
  }
  return -1;
}
*/

int Mailbox::Send(const Message& msg) {
    std::lock_guard<std::mutex> lk(lock_);
    // find the socket
    int id = msg.meta.recver;
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second;

    // send meta
    int meta_size = sizeof(msg.meta);
    char meta_buf[meta_size];
    memcpy(meta_buf, &msg.meta, meta_size);

    int tag = ZMQ_SNDMORE;
    int num_data = msg.data.size();
    if (num_data == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, MyFree, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to send message to node [" << id
                   << "] errno: " << errno << " " << zmq_strerror(errno);
      return -1;
    }
    zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;

    // send data
    for (int i = 0; i < num_data; ++i) {
      zmq_msg_t data_msg;
      third_party::SArray<char>* data = new third_party::SArray<char>(msg.data[i]);
      int data_size = data->size();
      zmq_msg_init_data(&data_msg, data->data(), data->size(), MyFree, data);
      if (i == num_data - 1) tag = 0;
      while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno)
                     << ". " << i << "/" << num_data;
        return -1;
      }
      zmq_msg_close(&data_msg);
      send_bytes += data_size;
    }
    return send_bytes;
  }

int Mailbox::Recv(Message* msg) {
  msg->data.clear();
  size_t recv_bytes = 0;
  for (int i = 0; ; ++i) {
    zmq_msg_t* zmsg = new zmq_msg_t;
    CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
    while (true) {
      if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to receive message. errno: "
                   << errno << " " << zmq_strerror(errno);
      return -1;
    }
    
    size_t size = zmq_msg_size(zmsg);
    recv_bytes += size;
    char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));

    if (i == 0) {
      // identify, don't care
      CHECK(zmq_msg_more(zmsg));
      zmq_msg_close(zmsg);
      delete zmsg;
    } else if (i == 1) {
      // task
      // Unpack the meta
      Meta meta = Meta();
      memcpy(&meta, buf, sizeof(buf));
      msg->meta.sender = meta.sender;
      msg->meta.recver = meta.recver;
      msg->meta.model_id = meta.model_id;
      msg->meta.flag = meta.flag;
      zmq_msg_close(zmsg);
      bool more = zmq_msg_more(zmsg);
      delete zmsg;
      if (!more) break;
    } else {
      // zero-copy
      third_party::SArray<char> data;
      data.reset(buf, size, [zmsg, size](char* buf) {
          zmq_msg_close(zmsg);
          delete zmsg;
        });
      msg->data.push_back(data);
      if (!zmq_msg_more(zmsg)) { break; }
    }
  }
  return recv_bytes;
}

}  // namespace flexps
