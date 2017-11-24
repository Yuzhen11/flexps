#include "driver/engine.hpp"

#include <thread>
#include <vector>

#include "glog/logging.h"

namespace flexps {

void Engine::StartEverything(int num_server_thread_per_node) {
  // Create IdMapper
  id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
  id_mapper_->Init(num_server_thread_per_node);
  
  // Start mailbox
  mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get()));
  CHECK(mailbox_);
  mailbox_->Start();
  VLOG(1) << "mailbox starts on node" << node_.id;

  // Start KVEngine
  kv_engine_.reset(new KVEngine(node_, nodes_, id_mapper_.get(), mailbox_.get()));
  kv_engine_->StartKVEngine();

  // Barrier
  mailbox_->Barrier();
  LOG(INFO) << "StartEverything in Node: " << node_.id;
}

void Engine::StopEverything() {
  // Stop mailbox
  CHECK(mailbox_);
  mailbox_->Stop();
  VLOG(1) << "mailbox stops on node" << node_.id;

  // Stop KVEngine
  kv_engine_->StopKVEngine();
  LOG(INFO) << "StopEverything in Node: " << node_.id;
}

void Engine::Run(const MLTask& task) {
  CHECK(kv_engine_);
  kv_engine_->Run(task);
}

void Engine::Barrier() {
  CHECK(mailbox_);
  mailbox_->Barrier();
}

}  // namespace flexps
