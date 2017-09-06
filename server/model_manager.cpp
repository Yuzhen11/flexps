#include "server/model_manager.hpp"

namespace flexps {

void ModelManager::Init() {
}

void ModelManager::Clock(int tid) {
  consistency_controller_->Clock(tid);
}

void ModelManager::Add(int tid, const Message& message) {
  consistency_controller_->Add(tid, message);
}

void ModelManager::Get(int tid, const Message& message) {
  consistency_controller_->Get(tid, message);
}

}  // namespace flexps
