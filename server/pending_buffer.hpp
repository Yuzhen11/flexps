#pragma once

#include "base/message.hpp"

#include <unordered_map>
#include <vector>

namespace flexps {

class PendingBuffer {
public:
	std::vector<Message> Pop(int32_t clock);
	void Push(int32_t clock, Message& message);
private:
	//TODO(Ruoyu Wu): each vec of buffer_ should be pop one by one, now it is not guaranteed
	std::unordered_map< int32_t, std::vector<Message> > buffer_;
};

}  // namespace flexps
