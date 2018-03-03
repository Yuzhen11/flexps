#include "gflags/gflags.h"
#include "glog/logging.h"

#include "comm/channel.hpp"

#include "base/node_util.hpp"
#include "comm/mailbox.hpp"
#include "driver/simple_id_mapper.hpp"

#include "examples/gbdt/src/data_loader.hpp"
#include "examples/gbdt/src/math_tools.hpp"

namespace flexps {

void channel_for_balancing_hdfs_data(DataLoader& train_data_loader, int& global_data_num, Node& my_node, std::vector<Node>& nodes) {

  // 1. Manually start id_mapper and mailbox
  SimpleIdMapper id_mapper(my_node, nodes);
  id_mapper.Init();
  Mailbox mailbox(my_node, nodes, &id_mapper);
  mailbox.Start();

  // 2. Allocate the channel threads and create channel
  // use thread 1 since it is communication between nodes
  const uint32_t num_local_threads = 1;
  const uint32_t num_global_threads = nodes.size();
  auto ret = id_mapper.GetChannelThreads(num_local_threads, num_global_threads);
  Channel ch(num_local_threads, num_global_threads, ret.first, ret.second, &mailbox);
  // Retrive the local_channels from channel
  auto local_channels = ch.GetLocalChannels();

  std::thread th = std::thread([local_channels, num_global_threads, & train_data_loader, & global_data_num]() { 
      LocalChannel* lc = local_channels[0];

      // Step 1: Send local num of data to channel 0, then channel 0 find the mean
      SArrayBinStream send_bin;
        
      const std::vector<int> msg {
        lc->GetId(), 
        train_data_loader.get_class_vect().size()
      };
      send_bin << msg;
      lc->PushTo(0, send_bin);

      // Channel 0 sum up all the result and find the mean
      auto recv_bin = lc->SyncAndGet();
      if (lc->GetId() == 0) {
        
        std::map<int, int> node_id_to_data_num_map;
        int sum = 0.0;
        int cnt = 0.0;

        std::vector<int> recv_result;
          
        for (SArrayBinStream stream: recv_bin) {
          stream >> recv_result;
          int node_id = recv_result[0];
          int data_num = recv_result[1];
          node_id_to_data_num_map[node_id] = data_num;
          sum += data_num;
          cnt += 1;
        }
        global_data_num = sum;

        // Check which nodes have sparse data, which nodes need more data
        int avg = sum / cnt;
        std::map<int, int> node_id_to_resource_available;
        std::map<int, int> node_id_to_resource_needed;

        for (auto const& record: node_id_to_data_num_map) {
          int diff = record.second - avg;
          if (diff < 0) {
            node_id_to_resource_needed[record.first] = -diff;
          }
          else if (diff > 0) {
            node_id_to_resource_available[record.first] = diff;
          }
        }

        // Distribute data to less data node
        std::map<int, std::vector<int>> node_id_to_instructions;

        std::map<int, int>::iterator it;
        for (it = node_id_to_resource_available.begin(); it != node_id_to_resource_available.end(); it++) {
          int from_node_id = it->first;
          int available_num = it->second;
          if (available_num == 0) {
          	continue;
          }

          std::map<int, int>::iterator it2;
          for (it2 = node_id_to_resource_needed.begin(); it2 != node_id_to_resource_needed.end(); it2++) {
            int to_node_id = it2->first;
            int needed_num = it2->second;

            if (needed_num == 0 || from_node_id == to_node_id) {
              continue;
            }

            int allocated_num = 0;
            if (available_num > needed_num) {
              allocated_num = needed_num;
            }
            else {
              allocated_num = available_num;
            }
            // Create instruction message
            if (allocated_num > 0) {
              if (node_id_to_instructions.find(from_node_id) == node_id_to_instructions.end()) {
                std::vector<int> v = {to_node_id, allocated_num};
                node_id_to_instructions[from_node_id] = v;
              }
              else {
              	node_id_to_instructions[from_node_id].push_back(to_node_id);
                node_id_to_instructions[from_node_id].push_back(allocated_num); 
              }
              it2->second -= allocated_num;
              available_num -= allocated_num;
            }
            if (available_num <= 0) {
              break;
            }
          }
          it->second = available_num;
        }
        // print log to check
        for (auto const& x : node_id_to_instructions) {
          int from_node_id = x.first;
          std::vector<int> v = x.second;
          for (int i = 0; i < v.size(); i += 2) {
            int to_node_id = v[i];
            int amount = v[i + 1];
          } 
        }

        // Distribute instructions to nodes
        for (auto const& x : node_id_to_instructions) {
          int from_node_id = x.first;
          const std::vector<int> msg = x.second; 
          SArrayBinStream send_bin;
          send_bin << msg;
          lc->PushTo(from_node_id, send_bin);
        }
      }

      // Nodes wait for instruction from channel 0
      auto recv_bin2 = lc->SyncAndGet();
      std::vector<int> recv_result2;
      for (SArrayBinStream stream: recv_bin2) {
        stream >> recv_result2;
        for (int i = 0; i < recv_result2.size(); i += 2) {
          int to_node_id = recv_result2[i];
          int amount = recv_result2[i + 1];

          SArrayBinStream send_bin;
          for (int i = 0; i < amount; i++) {
          	std::vector<float> msg = train_data_loader.pop();
          	send_bin << msg;
          }
          
          lc->PushTo(to_node_id, send_bin);
        }
      }

      // Receive record from other channel
      auto recv_bin3 = lc->SyncAndGet();
      std::vector<float> recv_result3;
      for (SArrayBinStream stream: recv_bin3) {
      	while (stream.Size() > 0) {
          stream >> recv_result3;
          train_data_loader.push(recv_result3);
        }
      }

      // Send global data num
      if(lc->GetId() == 0) {
        SArrayBinStream send_bin;
          
        const std::vector<int> msg {
          global_data_num
        };
        send_bin << msg;

        for (int i = 0; i < num_global_threads; i++) {
          lc->PushTo(i, send_bin);
        }
      }
      auto recv_bin4 = lc->SyncAndGet();
      std::vector<int> recv_result4;
      for (SArrayBinStream stream: recv_bin4) {
        stream >> recv_result4;
        global_data_num = recv_result4[0];
      }
  });

  th.join();  
  
  id_mapper.ReleaseChannelThreads();
  mailbox.Stop();
}

std::vector<std::map<std::string, float>> channel_for_global_min_max_feat(std::vector<std::map<std::string, float>>& min_max_feat_list, Node& my_node, std::vector<Node>& nodes) {
  
  std::vector<std::map<std::string, float>> global_min_max_feat_list;

  // 1. Manually start id_mapper and mailbox
  SimpleIdMapper id_mapper(my_node, nodes);
  id_mapper.Init();
  Mailbox mailbox(my_node, nodes, &id_mapper);
  mailbox.Start();

  // 2. Allocate the channel threads and create channel
  // use thread 1 since it is communication between nodes
  const uint32_t num_local_threads = 1;
  const uint32_t num_global_threads = nodes.size();
  auto ret = id_mapper.GetChannelThreads(num_local_threads, num_global_threads);
  Channel ch(num_local_threads, num_global_threads, ret.first, ret.second, &mailbox);
  // Retrive the local_channels from channel
  auto local_channels = ch.GetLocalChannels();

  // 3. Sprawn num_local_threads threads to use local_channels
  std::thread th = std::thread([local_channels, num_global_threads, & global_min_max_feat_list, & min_max_feat_list]() { 
      
    // Send the max min to channel 0
    LocalChannel* lc = local_channels[0];
      
    for (int j = 0; j < min_max_feat_list.size(); j++) {
      SArrayBinStream send_bin;
        
      const std::vector<float> msg {
        min_max_feat_list[j]["min"], 
        min_max_feat_list[j]["max"]
      };

      send_bin << msg;
      lc->PushTo(0, send_bin);

      // Channel 0 sum up all the result and find the RMSE
      auto recv_bin = lc->SyncAndGet();
      if (lc->GetId() == 0) {
        float min_feat = std::numeric_limits<float>::infinity();
        float max_feat = -std::numeric_limits<float>::infinity();
        std::vector<float> recv_result;
          
        for (SArrayBinStream stream: recv_bin) {
          stream >> recv_result;
          if (recv_result[0] < min_feat) {
            min_feat = recv_result[0];
          }
          if (recv_result[1] > max_feat) {
            max_feat = recv_result[1];
          }
        }

        std::map<std::string, float> res;
        res["min"] = min_feat;
        res["max"] = max_feat;
        global_min_max_feat_list.push_back(res);
      }
    }

    // thread 0 distributes global result to other threads
    if (lc->GetId() == 0) {
      SArrayBinStream send_bin;
      for (int j = 0; j < global_min_max_feat_list.size(); j++) {
        const std::vector<float> msg {
          global_min_max_feat_list[j]["min"], 
          global_min_max_feat_list[j]["max"]
        };
        send_bin << msg;
      }
      for (int k = 1; k < num_global_threads; k++) {
        lc->PushTo(k, send_bin);
      }
    }
    auto recv_bin2 = lc->SyncAndGet();
    if (lc->GetId() != 0) {
      std::vector<float> recv_result2;
      for (SArrayBinStream stream: recv_bin2) {
        while (stream.Size() > 0) {
          stream >> recv_result2;
          std::map<std::string, float> res;
          res["min"] = recv_result2[0];
          res["max"] = recv_result2[1];
          global_min_max_feat_list.push_back(res);
        }
      }
    }
  });

  th.join();

  id_mapper.ReleaseChannelThreads();
  mailbox.Stop();

  return global_min_max_feat_list;
}

void channel_for_global_predict_result(const std::map<std::string, float>& local_predict_result, Node& my_node, std::vector<Node>& nodes) {
  
  // 1. Manually start id_mapper and mailbox
  SimpleIdMapper id_mapper(my_node, nodes);
  id_mapper.Init();
  Mailbox mailbox(my_node, nodes, &id_mapper);
  mailbox.Start();

  // 2. Allocate the channel threads and create channel
  // use thread 1 since it is communication between nodes
  const uint32_t num_local_threads = 1;
  const uint32_t num_global_threads = nodes.size();
  auto ret = id_mapper.GetChannelThreads(num_local_threads, num_global_threads);
  Channel ch(num_local_threads, num_global_threads, ret.first, ret.second, &mailbox);
  // Retrive the local_channels from channel
  auto local_channels = ch.GetLocalChannels();

  // 3. Sprawn num_local_threads threads to use local_channels
  std::vector<std::thread> threads(local_channels.size());
  for (int i = 0; i < threads.size(); ++ i) {
    threads[i] = std::thread([i, local_channels, num_global_threads, & local_predict_result]() { 
      
      // Send the predict result to channel 0
      LocalChannel* lc = local_channels[i];

      SArrayBinStream send_bin;
      
      const std::vector<float> msg {
        local_predict_result.at("sse"), 
        local_predict_result.at("num")
      };

      send_bin << msg;
      lc->PushTo(0, send_bin);

      // Channel 0 sum up all the result and find the RMSE
      auto recv_bin = lc->SyncAndGet();
      if (lc->GetId() == 0) {
        std::map<std::string, float> global_predict_result;
        global_predict_result["sse"] = 0;
        global_predict_result["num"] = 0;

        std::vector<float> recv_msg;
        for (SArrayBinStream stream: recv_bin) {
          stream >> recv_msg;
          global_predict_result["sse"] += recv_msg[0];
          global_predict_result["num"] += recv_msg[1];
        }
        LOG(INFO) << "global sse = " << global_predict_result["sse"];
        LOG(INFO) << "global num = " << global_predict_result["num"];
        LOG(INFO) << "The RMSE is " << calculate_rmse(global_predict_result["sse"], global_predict_result["num"]);
      }
    });
  }

  for (auto& th: threads) {
    th.join();  
  }
  id_mapper.ReleaseChannelThreads();
  mailbox.Stop();
}

}
