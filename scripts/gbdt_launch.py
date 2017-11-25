#!/usr/bin/env python

import sys
from launch_utils import launch_util


# The file path should be related path from the project home path.
# The hostfile should be in the format:
# node_id:hostname:port
# 
# Example:
# 0:worker1:37542
# 1:worker2:37542
# 2:worker3:37542
# 3:worker4:37542
# 4:worker5:37542
# 
hostfile = "machinefiles/local"
#hostfile = "machinefiles/5node"

progfile = "debug/GBDTExample"

params = {
    "hdfs_namenode" : "proj10",
    "hdfs_namenode_port" : 9000,
    "cluster_train_input" : "hdfs:///calvinzhou/40_data_train.dat", # TODO: replace by your own data
    "cluster_test_input" : "hdfs:///calvinzhou/40_data_train.dat",
    "num_workers_per_node" : 1,
    "local_train_input" : "/home/ubuntu/Git_Project/flexps/examples/gbdt/data/40_train.dat",
    "local_test_input" : "/home/ubuntu/Git_Project/flexps/examples/gbdt/data/40_train.dat",
    "run_mode" : "local",
    # gbdt model config
    "num_of_trees" : 3,
    "max_depth" : 3,
    "complexity_of_leaf" : 0.05,
    "rank_fraction" : 0.1,
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  # this is to enable hdfs short-circuit read (disable the warning info)
  # change this path accordingly when we use other cluster
  # the current setting is for proj5-10
  "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  )

launch_util(prog_path, hostfile_path, env_params, params, sys.argv)
