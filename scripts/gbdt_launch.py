#!/usr/bin/env python

import sys
from launch_utils import launch_util

#hostfile = "machinefiles/local"
hostfile = "machinefiles/5node"
progfile = "debug/GBDTExample"

params = {
    "config_file" : hostfile,
    "hdfs_namenode" : "proj10",
    "hdfs_namenode_port" : 9000,
    # TODO: replace by your own data
    "cluster_train_input" : "hdfs:///calvinzhou/40_data_train.dat",
    #"cluster_train_input" : "hdfs:///calvinzhou/regression/YearPredictionMSD_train.dat",
    "cluster_test_input" : "hdfs:///calvinzhou/40_data_train.dat",
    #"cluster_test_input" : "hdfs:///calvinzhou/regression/YearPredictionMSD_test.dat",
    "num_workers_per_node" : 1,
    "local_train_input" : "/home/ubuntu/Dataset/40_train.dat",
    "local_test_input" : "/home/ubuntu/Dataset/40_train.dat",
    "run_mode" : "cluster",
    # gbdt model config
    "num_of_trees" : 3,
    "max_depth" : 3,
    "complexity_of_leaf" : 0.05,
    "rank_fraction" : 0.1
}

env_params = (
  "GLOG_alsologtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  # this is to enable hdfs short-circuit read (disable the warning info)
  # change this path accordingly when we use other cluster
  # the current setting is for proj5-10
  "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  )

launch_util(progfile, hostfile, env_params, params, sys.argv)
