#!/usr/bin/env python

import sys 
from launch_utils import launch_util

#hostfile = "machinefiles/local"
hostfile = "machinefiles/5node"
progfile = "debug/SGDExample"


params = {
    "kStaleness" : 0,
    "kModelType" : "SSP",  # {ASP/SSP/BSP/SparseSSP}
    "num_dims" : 54686452,
    "batch_size" : 1,
    "num_workers_per_node" : 1,
    "num_servers_per_node" : 1,
    "num_iters" : 20,
    "kStorageType" : "Vector",  # {Vector/Map}
    "hdfs_namenode" : "proj10",
    "hdfs_namenode_port" : 9000,
    "input" : "hdfs:///jasper/kdd12",
    "alpha" : 0.0005, # learning rate
    "learning_rate_decay" : 20,
    "report_interval" : 1,
    "trainer" : "lr",  # {lr/lasso/svm}
    "lambda" : 0.01,
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  )

launch_util(progfile, hostfile, env_params, params, sys.argv)
