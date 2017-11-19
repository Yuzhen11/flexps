#!/usr/bin/env python
 
import sys
from launch_utils import launch_util

#hostfile = "machinefiles/local"
hostfile = "machinefiles/5node"
progfile = "debug/KMeansExample"

params = {
  "kStaleness" : 1,
  "kModelType" : "SSP",  # {ASP/SSP/BSP}
  #"num_dims" : 123,
  "num_dims" : 18,
  "num_workers_per_node" : 3, # 3
  "num_servers_per_node" : 1,
  "num_iters" : 1000, #1000
  "kStorageType" : "Vector",  # {Vector/Map}
  "hdfs_namenode" : "proj10",
  "hdfs_namenode_port" : 9000,
  #"input" : "hdfs:///datasets/classification/a9",
  "input" : "hdfs:///jasper/SUSY",
  "K" : 2,
  "batch_size" : 1000, # 100
  "alpha" : 0.1,
  "kmeans_init_mode" : "random",
  "report_interval" : "10",
  }

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  )

launch_util(progfile, hostfile, env_params, params, sys.argv)
