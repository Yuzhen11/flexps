#!/usr/bin/env python

import sys
from launch_utils import launch_util

#hostfile = "machinefiles/local"
hostfile = "machinefiles/5node"
progfile = "debug/GBDTExample"

params = {
    "hdfs_namenode" : "proj10",
    "hdfs_namenode_port" : 9000,
    "input" : "hdfs:///jasper/kdd12", # TODO: replace by your own data
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
