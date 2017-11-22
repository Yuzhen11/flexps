#!/usr/bin/env python

import sys
from launch_utils import launch_util

hostfile = "machinefiles/5node"
progfile = "debug/HdfsManagerExample"

params = {
    "hdfs_namenode" : "proj10",
    "hdfs_namenode_port" : 9000,
    "input" : "hdfs:///jasper/SVHN",
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

launch_util(progfile, hostfile, env_params, params, sys.argv)
