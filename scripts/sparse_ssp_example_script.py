#!/usr/bin/env python

import sys
from launch_utils import launch_util

hostfile = "machinefiles/5node"
progfile = "build/SparseSSPExample"

params = {
    "kStaleness" : 0,
    "kSpeculation" : 5,
    "kModelType" : "SSP",  # {ASP/SSP/BSP/SparseSSP}
    "kSparseSSPRecorderType" : "Vector",  # {Vector/Map}
    "num_dims" : 10000000,
    "num_nonzeros" : 10,
    "num_workers_per_node" : 3,
    "num_servers_per_node" : 1,
    "num_iters" : 1000,
    "with_injected_straggler" : 1,  # {0/1}
    "kStorageType" : "Vector",  # {Vector/Map}
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  )

launch_util(progfile, hostfile, env_params, params, sys.argv)
