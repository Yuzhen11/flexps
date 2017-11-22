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
# hostfile = "machinefiles/local"
hostfile = "machinefiles/5node"
progfile = "debug/BasicExample"

params = {
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
)

# use `python scripts/launch.py` to launch the distributed programs
# use `python scripts/launch.py kill` to kill the distributed programs
launch_util(progfile, hostfile, env_params, params, sys.argv)
