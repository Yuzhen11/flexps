#!/usr/bin/env python

import os
from os.path import dirname, join

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
hostfile = "machinefiles/2node"
progfile = "debug/SparseSSPExample"

script_path = os.path.realpath(__file__)
proj_dir = dirname(dirname(script_path))
print "flexps porj_dir:", proj_dir
hostfile_path = join(proj_dir, hostfile)
prog_path = join(proj_dir, progfile)
print "hostfile_path:%s, prog_path:%s" % (hostfile_path, prog_path)

params = {
    "config_file" : hostfile_path,
    "kModelType" : "SparseSSP",
    # "kModelType" : "SSP",
    "num_dims" : 10,
    "num_nonzeros" : 1,
    "num_iters" : 100,
}

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    )

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  )

# TODO: May need to ls before run to make sure the related files are synced.
# clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null; ulimit -c unlimited; "
clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null;  "

with open(hostfile_path, "r") as f:
  hostlist = []  
  hostlines = f.read().splitlines()
  for line in hostlines:
    if not line.startswith("#"):
      hostlist.append(line.split(":"))

  for [node_id, host, port] in hostlist:
    print "node_id:%s, host:%s, port:%s" %(node_id, host, port)
    cmd = ssh_cmd + host + " "  # Start ssh command
    cmd += "\""  # Remote command starts
    cmd += clear_cmd
    # Command to run program
    cmd += env_params + " " + prog_path
    cmd += " --my_id="+node_id
    cmd += "".join([" --%s=%s" % (k,v) for k,v in params.items()])

    cmd += "\""  # Remote Command ends
    cmd += " &"
    print cmd
    os.system(cmd)
