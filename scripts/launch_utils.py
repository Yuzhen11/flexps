#!/usr/bin/env python

import os
import os.path
from os.path import dirname, join

ssh_cmd = (
  "ssh "
  "-o StrictHostKeyChecking=no "
  "-o UserKnownHostsFile=/dev/null "
)

def launch_nodes(prog_path, hostfile_path, env_params, params):
  assert os.path.isfile(prog_path)
  assert os.path.isfile(hostfile_path)

  clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null; "
  # use the following to allow core dumping
  # clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null; ulimit -c unlimited; "
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


def kill_nodes(prog_name, hostfile_path):
  assert os.path.isfile(hostfile_path)
  prog_name = prog_name.split("/")[-1]  # To prevent users give a path to prog
  print "Start killing <%s> according to <%s>" % (prog_name, hostfile_path)

  # Get host IPs
  with open(hostfile_path, "r") as f:
    hostlines = f.read().splitlines()
  host_ips = [line.split(":")[1] for line in hostlines]
  
  for ip in host_ips:
    cmd = ssh_cmd + ip + " killall -q " + prog_name
    os.system(cmd)
  print "Done killing <%s> for <%s>" % (prog_name, hostfile_path)


def parse_file(progfile, hostfile):
  script_path = os.path.realpath(__file__)
  proj_dir = dirname(dirname(script_path))
  print "flexps porj_dir:", proj_dir
  hostfile_path = join(proj_dir, hostfile)
  prog_path = join(proj_dir, progfile)
  print "hostfile_path:%s, prog_path:%s" % (hostfile_path, prog_path)
  return prog_path, hostfile_path


def launch_util(progfile, hostfile, env_params, params, argv):
  prog_path, hostfile_path = parse_file(progfile, hostfile)  
  if len(argv) == 1:
    params["config_file"] = hostfile_path
    launch_nodes(prog_path, hostfile_path, env_params, params)
  elif len(argv) == 2 and argv[1] == "kill":
    kill_nodes(prog_path, hostfile_path)
  else:
    print "arg error: " + str(argv)
