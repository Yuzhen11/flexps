#!/usr/bin/env python

import os, sys
from launch_utils import kill_nodes

if __name__ == "__main__":
  if len(sys.argv) != 3:
    print "usage: %s <hostfile> <prog_name>" % sys.argv[0]
    sys.exit(1)

  host_file = sys.argv[1]
  prog_name = sys.argv[2]
  kill_nodes(prog_name, host_file)

