#!/usr/bin/python

import subprocess
import sys

machines = ["dbserver%s.cs.washington.edu" % x for x in
            ["01", "05","12","13","14","15","17","18","19"]]

for machine in machines:
    cmd = ['ssh', machine]
    cmd.extend(sys.argv[1:])
    print ' '.join(cmd)
    subprocess.check_call(cmd)
    print
