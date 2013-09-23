#!/usr/bin/env python

import subprocess

json = """{
  "relation_key" : {
    "user_name" : "public",
    "program_name" : "adhoc",
    "relation_name" : "nccdc_reachable_target"
  },
  "schema" : {
    "column_types" : ["STRING_TYPE"],
    "column_names" : ["addr"]
  },
  "file_name" : "file:///disk1/whitaker/nccdc/nccdc_reachable_target.txt"
}
"""

subprocess.check_call(['curl', '-i', '-XPOST',
                       'vega.cs.washington.edu:1776/dataset', '-H',
                       'Content-type: application/json',  '-d',  json])
