#!/usr/bin/env python

import subprocess

json = """{
  "relation_key" : {
    "user_name" : "public",
    "program_name" : "adhoc",
    "relation_name" : "nccdc"
  },
  "schema" : {
    "column_types" : ["STRING_TYPE", "STRING_TYPE", "INT_TYPE", "INT_TYPE",
                      "INT_TYPE", "INT_TYPE", "INT_TYPE"],
    "column_names" : ["src", "dst", "proto", "time", "col4", "col5", "col6"]
  },
  "file_name" : "hdfs://vega.cs.washington.edu:8020/datasets/nccdc/nccdc_edges.txt",
  "delimiter" : "|"
}
"""

subprocess.check_call(['curl', '-i', '-XPOST', 'vega.cs.washington.edu:1776/dataset', '-H',
 'Content-type: application/json',  '-d',  json])
