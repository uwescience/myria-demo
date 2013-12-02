#!/usr/bin/env python

import subprocess

json = """{
  "relation_key" : {
    "user_name" : "public",
    "program_name" : "adhoc",
    "relation_name" : "sp2bench_100m"
  },
  "schema" : {
    "column_types" : ["STRING_TYPE", "STRING_TYPE", "STRING_TYPE"],
    "column_names" : ["subject", "predicate", "object"]
  },
  "file_name" : "hdfs://vega.cs.washington.edu:8020/datasets/sp2bench/sp2b_100m.txt"
}"""

subprocess.check_call(['curl', '-i', '-XPOST',
                       'vega.cs.washington.edu:1776/dataset', '-H',
                       'Content-type: application/json',  '-d',  json])
