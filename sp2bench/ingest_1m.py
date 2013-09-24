#!/usr/bin/env python

import subprocess

json = """{
  "relation_key" : {
    "user_name" : "public",
    "program_name" : "adhoc",
    "relation_name" : "sp2bench_1m"
  },
  "schema" : {
    "column_types" : ["STRING_TYPE", "STRING_TYPE", "STRING_TYPE"],
    "column_names" : ["subject", "predicate", "object"]
  },
  "file_name" : "file:///disk1/whitaker/sp2bench/sp2b/bin/sp2b_1m.txt"
}"""

subprocess.check_call(['curl', '-i', '-XPOST',
                       'vega.cs.washington.edu:1776/dataset', '-H',
                       'Content-type: application/json',  '-d',  json])
