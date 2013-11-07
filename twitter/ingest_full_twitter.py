#!/usr/bin/env python

import subprocess

json = """{
  "relation_key" : {
    "user_name" : "public",
    "program_name" : "adhoc",
    "relation_name" : "Twitter"
  },
  "schema" : {
    "column_types" : ["INT_TYPE", "INT_TYPE"],
    "column_names" : ["followee", "follower"]
  },
  "file_name" : "hdfs://vega.cs.washington.edu:8020//datasets/twitter/twitter_rv.net"
}
"""

subprocess.check_call(['curl', '-i', '-XPOST', 'vega.cs.washington.edu:1776/dataset', '-H',
 'Content-type: application/json',  '-d',  json])
