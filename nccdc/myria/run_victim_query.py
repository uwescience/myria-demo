#!/usr/bin/python

import subprocess
import time

import victim_query

plan = victim_query.generate()

subprocess.check_call(['curl', '-i', '-XPOST', 'vega.cs.washington.edu:1776/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
