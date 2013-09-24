#!/usr/bin/python

import subprocess
import time

import q3a_1m

plan = q3a_1m.generate()

subprocess.check_call(['curl', '-i', '-XPOST',
                       'vega.cs.washington.edu:1776/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
