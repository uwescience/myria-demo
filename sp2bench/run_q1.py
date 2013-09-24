#!/usr/bin/python

import subprocess
import time

import q1

plan = q1.generate()

subprocess.check_call(['curl', '-i', '-XPOST',
                       'vega.cs.washington.edu:1776/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
