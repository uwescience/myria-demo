#!/usr/bin/python

import subprocess
import time

import controller_query

plan = controller_query.generate()

subprocess.check_call(['curl', '-i', '-XPOST', 'localhost:8753/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
