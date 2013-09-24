#!/usr/bin/python

import subprocess
import time

import q1

plan = q1.generate()

subprocess.check_call(['curl', '-i', '-XPOST', 'localhost:8753/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
