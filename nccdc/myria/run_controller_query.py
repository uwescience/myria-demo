#!/usr/bin/python

import subprocess
import time

import controller_query

plan = controller_query.generate()

subprocess.check_call(['curl', '-i', '-XPOST', 'localhost:1776/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
