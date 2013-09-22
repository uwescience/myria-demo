#!/usr/bin/python

import subprocess
import time

import filter_query

plan = filter_query.generate()

subprocess.check_call(['curl', '-i', '-XPOST', 'localhost:1776/query', '-H',
                       'Content-type: application/json',  '-d',  plan])
