#!/usr/bin/env python 

"""
quick and dirty script to check if there are pilots that 
have been running (in theory) for more than 8 days
"""

import commands
import time

pilots = commands.getoutput('condor_q -format " MATCH_APF_QUEUE=%s" match_apf_queue -format " JobStatus=%d " jobstatus -format " EnteredCurrentStatus=%s " EnteredCurrentStatus   -format "%d" ClusterId -format  ".%d\n" ProcId')


eightdays = 3600*24*8
currenttime = time.time()

for pilot in pilots.split('\n'):
    fields = pilot.split()
    if fields[1] == "JobStatus=2":
        #print pilot
        startedrunning = fields[2].split('=')[1]
        #print startedrunning
        #print int(currenttime) - int(startedrunning)
        delta = int(currenttime) - int(startedrunning)
        if delta > eightdays:
            print pilot

