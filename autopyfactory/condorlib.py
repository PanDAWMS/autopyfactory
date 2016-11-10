#!/usr/bin/env python
'''
   Condor-related common utilities and library for AutoPyFactory.
   Focussed on properly processing output of condor_q -xml and condor_status -xml and converting
   to native Python data structures. 

'''
import commands
import datetime
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import xml.dom.minidom

import autopyfactory.utils as utils
from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure

from pprint import pprint
from Queue import Queue

from autopyfactory.info import JobInfo

#############################################################################
#               using HTCondor python bindings
#############################################################################

import htcondor
import classad
import copy


def condorhistorylib():

    schedd = htcondor.Schedd()
    history = schedd.history('True', ['MATCH_APF_QUEUE', 'JobStatus', 'EnteredCurrentStatus', 'RemoteWallClockTime'], 0)
    return history


def filtercondorhistorylib(history, constraints=[]):

    # contraints example ['JobStatus == 4', 'RemoteWallClockTime < 120']

    out = []
    for job in history:
        if _matches_constraints(job, constraints):
            out.append(job)
    return out


    

def querycondorlib(remotecollector=None, remoteschedd=None, extra_attributes=[], queueskey='match_apf_queue'):
    ''' 
    queries condor to get a list of ClassAds objects
    We query for a few specific ClassAd attributes
    (faster than getting everything)

    remotecollector and remoteschedd
    are passed when querying a remote HTCondor pool 
    They are the equivalent to -pool and -name input
    options to CLI condor_q
    
    extra_attributes are classads needed other than 'jobstatus'
    '''

    log = logging.getLogger('main.condor')

    if remotecollector:
        # FIXME: to be tested
        log.debug("querying remote pool %s" %remotecollector)
        collector = htcondor.collector(remotecollector)
        scheddAd = collector.locate(condor.DaemonTypes.Schedd, remoteschedd)
        schedd = htcondor.Schedd(scheddAd) 
    else:
        schedd = htcondor.Schedd() # Defaults to the local schedd.

    list_attrs = [queueskey, 'jobstatus']
    list_attrs += extra_attributes
    out = schedd.query('true', list_attrs)
    out = _aggregateinfolib(out, queueskey, 'jobstatus') 
    log.trace(out)
    return out 


def _aggregateinfolib(input, primary_key='match_apf_queue', secondary_keys=[]):
    # input is a list of job classads
    # secondary_keys can be, for example: ['jobstatus']    
    # output is a dict[primary_key] [secondary_key] [value] = # of jobs with that value

    log = logging.getLogger('main.condor')

    queues = {}
    for job in input:
        if not primary_key in job.keys():
            # This job is not managed by APF. Ignore...
            continue
        apfqname = job[primary_key]
        if apfqname not in queues.keys():
            queues[apfqname] = {}
            for sk in secondary_keys:
                queues[apfqname][sk] = {}

        for sk in secondary_keys:
            value = str(job[sk])
            if value not in queues[apfqname][sk].keys():
                queues[apfqname][sk][value] = 0
            queues[apfqname][sk][value] += 1
    
    log.trace(queues)
    return queues


def querystatuslib():
    ''' 
    Equivalent to condor_status
    We query for a few specific ClassAd attributes 
    (faster than getting everything)
    Output of collector.query(htcondor.AdTypes.Startd) looks like

     [
      [ Name = "slot1@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ], 
      [ Name = "slot2@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ]
     ]
    '''
    # We only want to try to import if we are actually using the call...
    # Later on we will need to handle Condor version >7.9.4 and <7.9.4
    #

    collector = htcondor.Collector()
    list_attrs = ['Name', 'State', 'Activity']
    outlist = collector.query(htcondor.AdTypes.Startd, 'true', list_attrs)
    return outlist



def _matches_constraints(ad, constraints):
    constraint_expression = " && ".join( ["TARGET." + i for i in constraints])
    return _matches_constraint_expr(ad, constraint_expression)


def _matches_contraint_expr(ad, constraint_expression):
    req_ad = classad.ClassAd()
    req_ad['Requirements'] = classad.ExprTree(constraint_expression)
    return ad.matches(req_ad)



##############################################################################

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)
