#!/usr/bin/env python
"""
   Condor-related common utilities and library for AutoPyFactory.
   Focussed on properly processing output of condor_q -xml and condor_status -xml and converting
   to native Python data structures. 

"""
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

from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure
from autopyfactory.info import JobInfo
from autopyfactory import utils as utils

from pprint import pprint
from Queue import Queue

import htcondor
import classad
import copy


#############################################################################
#              APF interface to query methods 
#############################################################################

def querycondorlib(remotecollector=None, remoteschedd=None, extra_attributes=[], queueskey='match_apf_queue'):
    """ 
    queries condor to get a list of ClassAds objects
    We query for a few specific ClassAd attributes
    (faster than getting everything)

    remotecollector and remoteschedd
    are passed when querying a remote HTCondor pool 
    They are the equivalent to -pool and -name input
    options to CLI condor_q
    
    extra_attributes are classads needed other than 'jobstatus'
    """

    log = logging.getLogger('autopyfactory')
    log.debug("Starting with values remotecollector=%s, remoteschedd=%s, extra_attributes=%s, queueskey=%s" %(remotecollector, remoteschedd, extra_attributes, queueskey))

    list_attrs = [queueskey, 'jobstatus']
    list_attrs += extra_attributes
    out = condor_q(list_attrs, remotecollector, remoteschedd)
    ###out = _aggregateinfolib(out, queueskey, 'jobstatus') 
    from mappings import JobStatusAnalyzer
    jobstatusanalyzer = JobStatusAnalyzer()
    out = _aggregateinfolib(out, queueskey, [jobstatusanalyzer]) 
    log.debug(out)
    return out 


def condorhistorylib( attributes, constraints=[]):
    default_attributes=['match_apf_queue', 'jobstatus', 'enteredcurrentstatus', 'remotewallclocktime','qdate']
    for da in default_attributes:
        if da not in attributes:
            attributes.append(da)
    logging.debug('history called with attributes: %s' % attributes)
    return condor_history( attributes, constraints)


#############################################################################
#              condor python methods 
#FIXME:
#   if one day we want to make these methods a separately library
#   then most probably they cannot have an 'autopyfactory' logger
#############################################################################

def condor_q(attributes, remotecollector=None, remoteschedd=None):
    '''
    Returns a list of ClassAd objects. 
    '''
    # NOTE:
    # when remotecollector has a valid value, 
    # then remoteschedd must have a valid value too

    log = logging.getLogger('autopyfactory')
    log.debug("Starting with values attributes=%s, remotecollector=%s, remoteschedd=%s" %(attributes, remotecollector, remoteschedd))
    if remotecollector:
        # FIXME: to be tested
        log.debug("querying remote pool %s" %remotecollector)
        collector = htcondor.Collector(remotecollector)
        scheddAd = collector.locate(htcondor.DaemonTypes.Schedd, remoteschedd)
        schedd = htcondor.Schedd(scheddAd) 
    else:
        schedd = htcondor.Schedd() # Defaults to the local schedd.

    out = schedd.query('true', attributes)
    log.debug(out)
    return out

def condor_history( attributes, constraints):
    schedd = htcondor.Schedd()
    if len(constraints) > 1:
        condor_constraint_expr = " && ".join(constraints)
        history = schedd.history(condor_constraint_expr, attributes, 0)
    else:
        history = schedd.history( "true" , attributes, 0)
    history = list(history)
    return history


def condor_status():
    """ 
    Equivalent to condor_status
    We query for a few specific ClassAd attributes 
    (faster than getting everything)
    Output of collector.query(htcondor.AdTypes.Startd) looks like
     [
      [ Name = "slot1@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ], 
      [ Name = "slot2@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ]
     ]
    """
    # We only want to try to import if we are actually using the call...
    # Later on we will need to handle Condor version >7.9.4 and <7.9.4
    #
    collector = htcondor.Collector()
    list_attrs = ['Name', 'State', 'Activity']
    outlist = collector.query(htcondor.AdTypes.Startd, 'true', list_attrs)
    return outlist



#############################################################################
#              parse and aggregate outputs
#############################################################################


def _aggregateinfolib(input, primary_key='match_apf_queue', analyzers=[]):
    # input is a list of job classads
    # analyzers is a list of mappings.BaseAnalyzer objects
    # output is a dict[primary_key] [secondary_key] [value] = # of jobs with that value

    queues = {}

    for job in input:
        if not primary_key in job.keys():
            # This job is not managed by APF. Ignore...
            continue

        apfqname = str(job[primary_key])
        if apfqname not in queues.keys():
            queues[apfqname] = {}

        for analyzer in analyzers:
            label = analyzer.getlabel()
            if label not in queues[apfqname].keys():
                queues[apfqname][label] = {}
            value = analyzer.analyze(job)
            if value != None:
                if value not in queues[apfqname][label].keys():
                    queues[apfqname][label][value] = 0
                queues[apfqname][label][value] += 1

    return queues


def _aggregatehistoryinfolib(jobs, primary_key='match_apf_queue', queues=None, analyzers=[]):

    if not queues:
        queues = {}
    else:
        queues = queues

    for job in jobs:
        if not primary_key in job:
            continue

        apfqname = str(job[primary_key])
        if apfqname not in queues.keys():
            queues[apfqname] = {'total':0, 'short':0}

        for analyzer in analyzers:
            out = analyzer.analyze(job)
            if out is not None:
                queues[apfqname]['total'] += 1
                if out is False:
                    queues[apfqname]['short'] += 1

    return queues


def filtercondorhistorylib(history, constraints=[]):

    # contraints example ['JobStatus == 4', 'RemoteWallClockTime < 120']

    out = []
    for job in history:
        if _matches_constraints(job, constraints):
            out.append(job)
    return out

def _matches_constraints(ad, constraints):
    constraint_expression = " && ".join( ["TARGET." + i for i in constraints])
    return _matches_constraint_expr(ad, constraint_expression)


def _matches_constraint_expr(ad, constraint_expression):
    req_ad = classad.ClassAd()
    req_ad['Requirements'] = classad.ExprTree(constraint_expression)
    return ad.matches(req_ad)


# this is a temp solution to add running jobs to output of condor_history
def _aggregatecondorqinfolib(jobs, primary_key='match_apf_queue', queues=None, analyzers=[]):

    if not queues:
        queues = {}
    else:
        queues = queues

    for job in jobs:
        if not primary_key in job:
            continue


        apfqname = str(job[primary_key])
        if apfqname not in queues.keys():
            queues[apfqname] = {'total':0}

        for analyzer in analyzers:
            out = analyzer.analyze(job)
            if out is not None:
                if out is True:
                    queues[apfqname]['total'] += 1


    return queues




##############################################################################

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)
