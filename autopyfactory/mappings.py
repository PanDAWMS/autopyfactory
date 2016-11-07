#!/usr/bin/env python
'''
classes and methods to map structures of info
from one type to another
'''
import logging
from Queue import Queue

import autopyfactory.utils as utils
from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure
from autopyfactory.info import JobInfo


log = logging.getLogger('main.mappings')



def map2info(input, info_container, mappings):
    '''
    This takes aggregated info by queue, with condor/condor-g specific status totals, and maps them 
    to the backend-agnostic APF *StatusInfo object.
    
       APF             Condor-C/Local              Condor-G/Globus 
    .pending           Unexp + Idle                PENDING
    .running           Running                     RUNNING
    .suspended         Held                        SUSPENDED
    .done              Completed                   DONE
    .unknown           
    .error
    
    Primary attributes. Each job is in one and only one state:
        pending            job is queued (somewhere) but not running yet.
        running            job is currently active (run + stagein + stageout)
        error              job has been reported to be in an error state
        suspended          job is active, but held or suspended
        done               job has completed
        unknown            unknown or transient intermediate state
        
    Secondary attributes. Each job may be in more than one category. 
        transferring       stagein + stageout
        stagein
        stageout           
        failed             (done - success)
        success            (done - failed)
        ?
    
      The JobStatus code indicates the current status of the job.
        
                Value   Status
                0       Unexpanded (the job has never run)
                1       Idle
                2       Running
                3       Removed
                4       Completed
                5       Held
                6       Transferring Output

    Input:
      Dictionary of APF queues consisting of dicts of job attributes and counts.
      { 'UC_ITB' : { 'Jobstatus' : { '1': '17',
                                   '2' : '24',
                                   '3' : '17',
                                 },
                  }
       }          
    Output:
        A *StatusInfo object (a BatchStatusInfo(), or WMSStatusInfo())
        which maps attribute counts to generic APF
        queue attribute counts. 
    '''

    log.debug('Starting.')

    try:
        for site in input.keys():
            qi = info_container.default()
            info_container[site] = qi
            attrdict = input[site]
            valdict = attrdict['jobstatus']
            qi.fill(valdict, mappings)

    except Exception, ex:
        self.log.error("Exception: %s" % str(e))
        self.log.error("Exception: %s" % traceback.format_exc())
                    
    info_container.lasttime = int(time.time())
    log.debug('Returning %s: %s' % (info_container.__class__.__name__, info_container))
    for site in info_container.keys():
        log.debug('Queue %s = %s' % (site, info_container[site]))           

    return info_container 

