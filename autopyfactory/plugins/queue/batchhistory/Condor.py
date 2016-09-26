#!/bin/env python
#
# AutoPyfactory batch history plugin for Condor
#

import commands
import subprocess
import logging
import os
import sys
import time
import threading
import traceback
import xml.dom.minidom

from datetime import datetime
from pprint import pprint
from autopyfactory.interfaces import BatchStatusInterface
from autopyfactory.interfaces import Singleton, CondorSingleton
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo

from autopyfactory.condor import checkCondor, querycondor, querycondorxml
from autopyfactory.condor import parseoutput, aggregateinfo
from autopyfactory.condor import querycondorlib

  
import autopyfactory.utils as utils



class Condor(threading.Thread, BatchHistoryInterface):
    '''
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each PandaQueue object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    '''
    
    __metaclass__ = CondorSingleton 
    
    def __init__(self, apfqueue, **kw):

        threading.Thread.__init__(self) # init the thread
        
        self.log = logging.getLogger("main.batchhistoryplugin[singleton: %s condor_q_id: %s]" %(apfqueue.apfqname, kw['condor_q_id']))
        self.log.trace('Initializing object...')
        self.stopevent = threading.Event()

        # to avoid the thread to be started more than once
        self.__started = False

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        
        try:
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId')
            self.maxage = apfqueue.fcl.generic_get('Factory', 'batchhistory.condor.maxage', default_value=360) 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchhistory.condor.sleep')
            self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'batchhistory.condor.queryargs') 

        except AttributeError:
            self.condoruser = 'apf'
            self.facoryid = 'test-local'
            self.sleeptime = 10
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")
       
        

        self.currentinfo = None              

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================
        

        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORBATCHSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)


        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        checkCondor()
        self.log.info('BatchHistoryStatus: Object initialized.')




    def getInfo(self, queue=None):
        '''
        Returns a  object populated by the analysis 
        over the output of a condor_q command

        If the info recorded is older than that maxage,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.trace('Starting with self.maxage=%s' % self.maxage)
        
        if self.currentinfo is None:
            self.log.trace('Not initialized yet. Returning None.')
            return None
        elif self.maxage > 0 and (int(time.time()) - self.currentinfo.lasttime) > self.maxage:
            self.log.trace('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                self.log.trace('Current info is %s' % self.currentinfo)                    
                self.log.trace('Leaving and returning info of %d entries.' % len(self.currentinfo))
                return self.currentinfo[queue]
            else:
                self.log.trace('Current info is %s' % self.currentinfo)
                self.log.trace('No queue given, returning entire BatchStatusInfo object')
                return self.currentinfo



    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.trace('Starting')

        if not self.__started:
                self.log.trace("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.trace('Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.trace('Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
                #self._updatelib()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.trace("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.trace('Leaving')

    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.trace('Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.trace('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.trace('Leaving')



    def _update(self):
        '''        
        Query Condor for job status, validate ?, and populate  object.
        Condor-G query template example:
        
        condor_q -constr '(owner=="apf") && stringListMember("PANDA_JSID=BNL-gridui11-jhover",Environment, " ")'
                 -format 'jobStatus=%d ' jobStatus 
                 -format 'globusStatus=%d ' GlobusStatus 
                 -format 'gkUrl=%s' MATCH_gatekeeper_url
                 -format '-%s ' MATCH_queue 
                 -format '%s\n' Environment

        NOTE: using a single backslash in the final part of the 
              condor_q command '\n' only works with the 
              latest versions of condor. 
              With older versions, there are two options:
                      - using 4 backslashes '\\\\n'
                      - using a raw string and two backslashes '\\n'

        The JobStatus code indicates the current Condor status of the job.
        
                Value   Status                            
                0       U - Unexpanded (the job has never run)    
                1       I - Idle                                  
                2       R - Running                               
                3       X - Removed                              
                4       C -Completed                            
                5       H - Held                                 
                6       > - Transferring Output

        The GlobusStatus code is defined by the Globus GRAM protocol. Here are their meanings:
        
                Value   Status
                1       PENDING 
                2       ACTIVE 
                4       FAILED 
                8       DONE 
                16      SUSPENDED 
                32      UNSUBMITTED 
                64      STAGE_IN 
                128     STAGE_OUT 
        '''

        self.log.trace('Starting.')
       
        if not utils.checkDaemon('condor'):
            self.log.error('condor daemon is not running. Doing nothing')
        else:
            try:
                strout = querycondor(self.queryargs)
                if not strout:
                    self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
                else:
                    outlist = parseoutput(strout)
                    self.log.trace("Got outlist.")
                    aggdict = aggregateinfo(outlist)
                    self.log.trace("Got aggredated info.")
                    newinfo = self._map2info(aggdict)
                    self.log.trace("Got new batchstatusinfo object: %s" % newinfo)
                    self.log.info("Replacing old info with newly generated info.")
                    self.currentinfo = newinfo
            except Exception, e:
                self.log.error("Exception: %s" % str(e))
                self.log.trace("Exception: %s" % traceback.format_exc())
        self.log.trace('Leaving.')

