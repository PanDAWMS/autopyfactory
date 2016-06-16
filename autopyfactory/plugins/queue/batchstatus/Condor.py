#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
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



class Condor(threading.Thread, BatchStatusInterface):
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
        
        self.log = logging.getLogger("main.batchstatusplugin[singleton: %s condor_q_id: %s]" %(apfqueue.apfqname, kw['condor_q_id']))
        self.log.trace('BatchStatusPlugin: Initializing object...')
        self.stopevent = threading.Event()

        # to avoid the thread to be started more than once
        self.__started = False

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        
        try:
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchstatus.condor.sleep')
            self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs') 

        except AttributeError:
            self.condoruser = 'apf'
            self.facoryid = 'test-local'
            self.sleeptime = 10
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")
       
        

        self.currentinfo = None              

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================
        

        self.globusstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORBATCHSTATUS-GLOBUSSTATUS2INFO')
        self.log.info('globusstatus2info mappings are %s' %self.globusstatus2info)
        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORBATCHSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)

        ###self.globusstatus2info = {'1':   'pending',
        ###                          '2':   'running',
        ###                          '4':   'done',
        ###                          '8':   'done',
        ###                          '16':  'suspended',
        ###                          '32':  'pending',
        ###                          '64':  'pending',
        ###                          '128': 'running'}
        ###
        ###self.jobstatus2info = {'0': 'pending',
        ###                       '1': 'pending',
        ###                       '2': 'running',
        ###                       '3': 'done',
        ###                       '4': 'done',
        ###                       '5': 'suspended',
        ###                       '6': 'running'}


        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        checkCondor()
        self.log.info('BatchStatusPlugin: Object initialized.')




    def getInfo(self, queue=None, maxtime=0):
        '''
        Returns a  object populated by the analysis 
        over the output of a condor_q command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.trace('Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.trace('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.trace('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                self.log.trace('Current info is %s' % self.currentinfo)                    
                self.log.trace('Leaving and returning info of %d entries.' % len(self.currentinfo))
                return self.currentinfo[queue]
            else:
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
                    aggdict = aggregateinfo(outlist)
                    newinfo = self._map2info(aggdict)
                    self.log.info("Replacing old info with newly generated info.")
                    self.currentinfo = newinfo
            except Exception, e:
                self.log.error("Exception: %s" % str(e))
                self.log.trace("Exception: %s" % traceback.format_exc())
        self.log.trace('Leaving.')



    def _map2info(self, input):
        '''
        This takes aggregated info by queue, with condor/condor-g specific status totals, and maps them 
        to the backend-agnostic APF BatchStatusInfo object.
        
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
        Input:
          Dictionary of APF queues consisting of dicts of job attributes and counts.
          { 'UC_ITB' : { 'Jobstatus' : { '1': '17',
                                       '2' : '24',
                                       '3' : '17',
                                     },
                       'Globusstatus' : { '1':'13',
                                          '2' : '26',
                                          }
                      }
           }          
        Output:
            A  object which maps attribute counts to generic APF
            queue attribute counts. 
        '''


        self.log.trace('Starting.')
        batchstatusinfo = BatchStatusInfo()
        try:
            for site in input.keys():
                qi = QueueInfo()
                batchstatusinfo[site] = qi
                attrdict = input[site]
                
                # use finer-grained globus statuses in preference to local summaries, if they exist. 
                if 'globusstatus' in attrdict.keys():
                    valdict = attrdict['globusstatus']
                    qi.fill(valdict, mappings=self.globusstatus2info)
                # must be a local-only job or other. 
                else:
                    valdict = attrdict['jobstatus']
                    qi.fill(valdict, mappings=self.jobstatus2info)
        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.trace("Exception: %s" % traceback.format_exc()) 
                    
        batchstatusinfo.lasttime = int(time.time())
        self.log.trace('Returning : %s' % batchstatusinfo )
        for site in batchstatusinfo.keys():
            self.log.trace('Queue %s = %s' % (site, batchstatusinfo[site]))           
        return batchstatusinfo 


###############################################################################
#               playing with the HTcondor python bindings
###############################################################################

    def _updatelib(self):

        self.log.trace('Starting.')
        self.log.debug('Starting.')

        if not utils.checkDaemon('condor'):
            self.log.error('condor daemon is not running. Doing nothing')
        else:
            try:
                strout = querycondorlib()
                self.log.debug('output of querycondorlib : ' %strout)
                if not strout:
                    self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.')
                else:
                    newinfo = self._map2infolib(strout)
                    self.log.info("Replacing old info with newly generated info.")
                    self.currentinfo = newinfo
            except Exception, e:
                self.log.error("Exception: %s" % str(e))
                self.log.trace("Exception: %s" % traceback.format_exc())

        self.log.trace('Leaving.')



    def _map2infolib(self, input):
        '''
        devel version of _map2info( ) to process the output of condor query 
        methods using the python bindings.

        Also, assumes that no more globusstatus, only jobstatus
        is in the output 
        '''


        self.log.trace('Starting.')
        batchstatusinfo = BatchStatusInfo()
        for site in input.keys():
            qi = QueueInfo()
            batchstatusinfo[site] = qi
            attrdict = input[site]

            qi.fill(attrdict, mappings=self.jobstatus2info)

        batchstatusinfo.lasttime = int(time.time())
        self.log.trace('Returning : %s' % batchstatusinfo )
        for site in batchstatusinfo.keys():
            self.log.trace('Queue %s = %s' % (site, batchstatusinfo[site]))
        return batchstatusinfo



###############################################################################

def test1():
    from autopyfactory.test import MockAPFQueue
    
    a = MockAPFQueue('BNL_CLOUD-ec2-spot')
    bsp = CondorBatchStatusPlugin(a, condor_q_id='local')
    bsp.start()
    while True:
        try:
            time.sleep(15)
        except KeyboardInterrupt:
            bsp.stopevent.set()
            sys.exit(0)    


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test1()


