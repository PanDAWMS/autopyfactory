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
from autopyfactory.condorlib import querycondorlib
from autopyfactory.mappings import map2info

  
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
            self.maxage = apfqueue.fcl.generic_get('Factory', 'batchstatus.condor.maxage', default_value=360) 
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
        

        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORBATCHSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)

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
                self._updatelib()
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


###############################################################################
#               playing with the HTcondor python bindings
###############################################################################

    def _updatelib(self):
        """
        Query Condor for job status, and populate  object.
        It uses the condor python bindings.

        The JobStatus code indicates the current Condor status of the job.
        
                Value   Status                            
                0       U - Unexpanded (the job has never run)    
                1       I - Idle                                  
                2       R - Running                               
                3       X - Removed                              
                4       C -Completed                            
                5       H - Held                                 
                6       > - Transferring Output

        """

        self.log.trace('Starting.')
        self.log.debug('Starting.')

        if not utils.checkDaemon('condor'):
            self.log.error('condor daemon is not running. Doing nothing')
        else:
            try:
                # FIXME: the self.queryargs need to be decomposed into querycondorlib() input options
                strout = querycondorlib()
                self.log.debug('output of querycondorlib : ' %strout)
                if not strout:
                    self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.')
                else:
                    newinfo = map2info(strout, BatchStatusInfo(), self,jobstatus2info)
                    self.log.info("Replacing old info with newly generated info.")
                    self.currentinfo = newinfo
            except Exception, e:
                self.log.error("Exception: %s" % str(e))
                self.log.trace("Exception: %s" % traceback.format_exc())

        self.log.trace('Leaving.')



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


