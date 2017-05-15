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
from autopyfactory.interfaces import BatchStatusInterface, _thread
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo

from autopyfactory.condor import checkCondor, querycondor, querycondorxml
from autopyfactory.condor import parseoutput, aggregateinfo
from autopyfactory.condorlib import querycondorlib
from autopyfactory.mappings import map2info

  
import autopyfactory.utils as utils


class _condor(_thread, BatchStatusInterface):
    """
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each PandaQueue object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    """
    def __init__(self, apfqueue, config, section):

        _thread.__init__(self)
        apfqueue.factory.threadsregistry.add("plugin", self)
        
        self.log = logging.getLogger('autopyfactory.batchstatus.%s' %apfqueue.apfqname)
        self.log.debug('BatchStatusPlugin: Initializing object...')

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        
        try:
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId')
            self.maxage = apfqueue.fcl.generic_get('Factory', 'batchstatus.condor.maxage', default_value=360) 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchstatus.condor.sleep')
            self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs') 


            ### BEGIN TEST ###
            self.remoteschedd = None
            self.remotecollector = None
            if self.queryargs:
                l = self.queryargs.split()  # convert the string into a list
                if '-name' in l:
                    self.remoteschedd = l[l.index('-name') + 1]
                if '-pool' in l:
                    self.remotecollector = l[l.index('-pool') + 1]
            ### END TEST ###

            
        except AttributeError:
            self.condoruser = 'apf'
            self.facoryid = 'test-local'
            self.sleeptime = 10
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")

        self._thread_loop_interval = self.sleeptime
       

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


    def _run(self):
        """
        Main loop
        """
        self.log.debug('Starting')
        self._updatelib()
        self.log.debug('Leaving')


    def getInfo(self, queue=None):
        """
        Returns a  object populated by the analysis 
        over the output of a condor_q command

        If the info recorded is older than that maxage,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        """           
        self.log.debug('Starting with self.maxage=%s' % self.maxage)
        
        if self.currentinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif self.maxage > 0 and (int(time.time()) - self.currentinfo.lasttime) > self.maxage:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                self.log.debug('Current info is %s' % self.currentinfo)                    
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentinfo))
                return self.currentinfo[queue]
            else:
                self.log.debug('Current info is %s' % self.currentinfo)
                self.log.debug('No queue given, returning entire BatchStatusInfo object')
                return self.currentinfo






    ###############################################################################
    #   playing with the HTcondor python bindings
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

        self.log.debug('Starting.')
        self.log.debug('Starting.')

        try:
            #### BEGIN TEST ###
            #strout = querycondorlib()
            strout = querycondorlib(self.remotecollector, self.remoteschedd)
            # FIXME: do we need the extra_attributes ???
            # FIXME: do we need the queueskey ???
            #### END TEST ###


            self.log.debug('output of querycondorlib : ' %strout)
            if strout is None:
                self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.')
            else:
                newinfo = map2info(strout, BatchStatusInfo(), self.jobstatus2info)
                self.log.info("Replacing old info with newly generated info.")
                self.currentinfo = newinfo
        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())

        self.log.debug('Leaving.')


# =============================================================================
#       Singleton wrapper
# =============================================================================

class Condor(object):
   
    instances = {}

    def __new__(cls, *k, **kw): 

        # ---------------------------------------------------------------------
        # get the ID
        apfqueue = k[0]
        conf = k[1]
        section = k[2]
        
        id = 'local'
        if conf.generic_get(section, 'batchstatusplugin') == 'Condor':
            queryargs = conf.generic_get(section, 'batchstatus.condor.queryargs')
            if queryargs:
                l = queryargs.split()  # convert the string into a list
                                       # e.g.  ['-name', 'foo', '-pool', 'bar'....]
                name = ''
                pool = ''
        
                if '-name' in l:
                    name = l[l.index('-name') + 1]
                if '-pool' in l:
                    pool = l[l.index('-pool') + 1]
        
                if name == '' and pool == '':
                    id = 'local'
                else:
                    id = '%s:%s' %(name, pool)
        # ---------------------------------------------------------------------

        if not id in Condor.instances.keys():
            Condor.instances[id] = _condor(*k, **kw)
        return Condor.instances[id]



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


