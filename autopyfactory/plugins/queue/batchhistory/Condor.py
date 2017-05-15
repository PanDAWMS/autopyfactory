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
from autopyfactory.interfaces import BatchStatusInterface, _thread
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo

from autopyfactory.condor import checkCondor
from autopyfactory.condor import parseoutput, aggregateinfo
from autopyfactory.condorlib import condorhistorylib, _aggregatehistoryinfolib
from autopyfactory.mappings import FinishedAnalyzer

  
import autopyfactory.utils as utils


class __condor(_thread, BatchHistoryInterface):
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
        
        self.log = logging.getLogger('autopyfactory.batchhistory.%s' %apfqueue.apfqname)
        self.log.debug('Initializing object...')

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        
        try:
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId')
            self.maxage = apfqueue.fcl.generic_get('Factory', 'batchhistory.condor.maxage', default_value=360) 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchhistory.condor.sleep')
            self._thread_loop_interval = self.sleeptime
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


    def _run(self):
        """
        Main loop
        """
        self.log.debug('Starting')
        self._update()
        #self._updatelib()
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


    def _update(self):
        """        
        """

        self.log.debug('Starting.')

       
        if not utils.checkDaemon('condor'):
            self.log.error('condor daemon is not running. Doing nothing')
        else:
            try:
                jobs = condorhistorylib()
                now = int( time.time() )                
                # FIXME: this is just mock code !!!
                old = now - 15*60
                ###out = filtercondorhistorylib(out, ['JobStatus == 4', 'RemoteWallClockTime < 150', 'EnteredCurrentStatus > %s' %old])
                queues = _aggregatehistoryinfolib(jobs, 'match_apf_queue', None, [FinishedAnalyzer(self.interval, self.mintime)]
                   
            except Exception, e:
                self.log.error("Exception: %s" % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc())

        self.log.debug('Leaving.')



# ==========================================
#       singleton wrapper
# ==========================================

class Condor(object):

    instances = {}

    def __new__(cls, *k, **kw): 

        # ---------------------------------------------------------------------
        # get the ID
        apfqueue = k[0]
        conf = k[1]
        section = k[2]
        
        id = 'local'
        if conf.generic_get(section, 'batchhistoryplugin') == 'Condor':
            queryargs = conf.generic_get(section, 'batchhistory.condor.queryargs')
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
            Condor.instances[id] = __condor(*k, **kw)
        return Condor.instances[id]

     
