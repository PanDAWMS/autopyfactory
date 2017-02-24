#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#

import subprocess
import logging
import time
import threading
import traceback
import xml.dom.minidom

from autopyfactory.interfaces import WMSStatusInterface, _thread

from autopyfactory.info import CloudInfo
from autopyfactory.info import SiteInfo
from autopyfactory.info import JobInfo
from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import WMSQueueInfo

from autopyfactory.condor import checkCondor, parseoutput, aggregateinfo
from autopyfactory.condorlib import querycondorlib
from autopyfactory.mappings import map2info


class _condor(_thread, WMSStatusInterface):
    '''
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    '''

    def __init__(self, apfqueue, config, section):
        #try:
        _thread.__init__(self) 
        apfqueue.factory.threadsregistry.add("plugin", self)
        
        self.log = logging.getLogger("main.wmsstatusplugin[singleton created by %s]" %apfqueue.apfqname)
        self.log.debug('Initializing object...')

        self.apfqueue = apfqueue   
        self.apfqname = apfqueue.apfqname
        #self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
        #self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
        self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.condor.sleep')
        self._thread_loop_interval = self.sleeptime
        self.maxage = self.apfqueue.fcl.generic_get('Factory', 'wmsstatus.condor.maxage', default_value=360)
        self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queryargs')
        self.queueskey = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queueskey', default_value='MATCH_APF_QUEUE')


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



        # FIXME
        # check if this works with a Singleton, or I need a different Singleton per value

        self.currentcloudinfo = None
        self.currentjobinfo = None
        self.currentsiteinfo = None
              

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================
        
        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORWMSSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)
        ###self.jobstatus2info = {'0': 'ready',
        ###                       '1': 'ready',
        ###                       '2': 'running',
        ###                       '3': 'done',
        ###                       '4': 'done',
        ###                       '5': 'failed',
        ###                       '6': 'running'}

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        checkCondor()
        self.log.info('WMSStatusPlugin: Object initialized.')


    def _run(self):
        '''
        Main loop
        '''
        self.log.debug('Starting')
        self._update()
        self.log.debug('Leaving')


    def getInfo(self, queue=None, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a condor_q command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentjobinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentjobinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                return self.currentjobinfo[queue]                    
            else:
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentjobinfo))
                return self.currentjobinfo

    def getCloudInfo(self, cloud=None, maxtime=0):
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentcloudinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentcloudinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if cloud:
                return self.currentcloudinfo[queue]                    
            else:
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentcloudinfo))
                return self.currentcloudinfo


    def getSiteInfo(self, site=None, maxtime=0):
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentsiteinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentsiteinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if site:
                return self.currentsiteinfo[queue]                    
            else:
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentsiteinfo))
                return self.currentsiteinfo


    def _update(self):
        '''        
        Query Condor for job status, validate ?, and populate BatchStatusInfo object.
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

        self.log.debug('Starting.')
        
        # These are not meaningful for Local Condor as WMS
        self.currentcloudinfo = None
        self.currentsiteinfo = None

        try:

            #### BEGIN TEST ####
            #strout = querycondorlib(self.queryargs, self.queueskey)
            strout = querycondorlib(remotecollector=self.remotecollector, remoteschedd=self.remoteschedd, queueskey=self.queueskey)
            # FIXME: the extra_attributes is missing !!
            #### END TEST ####
            
            if not strout:
                self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
            else:
                newjobinfo = map2info(aggdict, WMSStatusInfo(), self.jobstatus2info)
                self.log.info("Replacing old info with newly generated info.")
                self.currentjobinfo = newjobinfo
        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        self.log.debug('_ Leaving.')




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
        if conf.generic_get(section, 'wmsstatusplugin') == 'Condor':
            queryargs = conf.generic_get(section, 'wmsstatus.condor.queryargs')
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
 
   



def test():
    list =  [ { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '2' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '1' },
                           { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '1' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '1' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '2' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' }
            ] 
    
if __name__=='__main__':
    pass



