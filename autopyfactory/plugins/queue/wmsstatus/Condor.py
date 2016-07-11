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

from autopyfactory.interfaces import WMSStatusInterface
from autopyfactory.interfaces import Singleton, CondorSingleton

from autopyfactory.info import CloudInfo
from autopyfactory.info import SiteInfo
from autopyfactory.info import JobInfo
from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import WMSQueueInfo

#from autopyfactory.condor import checkCondor, querycondor, querycondorxml, querycondorlib  
from autopyfactory.condor import checkCondor, querycondor, querycondorxml
from autopyfactory.condor import parseoutput, aggregateinfo


class Condor(threading.Thread, WMSStatusInterface):
    '''
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    '''
   
    __metaclass__ = CondorSingleton 
    
    def __init__(self, apfqueue, **kw):
        #try:
        threading.Thread.__init__(self) # init the thread
        
        self.log = logging.getLogger("main.wmsstatusplugin[singleton created by %s with condor_q_id: %s]" %(apfqueue.apfqname, kw['condor_q_id']))
        self.log.debug('Initializing object...')
        self.stopevent = threading.Event()

        # to avoid the thread to be started more than once
        self.__started = False
        
        self.apfqueue = apfqueue   
        self.apfqname = apfqueue.apfqname
        #self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
        #self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
        self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.condor.sleep')
        self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queryargs')
        self.queueskey = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queueskey', default_value='MATCH_APF_QUEUE')
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

  
    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.debug('Starting')

        if not self.__started:
                self.log.debug("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.debug('Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.debug('Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.debug("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.debug('Leaving')

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
            strout = querycondor(self.queryargs, self.queueskey)
            if not strout:
                self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
            else:
                outlist = parseoutput(strout)
                aggdict = aggregateinfo(outlist, self.queueskey)
                newjobinfo = self._map2info(aggdict)
                self.log.info("Replacing old info with newly generated info.")
                self.currentjobinfo = newjobinfo
        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        self.log.debug('_ Leaving.')


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
            A BatchStatusInfo object which maps attribute counts to generic APF
            queue attribute counts. 
        '''
        self.log.debug('Starting.')
        wmsstatusinfo = WMSStatusInfo()
        for site in input.keys():
                qi = WMSQueueInfo()
                wmsstatusinfo[site] = qi
                attrdict = input[site]
                valdict = attrdict['jobstatus']
                qi.fill(valdict, mappings=self.jobstatus2info)
                        
        wmsstatusinfo.lasttime = int(time.time())
        self.log.debug('Returning WMSStatusInfo: %s' % wmsstatusinfo)
        for site in wmsstatusinfo.keys():
            self.log.debug('Queue %s = %s' % (site, wmsstatusinfo[site]))           
        return wmsstatusinfo


    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('Leaving')


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



