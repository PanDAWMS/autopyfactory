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
from autopyfactory.factory import BatchStatusInfo
from autopyfactory.factory import QueueInfo
from autopyfactory.factory import Singleton 

from autopyfactory.info import CloudInfo
from autopyfactory.info import SiteInfo
from autopyfactory.info import JobInfo
from autopyfactory.info import InfoContainer
from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import WMSQueueInfo

#from autopyfactory.condor import checkCondor, querycondor, querycondorxml, querycondorlib  
from autopyfactory.condor import checkCondor, querycondor, querycondorxml
from autopyfactory.condor import parseoutput, aggregateinfo

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorLocalWMSStatusPlugin(threading.Thread, WMSStatusInterface):
    '''
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each PandaQueue object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    '''
    
    __metaclass__ = Singleton 
    
    def __init__(self, apfqueue):
        #try:
        threading.Thread.__init__(self) # init the thread
        
        self.log = logging.getLogger("main.wmsstatusplugin[singleton created by %s]" %apfqueue.apfqname)
        self.log.debug('Initializing object...')
        self.stopevent = threading.Event()

        # to avoid the thread to be started more than once
        self.__started = False
        
        self.apfqueue = apfqueue   
        self.apfqname = apfqueue.apfqname
        #self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
        #self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
        self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.condor.sleep')
        self.currentinfo = None              

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================
        
        self.jobstatus2info = {'0': 'ready',
                               '1': 'ready',
                               '2': 'running',
                               '3': 'done',
                               '4': 'done',
                               '5': 'failed',
                               '6': 'running'}

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        checkCondor()
        self.log.info('WMSStatusPlugin: Object initialized.')

        #except Exception, ex:
        #    self.log.error("WMSStatusPlugin object initialization failed. Raising exception")
        #    raise ex


    def getInfo(self, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a condor_q command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('getInfo: Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.debug('getInfo: Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.debug('getInfo: Info too old. Leaving and returning None.')
            return None
        else:                    
            self.log.debug('getInfo: Leaving and returning info of %d entries.' % len(self.currentinfo))
            return self.currentinfo

    # temporary solution
    def getCloudInfo(self, maxtime=0):
    
        self.log.debug('getCloudInfo: Starting maxtime = %s' %maxtime)
        out = self.currentinfo.cloud
        self.log.info('getCloudInfo: Cloud has %d entries' % len(out))
        return out

    # temporary solution
    def getSiteInfo(self, maxtime=0):
    
        self.log.debug('getSiteInfo: Starting. maxtime = %s' %maxtime)
        out = self.currentinfo.site
        self.log.info('getSiteInfo: Siteinfo has %d entries' %len(out))
        return out

    # temporary solution
    def getJobsInfo(self, maxtime=0):
    
        self.log.debug('getSiteInfo: Starting. maxtime = %s' %maxtime)
        out = self.currentinfo.jobs
        self.log.info('getSiteInfo: Siteinfo has %d entries' %len(out))
        return out
    

    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.debug('start: Starting')

        if not self.__started:
                self.log.debug("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.debug('start: Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.debug('run: Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.debug("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.debug('run: Leaving')

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

        self.log.debug('_update: Starting.')
        
        try:
            strout = querycondor()
            outlist = parseoutput(strout)
            aggdict = aggregateinfo(outlist)
            newinfojobs = self._map2info(aggdict)

            # Info object
            #   The cloud and site parts are just empty (legacy code)
            newinfo = WMSStatusInfo()
            newinfo.cloud = InfoContainer('clouds', CloudInfo())
            newinfo.site = InfoContainer('sites', SiteInfo())
            newinfo.jobs = newinfojobs

            self.log.info("Replacing old info with newly generated info.")
            self.currentinfo = newinfo
        except Exception, e:
            self.log.error("_update: Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        self.log.debug('__update: Leaving.')


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
        self.log.debug('_map2info: Starting.')
        wmsstatusinfo = InfoContainer('jobs', WMSQueueInfo())
        for site in input.keys():
                qi = WMSQueueInfo()
                wmsstatusinfo[site] = qi
                attrdict = input[site]
               
                valdict = attrdict['jobstatus']
                qi.fill(valdict, mappings=self.jobstatus2info)
                        
        wmsstatusinfo.lasttime = int(time.time())
        self.log.debug('_map2info: Returning WMSStatusInfo: %s' % wmsstatusinfo)
        for site in wmsstatusinfo.keys():
            self.log.debug('_map2info: Queue %s = %s' % (site, wmsstatusinfo[site]))           
        return wmsstatusinfo


    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving')


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



