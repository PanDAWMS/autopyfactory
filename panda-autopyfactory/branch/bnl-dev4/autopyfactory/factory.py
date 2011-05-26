#! /usr/bin/env python
#
# Simple(ish) python condor_g factory for panda pilots
#
# $Id: factory.py 7688 2011-04-08 22:15:52Z jhover $
#
#
#  Copyright (C) 2007,2008,2009,2010 Graeme Andrew Stewart
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

import commands
import grp
import logging
import os
import pprint
import pwd
import re
import string
import sys
import threading
import time

from autopyfactory.configloader import FactoryConfigLoader, QueueConfigLoader
from autopyfactory.exceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.monitor import Monitor

import userinterface.Client as Client

class Factory:
    '''
    
    
    '''
    def __init__(self, fcl):
        '''
        fconfig is a FactoryConfigLoader object. 
        
        '''
        self.log = logging.getLogger('main.factory')
        self.log.debug('Factory initializing...')
        self.fcl = fcl
        self.dryRun = fcl.config.get("Factory", "dryRun")
        self.cycles = fcl.config.get("Factory", "cycles")
        self.sleep = fcl.config.get("Factory", "sleep")
        self.log.debug("queueConf file(s) = %s" % fcl.config.get('Factory', 'queueConf'))
        self.qcl= QueueConfigLoader(fcl.config.get('Factory', 'queueConf').split(','))
        
        # Create all PandaQueue objects
        self.queues = []
        for section in self.qcl.config.sections():
            q = PandaQueue(self, section)
            self.queues.append(q)
        
        self.pandastatus = PandaStatus(self.fcl)
        # NOTE: I have removed out-commented code from here (JC)
        
        self.log.debug("Factory initialized.")

    def mainLoop(self):
        '''
        Main functional loop of overall Factory. 
        '''
        self.pandastatus.start()
        self.log.info("Starting all Queue threads...")
        for q in self.queues:
            q.start()
        
        try:
            while True:
                time.sleep(10)
                self.log.debug('Checking for interrupt.')
                
        except (KeyboardInterrupt): 
            logging.info("Shutdown via Ctrl-C or -INT signal.")
            logging.debug(" Shutting down all threads...")
            
            self.log.info("Joining all Queue threads...")
            for q in self.queues:
                q.join()
            
            self.log.info("All Queue threads joined. Exitting.")
                
class PandaQueue(threading.Thread):
    '''
    Encapsulates all the functionality related to servicing each Panda queue (i.e. siteid, i.e. site).
    
    
    '''
    
    def __init__(self, factory, siteid ):
        '''
        factory is the parent factory
        qcl is a QueueConfigLoader object
        siteid is the name of the section in the queueconfig
        
        '''
        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.pandaqueue')
        self.stopevent = threading.Event()
        self.factory = factory       # Factory object that is the parent of this queue object. 
        self.fcl = factory.fcl       # FactoryConfigLoader for this factory
        self.qcl = factory.qcl       # Queue config object for this queue object.
        self.siteid = siteid         # Queue section designator from config
        self.nickname = self.qcl.config.get(siteid, "nickname")
        self.dryrun = self.fcl.config.get("Factory", "dryRun")
        self.cycles = self.fcl.config.get("Factory", "cycles" )
        self.sleep = int(self.fcl.config.get("Factory", "sleep"))
        self.cyclesrun = 0
        
        # Handle sched plugin
        schedclass = self.qcl.config.get(self.siteid, "schedplugin")
        self.log.debug("[%s] Attempting to import derived classname: autopyfactory.plugins.%sSchedPlugin.%sSchedPlugin" % (self.siteid,schedclass,schedclass))                
        _temp = __import__("autopyfactory.plugins.%sSchedPlugin" % (schedclass), 
                                 fromlist=["%sSchedPlugin" % schedclass])
        SchedPlugin = _temp.SchedPlugin
        self.scheduler = SchedPlugin()
        
        # Handle status and submit batch plugins. 
        batchclass = self.qcl.config.get(self.siteid, "batchplugin")
        
        _temp =  __import__("autopyfactory.plugins.%sBatchPlugin" % batchclass, fromlist=["%sBatchPlugin" % batchclass])
        BatchStatusPlugin = _temp.BatchStatusPlugin
        self.batchstatus = BatchStatusPlugin(self)

        BatchSubmitPlugin = _temp.BatchSubmitPlugin
        self.batchsubmit = BatchSubmitPlugin(self)
        self.log.debug("[%s] PandaQueue initialization done." % self.siteid)
        
        
    def run(self):
        '''
        Method called by thread.start()
        Main functional loop of this Queue. 
        '''    
        while not self.stopevent.isSet():
            self.log.debug("[%s] Would be grabbing Batch info relevant to this queue." % self.siteid)
            # update batch info
            #batchstatus = self.factory.batchstatus.getInfo()
            # update panda info
            self.log.debug("[%s] Would be getting panda info relevant to this queue."% self.siteid)
            
            self.log.debug("[%s] Would be calculating number to submit."% self.siteid)
            # calculate number to submit
            #nsub = self.scheduler.calcSubmitNum()
            # submit using this number
            self.log.debug("[%s] Would be submitting jobs for this queue."% self.siteid)
            #self.submitPilots(nsub)
            # Exit loop if desired number of cycles is reached...  
            self.log.debug("[%s] Checking to see how many cycles to run."% self.siteid)
            if self.cycles and self.cyclesrun >= self.cycles:
                self.stopevent.set()            
            self.log.debug("[%s] Incrementing cycles..."% self.siteid)
            self.cyclesrun += 1
            # sleep interval
            self.log.debug("[%s] Sleeping for %d seconds..." % (self.siteid, self.sleep))
            time.sleep(self.sleep)
              
    def join(self,timeout=None):
        """
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        """
        self.stopevent.set()
        self.log.debug('[%s] Stopping thread...' % self.siteid )
        threading.Thread.join(self, timeout)
         

    def submitPilots(self):
        
        for queue in self.config.queueKeys:
             queueParameters = self.config.queues[queue]
    
             # Check to see if a site or cloud is offline or in an error state
             if queueParameters['cloud'] in self.pandaCloudStatus and self.pandaCloudStatus[queueParameters['cloud']]['status'] == 'offline':
                 self.log.info('Cloud %s containing queue %s: is offline - will not submit pilots.' % (queue, queueParameters['cloud']))
                 continue
                 
             if queueParameters['status'] == 'offline':
                 self.log.info('Site %s containing queue %s: is offline - will not submit pilots.' % (queue, queueParameters['site']))
                 continue
                 
             if queueParameters['status'] == 'error':
                 self.log.info('Site %s containing queue %s: is in an error state - will not submit pilots.' % (queue, queueParameters['site']))
                 continue
                 
             # Check to see if the cloud is in test mode
             if queueParameters['cloud'] in self.pandaCloudStatus and self.pandaCloudStatus[queueParameters['cloud']]['status'] == 'test':
                 self.log.info('Cloud %s containing queue %s: is in test mode.' % (queue, queueParameters['cloud']))
                 cloudTestStatus = True
             else:
                 cloudTestStatus = False
                 
                 
             # Now normal queue submission algorithm begins
             if queueParameters['pilotlimit'] != None and queueParameters['pilotQueue']['total'] >= queueParameters['pilotlimit']:
                 self.log.info('%s: reached pilot limit %d (%s) - will not submit more pilots.', 
                                           queue, queueParameters['pilotlimit'], queueParameters['pilotQueue'])
                 continue
    
             if queueParameters['transferringlimit'] != None and 'transferring' in queueParameters['pandaStatus'] and \
                     queueParameters['pandaStatus']['transferring'] >= queueParameters['transferringlimit']:
                 self.log.info('%s: too many transferring jobs (%d > limit %d) - will not submit more pilots.', 
                                           queue, queueParameters['pandaStatus']['transferring'], queueParameters['transferringlimit'])
                 continue
    
             if queueParameters['status'] == 'test' or cloudTestStatus == True:
                 # For test sites only ever have one pilot queued, but allow up to nqueue to run
                 if queueParameters['pilotQueue']['inactive'] > 0 or queueParameters['pilotQueue']['total'] > queueParameters['nqueue']:
                     self.log.info('%s: test site has %d pilots, %d queued. Doing nothing.',
                                               queue, queueParameters['pilotQueue']['total'], queueParameters['pilotQueue']['inactive'])
                 else:
                     self.log.info('%s: test site has %d pilots, %d queued. Will submit 1 testing pilot.',
                                               queue, queueParameters['pilotQueue']['total'], queueParameters['pilotQueue']['inactive'])
                     self.condorPilotSubmit(queue, cycleNumber, 1)
                 continue
    
             # Production site, online - look for activated jobs and ensure pilot queue is topped up, or
             # submit some idling pilots
             if queueParameters['pandaStatus']['activated'] > 0:
                 # Activated jobs at this site
                 if queueParameters['depthboost'] == None:
                     self.log.info('Depth boost unset for queue %s - defaulting to 2' % queue)
                     depthboost = 2
                 else:
                     depthboost = queueParameters['depthboost']
                 if queueParameters['pilotQueue']['inactive'] < queueParameters['nqueue'] or \
                         (queueParameters['pandaStatus']['activated'] > queueParameters['pilotQueue']['inactive'] and \
                          queueParameters['pilotQueue']['inactive'] < queueParameters['nqueue'] * depthboost):
                     self.log.info('%s: %d activated jobs, %d inactive pilots queued (< queue depth %d * depth boost %d). Will submit full pilot load.',
                                               queue, queueParameters['pandaStatus']['activated'], 
                                               queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'], depthboost)
                     self.condorPilotSubmit(queue, cycleNumber, queueParameters['nqueue'])
                 else:
                     self.log.info('%s: %d activated jobs, %d inactive pilots queued (>= queue depth %d * depth boost %d). No extra pilots needed.',
                                               queue, queueParameters['pandaStatus']['activated'],
                                               queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'], depthboost)
                 continue
    
             # No activated jobs - send an idling pilot if there are less than queue depth pilots
             # and we are not in a suppressed cycle for this queue (so avoid racking up too many idleing jobs)
             if queueParameters['pilotQueue']['inactive'] < queueParameters['nqueue']:
                 if queueParameters['idlepilotsuppression'] > 1 and cycleNumber % queueParameters['idlepilotsuppression'] != 0:
                     self.log.info('%s: No activated jobs, %d inactive pilots queued (queue depth %d). This factory cycle supressed (%d mod %d != 0).',
                                               queue, queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'],
                                               cycleNumber, queueParameters['idlepilotsuppression'])
                 else:
                     self.log.info('%s: No activated jobs, %d inactive pilots queued (queue depth %d). Will submit 1 idling pilot.',
                                               queue, queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'])
                     self.condorPilotSubmit(queue, cycleNumber, 1)
             else:
                 self.log.info('%s: No activated jobs, %d inactive pilots queued (queue depth %d). No extra pilots needed.',
                                           queue, queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'])





class PandaStatus(threading.Thread):
    '''
    Contains all information pulled from Panda (schedconfig/job statistics/cloudspecs). 
    Encapsulates all understanding needed to interpret data returned from Panda. 
    Runs every <pandaCheckInterval> seconds. 
    '''  
    def __init__(self, fcl):
        '''
        fcl is the FactoryConfigLoader for this factory. 
        
        '''
        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.pandastatus')
        self.fcl = fcl
        self.interval = int(self.fcl.config.get('Factory','pandaCheckInterval'))
        self.stopevent = threading.Event()
        # Hold return value of getCloudSpecs
        self.cloudconfig = None 
        self.newcloudconfig = None

        self.siteconfig = None
        self.newsiteconfig = None
        
        # Holds return vlue of getJobStatisticsPerSite(countryGroup='',workingGroup='')
        self.jobstats = None
        self.newjobstats = None
        
    def join(self,timeout=None):
        """
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        """
        self.stopevent.set()
        self.log.debug('Stopping PandaStatus thread...')
        threading.Thread.join(self, timeout)
    
    def run(self):
        '''
        Run the thread (from start()). 
        '''
        while not self.stopevent.isSet():
            self.log.info("Starting PandaStatus update cycle...")
            self.updateCloudConfig()
            self.updateSiteConfig()
            self.updateJobStats()
            self.log.debug("Finished update loop. Sleeping %d seconds..." % self.interval)
            time.sleep(self.interval)
        
      
    def updateCloudConfig(self):
        self.log.info('Polling Panda for cloud status...')
        error,self.newcloudconfig = Client.getCloudSpecs()
        if error != 0:
            raise PandaStatusFailure, 'Client.getCloudSpecs() error: %s' % (error)
        #self.log.debug("Got new cloud config: %s" % pprint.pformat(self.newcloudconfig))
        self.log.debug("Got new cloud config with entries for %d clouds" % len(self.newcloudconfig))
        self.cloudconfig = self.newcloudconfig
    
    
    def updateJobStats(self):
        self.log.info('Polling Panda for Job statistics')
        error, self.newjobstats = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')
        if error != 0:
            raise PandaStatusFailure, 'Client.getJobStatisticsPerSite() error: %s' % (error)
        #self.log.debug("Got new jobstats: %s" % pprint.pformat(self.newjobstats))
        self.log.debug("Got new jobstats with entries for %d sites: " % len(self.newjobstats))
        self.jobstats = self.newjobstats
    
    def updateSiteConfig(self):
        self.log.info('Polling Panda for Site Configuration')
        error, self.newsiteconfig = Client.getSiteSpecs(siteType='all')
        if error != 0:
            raise PandaStatusFailure, 'Client.getSiteSpecs() error: %s' % (error)
        #self.log.debug("Got new site config: %s" % pprint.pformat(self.newsiteconfig))
        self.log.debug("Got new site configs for %d sites" % len(self.newsiteconfig))
        self.siteconfig = self.newsiteconfig
    

class BatchStatus(object):
    '''
    Batch-agnostic aggregate information container returned by the BatchStatus getInfo() call. 
    
    ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD   
    
    '''
    def _init__(self, queue, jobid, owner, submittime, runtime, state, priority  ):
        self.queue = queue
        
class JobStatus(object):
    '''
    Batch agnostic information container about particular job. Returned by getJobInfo() call 
    
    '''


####################################################################################
#           Interface definitions, for clarity in plugin programming. 
####################################################################################

class BatchStatusInterface(object):
    '''
    Interacts with the underlying batch system to get job status. 
    Instantiated at the Factory level. 
    Should return information about number of jobs currently on the desired queue. 
    
    '''
    def getInfo(self, queue):
        '''
        Returns aggregate info about jobs on queue in batch system. 
        '''
        raise NotImplementedError
    
    def getJobInfo(self, queue):
        '''
        Returns a list of JobStatus objects, one for each job. 
        '''
        raise NotImplementedError
    
class BatchSubmitInterface(object):
    '''
    Interacts with underlying batch system to submit jobs. 
    It should be instantiated one per queue. 
    
    '''
    def submitPilots(self, number):
        '''
        
        '''
        raise NotImplementedError

class SchedInterface(object):
    '''
    Calculates the number of jobs to submit for a queue. 
    
    ''' 

    def calcSubmitNum(self, config, activated, failed, running, transferring):
        '''
        Calculates and exact number of new pilots to submit, based on provided Panda site info
        and whatever relevant parameters are in config.
        All Panda info, not all relevant:    
        'activated': 0,
        'assigned': 0,
        'cancelled': 0,
        'defined': 0,
        'failed': 4
        'finished': 493,
        'holding' : 3,
        'running': 18,
        'transferring': 38},
        '''
        raise NotImplementedError

# ------------------------------------------------------------------------------ 
#                       T E S T S 
# ------------------------------------------------------------------------------ 

def testPandaRetrieve():
    import pprint
    error, cloudconfig = Client.getCloudSpecs()
    print("Error: %s" % error)
    pprint.pprint(cloudconfig)
    error, jobstats = Client.getJobStatisticsPerSite(countryGroup='',workingGroup='')
    print("Error: %s" % error)
    pprint.pprint(jobstats)    
    error, siteconfig = Client.getSiteSpecs(siteType='all')
    print("Error: %s" % error)
    pprint.pprint(siteconfig)

if __name__ == "__main__":
    testPandaRetrieve()
    

        
