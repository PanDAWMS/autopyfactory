#! /usr/bin/env python
#
# Simple(ish) python condor_g factory for panda pilots
#
# $Id$
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

import os
import sys
import logging
import commands
import time
import string
import re
import threading

import os
import pwd
import grp

from autopyfactory.exceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.configloader import FactoryConfigLoader
from autopyfactory.monitor import Monitor

import userinterface.Client as Client

class Factory:
    
    def __init__(self, config):
        self.log = logging.getLogger('main.factory')
        self.log.debug('Factory initializing...')
        self.config = config
        self.dryRun = config.get("Factory", "dryRun")
        self.cycles = config.get("Factory", "cycles")
        self.sleepinterval = config.get("Factory", "sleep")
        self.qconfig = QueueConfigLoader(config.get('Factory', 'queueConf'))
        self.queues = []
        for section in self.qconfig.sections():
            q = PandaQueue(self, self.qconfig, section)
            self.queues.append(q)
        
        try:
            args = dict(self.config.items('Factory'))
            args.update(self.config.items('Pilots'))
            # pass through all Factory and Pilots config items to Monitor
            self.monitor = Monitor(**args)
        except:
            self.log.warn('Monitoring not configured')
        # Set up Panda status
        self.pandastatus = PandaStatus(config)
                
        
        # Handle batch plugin
        batchclass = self.config.get("Factory", "batchplugin")
        BatchStatusPlugin = __import__("autopyfactory.plugins.%s" % batchclass)
        self.batchplugin = BatchPlugin()
        
        
        
        
        
        self.log.debug("Factory initialized.")

    def mainLoop(self):
        '''
        Main functional loop of overall Factory. 
        '''
        self.condorstatus.start()
        self.pandastatus.start()
        
        cyclesDone = 0
        while True:
            self.log.info('\nStarting factory cycle %d at %s', cyclesDone, time.asctime(time.localtime()))
            self.factorySubmitCycle(cyclesDone)
            self.log.info('Factory cycle %d done' % cyclesDone)
            cyclesDone += 1
            if cyclesDone == self.config.get("options","cyclesToDo"):
                break
            self.log.info('Sleeping %ds' % options.sleepTime)
            time.sleep(options.sleepTime)
            f.updateConfig(cyclesDone)

    def note(self, queue, msg):
        self.log.info('%s: %s' % (queue,msg))
        if isinstance(self.mon, Monitor):
            nick = self.config.queues[queue]['nickname']
            self.mon.msg(nick, queue, msg)

   


    def submitPilots(self, cycleNumber=0):
        for queue in self.config.queueKeys:
            queueParameters = self.config.queues[queue]
 
            # Check to see if a site or cloud is offline or in an error state
            if queueParameters['cloud'] in self.pandaCloudStatus and self.pandaCloudStatus[queueParameters['cloud']]['status'] == 'offline':
                msg = 'Cloud %s is offline - will not submit pilots.' % queueParameters['cloud']
                self.note(queue, msg)
                continue
                
            if queueParameters['status'] == 'offline':
                msg = 'Site %s is offline - will not submit pilots.' % queueParameters['siteid']
                self.note(queue, msg)
                continue
                
            if queueParameters['status'] == 'error':
                msg = 'Site %s is in an error state - will not submit pilots.' % queueParameters['siteid']
                self.note(queue, msg)
                continue
                
            # Check to see if the cloud is in test mode
            if queueParameters['cloud'] in self.pandaCloudStatus and self.pandaCloudStatus[queueParameters['cloud']]['status'] == 'test':
                msg = 'Cloud %s in test mode.' % queueParameters['cloud']
                self.note(queue, msg)
                cloudTestStatus = True
            else:
                cloudTestStatus = False
                
                
            # Now normal queue submission algorithm begins
            if queueParameters['pilotlimit'] != None and queueParameters['pilotQueue']['total'] >= queueParameters['pilotlimit']:
                msg = 'reached pilot limit %d (%s) - will not submit more pilots.' % (queueParameters['pilotlimit'], queueParameters['pilotQueue'])
                self.note(queue, msg)
                continue

            if queueParameters['transferringlimit'] != None and 'transferring' in queueParameters['pandaStatus'] and \
                    queueParameters['pandaStatus']['transferring'] >= queueParameters['transferringlimit']:
                msg = 'too many transferring jobs (%d > limit %d) - will not submit more pilots.' % (queueParameters['pandaStatus']['transferring'], queueParameters['transferringlimit'])
                self.note(queue, msg)
                continue

            if queueParameters['status'] == 'test' or cloudTestStatus == True:
                # For test sites only ever have one pilot queued, but allow up to nqueue to run
                if queueParameters['pilotQueue']['inactive'] > 0 or queueParameters['pilotQueue']['total'] > queueParameters['nqueue']:
                    msg = 'test site has %d pilots, %d queued. Doing nothing.' % (queueParameters['pilotQueue']['total'], queueParameters['pilotQueue']['inactive'])
                    self.note(queue, msg)
                else:
                    msg = 'test site has %d pilots, %d queued. Will submit 1 testing pilot.' % (queueParameters['pilotQueue']['total'], queueParameters['pilotQueue']['inactive'])
                    self.note(queue, msg)
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
                    msg = '%d activated jobs, %d inactive pilots queued (< queue depth %d * depth boost %d). Will submit full pilot load.' % (queueParameters['pandaStatus']['activated'], queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'], depthboost)
                    self.note(queue, msg)
                    self.condorPilotSubmit(queue, cycleNumber, queueParameters['nqueue'])
                else:
                    msg = '%d activated jobs, %d inactive pilots queued (>= queue depth %d * depth boost %d). No extra pilots needed.' % (queueParameters['pandaStatus']['activated'],queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'], depthboost)
                    self.note(queue, msg)
                continue

            # No activated jobs - send an idling pilot if there are less than queue depth pilots
            # and we are not in a suppressed cycle for this queue (so avoid racking up too many idleing jobs)
            if queueParameters['pilotQueue']['inactive'] < queueParameters['nqueue']:
                if queueParameters['idlepilotsuppression'] > 1 and cycleNumber % queueParameters['idlepilotsuppression'] != 0:
                    msg = 'No activated jobs, %d inactive pilots queued (queue depth %d). This factory cycle supressed (%d m od %d != 0).' % (queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'],cycleNumber, queueParameters['idlepilotsuppression'])
                    self.note(queue, msg)
                else:
                    msg = 'No activated jobs, %d inactive pilots queued (queue depth %d). Will submit 1 idling pilot.' % (queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'])
                    self.note(queue, msg)
                    self.condorPilotSubmit(queue, cycleNumber, 1)
            else:
                msg = 'No activated jobs, %d inactive pilots queued (queue depth %d). No extra pilots needed.' % (queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'])
                self.note(queue, msg)



            

  

    def getPandaStatus(self):
        for country in self.config.sites.keys():
            for group in self.config.sites[country].keys():
                # country/group = None is equivalent to not specifing anything
                self.log.info('Polling panda status for country=%s, group=%s' % (country, group,))
                error,self.config.sites[country][group]['siteStatus'] = Client.getJobStatisticsPerSite(countryGroup=country,workingGroup=group)
                if error != 0:
                    raise PandaStatusFailure, 'Client.getJobStatisticsPerSite(countryGroup=%s,workingGroup=%s) error: %s' % (country, group, error)

                for siteid, queues in self.config.sites[country][group].iteritems():
                    if siteid == 'siteStatus':
                        continue
                    if siteid in self.config.sites[country][group]['siteStatus']:
                        self.log.debug('Panda status: %s (country=%s, group=%s) %s' % (siteid, country, group, self.config.sites[country][group]['siteStatus'][siteid]))
                        for queue in queues:
                            self.config.queues[queue]['pandaStatus'] = self.config.sites[country][group]['siteStatus'][siteid]
                    else:
                        # If panda knows nothing, then we assume all zeros (site may be inactive)
                        self.log.debug('Panda status for siteid %s (country=%s, group=%s) not found - setting zeros in status to allow bootstraping of site.' % (siteid, country, group))
                        for queue in queues:
                            self.config.queues[queue]['pandaStatus'] = {'transferring': 0, 'activated': 0, 'running': 0, 'assigned': 0, 'failed': 0, 'finished': 0}

        # Now poll site and cloud status to suppress pilots if a site is offline
        # Take site staus out - better to use individual queue status from schedconfig
        #self.log.info('Polling panda for site status')
        #error,self.pandaSiteStatus = Client.getSiteSpecs(siteType='all')
        #if error != 0:
        #    raise PandaStatusFailure, '''Client.getSiteSpecs(siteType='all') error: %s''' % (error)
        self.log.info('Polling panda for cloud status')
        error,self.pandaCloudStatus = Client.getCloudSpecs()
        if error != 0:
            raise PandaStatusFailure, 'Client.getCloudSpecs() error: %s' % (error)


    def updateConfig(self, cycleNumber):
        '''Update configuration if necessary'''
        self.config.reloadConfigFilesIfChanged()
        if cycleNumber % self.config.config.getint('Factory', 'schedConfigPoll') == 0:
            self.config.reloadSchedConfig()


    def factorySubmitCycle(self, cycleNumber=0):
        '''Go through one status/submission cycle'''
        try:
            self.getCondorStatus()
            self.getPandaStatus()
            self.submitPilots(cycleNumber)
            if isinstance(self.mon, Monitor):
                self.mon.shout(cycleNumber)
        except CondorStatusFailure, errMsg:
            self.log.error('Condor status polling failure: %s', errMsg)
            self.log.error('Will sleep and carry on.')
        except PandaStatusFailure, errMsg:
            self.log.error('Panda status polling failure: %s', errMsg)
            self.log.error('Will sleep and carry on.')
        except IOError, (errno, errMsg):
            self.log.error('Caught IOError Exception %d: %s (%d)' % (errno, errMsg))
            self.log.error('Will sleep and carry on.')
            

class PandaQueue(threading.Thread):
    '''
    Encapsulates all the functionality related to servicing each Panda queue (i.e. siteid, i.e. site).
    
    
    '''
    
    def __init__(self, factory, qconfig, section ):
        self.log = logging.getLogger()
        self.factory = factory      # Factory object that is the parent of this queue object. 
        self.fconfig = factory.config
        self.qconfig = qconfig        # Queue config object for this queue object.
        self.siteid = section    # Queue section designator from config
        self.nickname = qconfig.get(section, "nickname")
        self.dryrun = factory.config.get("Factory", "dryRun")
        self.cycles = factory.config.get("Factory", "cycles" )
        self.sleeptime = factory.config.get("Factory", "sleep")
        self.cyclesrun = 0
        
        # Handle sched plugin
        schedclass = self.qconfig.get(self.siteid, "schedplugin")                
        SchedPlugin = __import__("autopyfactory.plugins.%sSchedPlugin" % schedclass)
        self.scheduler = SchedPlugin()
        
        
        
        
    def run(self):
        '''
        Method called by thread.start()
        Main functional loop of this Queue. 
        '''    
        while True:
            # update batch info
            batchstatus = self.factory.batchplugin.getInfo()
            # update panda info
            
            
            # calculate number to submit
            nsub = self.scheduler.calcSubmitNum()
            # submit using this number
            self.submitPilots(nsub)
            # Exit loop if desired number of cycles is reached...  
            if self.cycles and self.cyclesrun >= self.cycles:
                break            
            self.cyclesrun += 1
            # sleep interval
            time.sleep(self.sleeptime)
              
          

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
    def __init__(self, config):
        self.log = logging.getLogger('main.factory')
        self.interval = int(config.get('Factory','pandaCheckInterval'))
        self.pandaCloudStatus = None 
        self.jobStats = {}
        self.queconfig = {}

    def run(self):
        self.getPandaStatus()
        self.getQueueInfo()
        time.sleep(self.interval)

    def getQueueData(self, queue):
        pass
        


    def getPandaStatus(self):
        
        
        
        
        
        
        
        for country in self.config.sites.keys():
            for group in self.config.sites[country].keys():
                # country/group = None is equivalent to not specifing anything
                self.log.info('Polling panda status for country=%s, group=%s' % (country, group,))
                error,self.config.sites[country][group]['siteStatus'] = Client.getJobStatisticsPerSite(countryGroup=country,workingGroup=group)
                if error != 0:
                    raise PandaStatusFailure, 'Client.getJobStatisticsPerSite(countryGroup=%s,workingGroup=%s) error: %s' % (country, group, error)

                for siteid, queues in self.config.sites[country][group].iteritems():
                    if siteid == 'siteStatus':
                        continue
                    if siteid in self.config.sites[country][group]['siteStatus']:
                        self.log.debug('Panda status: %s (country=%s, group=%s) %s' % (siteid, country, group, self.config.sites[country][group]['siteStatus'][siteid]))
                        for queue in queues:
                            self.config.queues[queue]['pandaStatus'] = self.config.sites[country][group]['siteStatus'][siteid]
                    else:
                        # If panda knows nothing, then we assume all zeros (site may be inactive)
                        self.log.debug('Panda status for siteid %s (country=%s, group=%s) not found - setting zeros in status to allow bootstrapping of site.' % (siteid, country, group))
                        for queue in queues:
                            self.config.queues[queue]['pandaStatus'] = {'transferring': 0, 'activated': 0, 'running': 0, 'assigned': 0, 'failed': 0, 'finished': 0}

        # Now poll site and cloud status to suppress pilots if a site is offline
        # Take site staus out - better to use individual queue status from schedconfig
        #self.factoryMessages.info('Polling panda for site status')
        #error,self.pandaSiteStatus = Client.getSiteSpecs(siteType='all')
        #if error != 0:
        #    raise PandaStatusFailure, '''Client.getSiteSpecs(siteType='all') error: %s''' % (error)
        self.factoryMessages.info('Polling panda for cloud status')
        error,self.pandaCloudStatus = Client.getCloudSpecs()
        if error != 0:
            raise PandaStatusFailure, 'Client.getCloudSpecs() error: %s' % (error)

        

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
        pass
    
    def getJobInfo(self, queue):
        '''
        Returns a list of JobStatus objects, one for each job. 
        '''
        pass
    
class BatchSubmitInterface(object):
    '''
    Interacts with underlying batch system to submit jobs. 
    It should be instantiated one per queue. 
    
    '''
    def submitPilots(self, number):
        '''
        
        '''

class SchedInterface(object):
    '''
    Calculates the number of jobs to submit for a queue. 
    
    ''' 

    def calcSubmitNum(self, config):
        pass





if __name__ == "__main__":
    import pprint
    pandaCloudStatus = Client.getCloudSpecs()
    pprint.pprint(pandaCloudStatus)
    error, sitestatus = Client.getJobStatisticsPerSite(countryGroup='OSG',workingGroup='')
    print("Error: %s" % error)
    pprint.pprint(sitestatus)

        
