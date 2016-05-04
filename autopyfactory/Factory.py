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


import os, os.path, sys, logging, commands, time, string, re, subprocess

from xml.dom import minidom, DOMException
from xml.parsers.expat import ExpatError

from autopyfactory.Exceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.ConfigLoader import factoryConfigLoader
from autopyfactory.Monitor import Monitor
import userinterface.Client as Client


class factory:
    def __init__(self, mainLogger, dryRun=False, configFiles=('factory.conf',)):
        self.factoryMessages = logging.getLogger('main.factory')
        self.factoryMessages.debug('Factory class initialised.')

        self.dryRun = dryRun
        if configFiles != None:
            self.config = factoryConfigLoader(self.factoryMessages, configFiles)

        self.mon = None
        try:
            args = dict(self.config.config.items('Factory'))
            args.update(self.config.config.items('Pilots'))
            self.mon = Monitor(**args)
        except:
            self.factoryMessages.warn('Monitoring not configured')


    def note(self, queue, msg):
        self.factoryMessages.info('%s: %s' % (queue,msg))
        if isinstance(self.mon, Monitor):
            nick = self.config.queues[queue]['nickname']
            self.mon.msg(nick, queue, msg)


    def getCondorStatus(self):
        # We query condor for jobs running as us (owner) and this factoryId so that multiple 
        # factories can run on the same machine
        # Ask for the output from condor to be in the form of "key=value" pairs so we can easily 
        # convert to a dictionary
        condorQuery = '''condor_q -constr '(owner=="''' + self.config.config.get('Factory', 'condorUser') + \
            '''") && stringListMember("PANDA_JSID=''' + self.config.config.get('Factory', 'factoryId') + \
            '''", Environment, " ")' -format 'jobStatus=%d ' JobStatus -format 'globusStatus=%d ' GlobusStatus -format 'gkUrl=%s' MATCH_gatekeeper_url -format '-%s ' MATCH_queue -format '%s\n' Environment'''
        self.factoryMessages.debug("condor query: %s" % (condorQuery))
        (condorStatus, condorOutput) = commands.getstatusoutput(condorQuery)
        if condorStatus != 0:
            raise CondorStatusFailure, 'Condor queue query returned %d: %s' % (condorStatus, condorOutput)
        # Count the number of queued pilots for each queue
        # For now simply divide into active and inactive pilots (JobStatus == or != 2)
        try:
            for queue in self.config.backend['condor']:
                self.config.queues[queue]['pilotQueue'] = {'active' : 0, 'inactive' : 0, 'total' : 0,}
            for line in condorOutput.splitlines():
                statusItems = line.split()
                statusDict = {}
                for item in statusItems:
                    try:
                        (key, value) = item.split('=', 1)
                        statusDict[key] = value
                    except ValueError:
                        self.factoryMessages.warning('Unexpected output from condor_q query: %s' % line)
                        continue
                # We have encoded the factory queue name in the environment
                try:
                    self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['total'] += 1                
                    if statusDict['jobStatus'] == '2':
                        self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['active'] += 1
                    else:
                        self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['inactive'] += 1
                except KeyError,e:
                    self.factoryMessages.debug('Key error from unusual condor status line: %s %s' % (e, line))
            for queue, queueParameters in self.config.queues.iteritems():
                self.factoryMessages.debug('Condor: %s, %s: pilot status: %s',  queueParameters['siteid'], 
                                           queue, queueParameters['pilotQueue'])
        except ValueError, errorMsg:
            raise CondorStatusFailure, 'Error in condor queue result: %s' % errorMsg


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
                    self.pilotSubmit(queue, cycleNumber, 1)
                continue

            # Production site, online - look for activated jobs and ensure pilot queue is topped up, or
            # submit some idling pilots
            if queueParameters['pandaStatus']['activated'] > 0:
                # Activated jobs at this site
                if queueParameters['depthboost'] == None:
                    self.factoryMessages.info('Depth boost unset for queue %s - defaulting to 2' % queue)
                    depthboost = 2
                else:
                    depthboost = queueParameters['depthboost']
                if queueParameters['pilotQueue']['inactive'] < queueParameters['nqueue'] or \
                        (queueParameters['pandaStatus']['activated'] > queueParameters['pilotQueue']['inactive'] and \
                         queueParameters['pilotQueue']['inactive'] < queueParameters['nqueue'] * depthboost):
                    msg = '%d activated jobs, %d inactive pilots queued (< queue depth %d * depth boost %d). Will submit full pilot load.' % (queueParameters['pandaStatus']['activated'], queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'], depthboost)
                    self.note(queue, msg)
                    self.pilotSubmit(queue, cycleNumber, queueParameters['nqueue'])
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
                    self.pilotSubmit(queue, cycleNumber, 1)
            else:
                msg = 'No activated jobs, %d inactive pilots queued (queue depth %d). No extra pilots needed.' % (queueParameters['pilotQueue']['inactive'], queueParameters['nqueue'])
                self.note(queue, msg)


    def pilotSubmit(self, queue, cycleNumber=0, pilotNumber=1):
        now = time.localtime()
        logPath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2]) + queue.translate(string.maketrans('/:','__'))
        logDir = self.config.config.get('Pilots', 'baseLogDir') + logPath
        logUrl = self.config.config.get('Pilots', 'baseLogDirUrl') + logPath
        if not os.access(logDir, os.F_OK):
            try:
                os.makedirs(logDir)
                self.factoryMessages.debug('Created directory %s', logDir)
            except OSError, (errno, errMsg):
                self.factoryMessages.error('Failed to create directory %s (error %d): %s', logDir, errno, errMsg)
                self.factoryMessages.error('Cannot submit pilots for %s', queue)
                return

        if queue['backend'] == 'condor':
            self.pilotSubmit(queue, cycleNumber, pilotNumber, logDir, logUrl)
        elif queue['backend'] == 'batch':
            self.batchPilotSubmit(queue,cycleNumber, pilotNumber, logDir, logUrl)

    def condorPilotSubmit(self, queue, cycleNumber, pilotNumber, logDir, logUrl):
        jdlFile = logDir + '/submitMe.jdl'
        error = self.writeCondorJDL(queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber)
        if error != 0:
            self.factoryMessages.error('Cannot submit pilots for %s', gatekeeper)
            return
        if not self.dryRun:
            (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + jdlFile)
            if exitStatus != 0:
                self.factoryMessages.error('condor_submit command for %s failed (status %d): %s', queue, exitStatus, output)
            else:
                self.factoryMessages.debug('condor_submit command for %s succeeded', queue)
                if isinstance(self.mon, Monitor):
                    nick = self.config.queues[queue]['nickname']
                    label = queue
                    self.mon.notify(nick, label, output)

        else:
            self.factoryMessages.debug('Dry run mode - pilot submission suppressed.')
            

    def writeCondorJDL(self, queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber=0):
        # Encoding the wrapper in the script is a bit inflexible, but saves
        # nasty search and replace on a template file, and means one less 
        # dependency for the factory.
        try:
            JDL = open(jdlFile, "w")
        except IOError, (errno, errMsg) :
            self.factoryMessages.error('Failed to open file %s (error %d): %s', jdlFile, errno, errMsg)
            return 1

        print >>JDL, "# Condor pilot for panda, queue %s, type %s, subtype" % (queue, self.config.queues[queue][type],
            self.config.queues[queue][subtype])
        print >>JDL, "executable=%s" % self.config.config.get('Pilots', 'executable')
        print >>JDL, "Dir=%s/" % logDir
        print >>JDL, "output=$(Dir)/$(Cluster).$(Process).out"
        print >>JDL, "error=$(Dir)/$(Cluster).$(Process).err"
        print >>JDL, "log=$(Dir)/$(Cluster).$(Process).log"
        print >>JDL, "stream_output=False"
        print >>JDL, "stream_error=False"
        print >>JDL, "notification=Error"
        print >>JDL, "notify_user=%s" % self.config.config.get('Factory', 'factoryOwner')
        if self.config.queues[queue][type] == 'grid':
            if self.config.queues[queue][subtype] == 'cream':
                print >>JDL, "grid_resource=cream %s:%d/ce-cream/services/CREAM2 %s %s" % (
                    self.config.queues[queue]['_creamHost'], self.config.queues[queue]['_creamPort'], 
                    self.config.queues[queue]['_creamBatchSys'], self.config.queues[queue]['localqueue'])
            elif self.config.queues[queue][subtype] == 'gt2':
                print >>JDL, "grid_resource=gt2 %s" % self.config.queues[queue]['queue']
                print >>JDL, "globusrsl=(queue=%s)(jobtype=single)" % self.config.queues[queue]['localqueue']
            print >>JDL, "x509userproxy=%s" % self.config.queues[queue]['gridProxy']
            print >>JDL, 'periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30)'
            print >>JDL, 'periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)'
        elif self.config.queues[queue][subtype] == 'vanilla':
            # Do what needs to be done for straight condor submission
            pass
        # In job environment correct GTAG to URL for logs, JSID should be factoryId
        print >>JDL, 'environment = "PANDA_JSID=%s' % self.config.config.get('Factory', 'factoryId'),
        print >>JDL, 'GTAG=%s/$(Cluster).$(Process).out' % logUrl,
        print >>JDL, 'APFCID=$(Cluster).$(Process)',
        print >>JDL, 'APFFID=%s' % self.config.config.get('Factory', 'factoryId'),
        if isinstance(self.mon, Monitor):
            print >>JDL, 'APFMON=%s' % self.config.config.get('Factory', 'monitorURL'),
        print >>JDL, 'FACTORYQUEUE=%s' % queue,
        if self.config.queues[queue]['user'] != None:
            print >>JDL, 'FACTORYUSER=%s' % self.config.queues[queue]['user'],
        if self.config.queues[queue]['environ'] != None and self.config.queues[queue]['environ'] != '':
            print >>JDL, self.config.queues[queue]['environ'],
        print >>JDL, '"'
        print >>JDL, "arguments = -s %s -h %s" % (self.config.queues[queue]['siteid'], self.config.queues[queue]['nickname']),
        print >>JDL, "-p %d -w %s" % (self.config.queues[queue]['port'], self.config.queues[queue]['server']),
        if self.config.queues[queue]['jobRecovery'] == False:
            print >>JDL, " -j false",
        if self.config.queues[queue]['memory'] != None:
            print >>JDL, " -k %d" % self.config.queues[queue]['memory'],
        if self.config.queues[queue]['user'] != None:
            print >>JDL, " -u %s" % self.config.queues[queue]['user'],
        if self.config.queues[queue]['group'] != None:
            print >>JDL, " -v %s" % self.config.queues[queue]['group'],
        if self.config.queues[queue]['country'] != None:
            print >>JDL, " -o %s" % self.config.queues[queue]['country'],
        if self.config.queues[queue]['allowothercountry'] == True:
            print >>JDL, " -A True",
        print >>JDL
        print >>JDL, "queue %d" % pilotNumber
        JDL.close()
        return 0


    def batchPilotSubmit(self, queue, cycleNumber, pilotNumber, logDir, logUrl):
        '''Submit pilots to the batch system - this must be done one by one in torque'''
        for pilotN in xrange(pilotNumber):
            myId = '%06d-%03d' % (cycleNumber, pilotN)
            jdlFile = os.path.join(logDir, myId + '.submit')
            try:
                JDL = open(jdlFile, "w")
            except IOError, (errno, errMsg) :
                self.factoryMessages.error('Failed to open file %s (error %d): %s', jdlFile, errno, errMsg)
                return 1

            print >>JDL, "#! /bin/bash"
            print >>JDL, "#PBS -e %s" % os.path.join(logDir, myId + '.err')
            print >>JDL, "#PBS -o %s" % os.path.join(logDir, myId + '.out')
            print >>JDL, "#PBS -u %s" % self.config.config[queue]['user']
            print >>JDL, "#PBS -q %s" % self.config.config[queue]['batchQueue']
            
            print >>JDL, "%s -s %s -h %s" % (self.config.config.get('Pilots', 'executable'), 
                                             self.config.queues[queue]['siteid'], self.config.queues[queue]['nickname']),
            print >>JDL, "-p %d -w %s" % (self.config.queues[queue]['port'], self.config.queues[queue]['server']),  
            if self.config.queues[queue]['jobRecovery'] == False:
                print >>JDL, " -j false",
            if self.config.queues[queue]['memory'] != None:
                print >>JDL, " -k %d" % self.config.queues[queue]['memory'],
            if self.config.queues[queue]['user'] != None:
                print >>JDL, " -u %s" % self.config.queues[queue]['user'],
            if self.config.queues[queue]['group'] != None:
                print >>JDL, " -v %s" % self.config.queues[queue]['group'],
            if self.config.queues[queue]['country'] != None:
                print >>JDL, " -o %s" % self.config.queues[queue]['country'],
            if self.config.queues[queue]['allowothercountry'] == True:
                print >>JDL, " -A True",
            print >>JDL
            JDL.close()
            
            if not self.dryRun:
                try:
                    subprocess.call(['qsub', '-V', 'jdlFile'])
                except CalledProcessError:
                    self.factoryMessages.error('qsub command failed for %s on %s' % (myId, queue))
            else:
                self.factoryMessages.debug('Dry run mode - pilot submission suppressed.')


    def getPandaStatus(self):
        for country in self.config.sites.keys():
            for group in self.config.sites[country].keys():
                # country/group = None is equivalent to not specifing anything
                self.factoryMessages.info('Polling panda status for country=%s, group=%s' % (country, group,))
                error,self.config.sites[country][group]['siteStatus'] = Client.getJobStatisticsPerSite(countryGroup=country,workingGroup=group)
                if error != 0:
                    raise PandaStatusFailure, 'Client.getJobStatisticsPerSite(countryGroup=%s,workingGroup=%s) error: %s' % (country, group, error)

                for siteid, queues in self.config.sites[country][group].iteritems():
                    if siteid == 'siteStatus':
                        continue
                    if siteid in self.config.sites[country][group]['siteStatus']:
                        self.factoryMessages.debug('Panda status: %s (country=%s, group=%s) %s' % (siteid, country, group, self.config.sites[country][group]['siteStatus'][siteid]))
                        for queue in queues:
                            self.config.queues[queue]['pandaStatus'] = self.config.sites[country][group]['siteStatus'][siteid]
                    else:
                        # If panda knows nothing, then we assume all zeros (site may be inactive)
                        self.factoryMessages.debug('Panda status for siteid %s (country=%s, group=%s) not found - setting zeros in status to allow bootstraping of site.' % (siteid, country, group))
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


    def getBatchStatus(self):
        '''Use XML dump of qstat information'''
        # Set current totals to zero here (in case of parsing error)
        for queue in self.config.backends['batch']:
            self.config.queues[queue]['pilotQueue'] = {'active' : 0, 'inactive' : 0, 'total' : 0,}
        
        try:
            batchState = minidom.parseString(subprocess.Popen(["qstat", "-x"], stdout=subprocess.PIPE).communicate()[0])
        except DOMException:
            raise
        except ExpatError, error:
            self.factoryMessages.warning('Problem while parsing XML: %s' % (error, ))
            return
        except OSError:
            raise

        jobs = batchState.getElementsByTagName('Job')
        for j in jobs:
            # No idea if this is efficient, but it works!
            user = j.getElementsByTagName('euser')[0].firstChild.data.encode('utf-8')
            state = j.getElementsByTagName('job_state')[0].firstChild.data.encode('utf-8')
            
            for queue in self.config.backends['batch']:
                if user == self.config.queues[queue]['user']:
                    self.config.queues[queue]['pilotQueue']['total'] += 1
                    if state == 'R':
                        self.config.queues[queue]['pilotQueue']['active'] += 1
                    else:
                        self.config.queues[queue]['pilotQueue']['inactive'] += 1


    def updateConfig(self, cycleNumber):
        '''Update configuration if necessary'''
        self.config.reloadConfigFilesIfChanged()
        if cycleNumber % self.config.config.getint('Factory', 'schedConfigPoll') == 0:
            self.config.reloadSchedConfig()


    def factorySubmitCycle(self, cycleNumber=0):
        '''Go through one status/submission cycle'''
        try:
            # Only bother polling for configured backends
            if 'condor' in self.config.backends:
                self.getCondorStatus()
            if 'batch' in self.config.backends:
                self.getBatchStatus()
            self.getPandaStatus()
            self.submitPilots(cycleNumber)
            if isinstance(self.mon, Monitor):
                self.mon.shout(cycleNumber)
        except CondorStatusFailure, errMsg:
            self.factoryMessages.error('Condor status polling failure: %s', errMsg)
            self.factoryMessages.error('Will sleep and carry on.')
        except PandaStatusFailure, errMsg:
            self.factoryMessages.error('Panda status polling failure: %s', errMsg)
            self.factoryMessages.error('Will sleep and carry on.')
        
