#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

import commands
import logging
import os
import string
import time

from autopyfactory.factory import BatchSubmitInterface
import submit 


class BatchSubmitPlugin(BatchSubmitInterface):
        '''
        This class is expected to have separate instances for each PandaQueue object. 
        '''
        
        def __init__(self):
                self.log = logging.getLogger("main.condorsubmit")
 
        def submitPilots(self, queue, nbpilots, fcl, qcl):
                '''
                queue is the queue
                nsub is the number of pilots to be submitted 
                fcl is the FactoryConfigLoader object
                qcl is the QueueConfigLoader object
                '''
                self.queue = queue
                self.nbpilots = nbpilots
                self.fcl = fcl
                self.qcl = qcl

                self.__prepareJSDFile()
                self.__submit() 

        def __prepareJSDFile(self):

                self.JSD = submit.JSDFile()
                self.__createJSDFile()
                self.__writeJSDFile()

        def __createJSDFile(self):

                ### def writeJDL(self, queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber=0):
                # Encoding the wrapper in the script is a bit inflexible, but saves
                # nasty search and replace on a template file, and means one less 
                # dependency for the factory.


                #try:
                #        JDL = open(self.jdlFile, "w")
                #except IOError, (errno, errMsg) :
                #        self.log.error('Failed to open file %s (error %d): %s', self.jdlFile, errno, errMsg)
                #        return 1
        

                self.JSD.add("# Condor-G glidein pilot for panda")
                self.JSD.add("executable=%s" % self.fcl.config.get('Pilots', 'executable'))
                self.JSD.add("Dir=%s/" % self.logDir)
                self.JSD.add("output=$(Dir)/$(Cluster).$(Process).out")
                self.JSD.add("error=$(Dir)/$(Cluster).$(Process).err")
                self.JSD.add("log=$(Dir)/$(Cluster).$(Process).log")
                self.JSD.add("stream_output=False")
                self.JSD.add("stream_error=False")
                self.JSD.add("notification=Error")
                self.JSD.add("notify_user=%s" % self.fcl.config.get('Factory', 'factoryOwner'))
                self.JSD.add("universe=grid")

                # Here we insert the switch for CREAM CEs. This is rather a hack for now, but will
                # improve once multiple backends are supported properly
                if self.config.queues[self.queue]['_isCream']:
                        self.JSD.add("grid_resource=cream %s:%d/ce-cream/services/CREAM2 %s %s" % (
                                 self.config.queues[self.queue]['_creamHost'], self.config.queues[self.queue]['_creamPort'], 
                                 self.config.queues[self.queue]['_creamBatchSys'], self.config.queues[self.queue]['localqueue']))
                else:
                        # GRAM resource
                        self.JSD.add("grid_resource=gt2 %s" % self.config.queues[self.queue]['queue'])
                        self.JSD.add("globusrsl=(queue=%s)(jobtype=single)" % self.config.queues[self.queue]['localqueue'])
                # Probably not so helpful to set these in the JDL
                #if self.config.queues[self.queue]['memory'] != None:
                #        self.JSD.add("(maxMemory=%d)" % self.config.queues[self.queue]['memory'],)
                #if self.config.queues[self.queue]['wallClock'] != None:
                #        self.JSD.add("(maxWallTime=%d)" % self.config.queues[self.queue]['wallClock'],)
                ##print >>JDL
                #self.JSD.add('+MATCH_gatekeeper_url="%s"' % self.config.queues[self.queue]['queue'])
                #self.JSD.add('+MATCH_queue="%s"' % self.config.queues[self.queue]['localqueue'])
                self.JSD.add("x509userproxy=%s" % self.config.queues[self.queue]['gridProxy'])
                self.JSD.add('periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30)')
                self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')
                # In job environment correct GTAG to URL for logs, JSID should be factoryId
                self.JSD.add('environment = "PANDA_JSID=%s' % self.fcl.config.get('Factory', 'factoryId'),)
                self.JSD.add('GTAG=%s/$(Cluster).$(Process).out' % self.logUrl,)
                self.JSD.add('APFCID=$(Cluster).$(Process)',)
                self.JSD.add('APFFID=%s' % self.fcl.config.get('Factory', 'factoryId'),)
                if isinstance(self.mon, Monitor):
                        self.JSD.add('APFMON=%s' % self.fcl.config.get('Factory', 'monitorURL'),)
                self.JSD.add('FACTORYQUEUE=%s' % self.queue,)
                if self.config.queues[self.queue]['user'] != None:
                        self.JSD.add('FACTORYUSER=%s' % self.config.queues[self.queue]['user'],)
                if self.config.queues[self.queue]['environ'] != None and self.config.queues[self.queue]['environ'] != '':
                        self.JSD.add(self.config.queues[self.queue]['environ'],)
                self.JSD.add('"')
                self.JSD.add("arguments = -s %s -h %s" % (self.config.queues[self.queue]['siteid'], self.config.queues[self.queue]['nickname']),)
                self.JSD.add("-p %d -w %s" % (self.config.queues[self.queue]['port'], self.config.queues[self.queue]['server']),)
                if self.config.queues[self.queue]['jobRecovery'] == False:
                        self.JSD.add(" -j false",)
                if self.config.queues[self.queue]['memory'] != None:
                        self.JSD.add(" -k %d" % self.config.queues[self.queue]['memory'],)
                if self.config.queues[self.queue]['user'] != None:
                        self.JSD.add(" -u %s" % self.config.queues[self.queue]['user'],)
                if self.config.queues[self.queue]['group'] != None:
                        self.JSD.add(" -v %s" % self.config.queues[self.queue]['group'],)
                if self.config.queues[self.queue]['country'] != None:
                        self.JSD.add(" -o %s" % self.config.queues[self.queue]['country'],)
                if self.config.queues[self.queue]['allowothercountry'] == True:
                        self.JSD.add(" -A True",)
                #print >>JDL
                self.JSD.add("queue %d" % self.nbpilots)
                #return 0

        def __writeJSDFile(self):
                '''
                Dumps the whole content of the JSDFile object into a disk file
                '''

                now = time.localtime()
                logPath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2]) + self.queue.translate(string.maketrans('/:','__'))
                self.logDir = self.fcl.config.get('Pilots', 'baseLogDir') + logPath
                self.logUrl = self.fcl.config.get('Pilots', 'baseLogDirUrl') + logPath
                if not os.access(self.logDir, os.F_OK):
                        try:
                                os.makedirs(self.logDir)
                                self.log.debug('Created directory %s', self.logDir)
                        except OSError, (errno, errMsg):
                                self.log.error('Failed to create directory %s (error %d): %s', self.logDir, errno, errMsg)
                                self.log.error('Cannot submit pilots for %s', self.queue)
                                return
                self.jdlFile = self.logDir + '/submitMe.jdl'
                ### error = self.writeJDL(queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber)
                self.JSD.write(self.jdlFile)

        def __submit(self):
                '''
                Submit pilots
                '''

                ###def _condorPilotSubmit(self, queue, cycleNumber=0, pilotNumber=1):
                ###     if error != 0:
                ###             self.log.error('Cannot submit pilots for %s', gatekeeper)
                ###             return
                ###     if not self.dryRun:
                ###             (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + self.jdlFile)
                ###             if exitStatus != 0:
                ###                     self.log.error('condor_submit command for %s failed (status %d): %s', queue, exitStatus, output)
                ###             else:
                ###                     self.log.debug('condor_submit command for %s succeeded', queue)
                ###                     if isinstance(self.mon, Monitor):
                ###                             nick = self.config.queues[self.queue]['nickname']
                ###                             label = queue
                ###                             self.mon.notify(nick, label, output)
                ###     else:
                ###             self.log.debug('Dry run mode - pilot submission supressed.')

                if not self.dryRun:
                        (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + self.jdlFile)
                        if exitStatus != 0:
                                self.log.error('condor_submit command for %s failed (status %d): %s', self.queue, exitStatus, output)
                        else:
                                self.log.debug('condor_submit command for %s succeeded', self.queue)
                                if isinstance(self.mon, Monitor):
                                        nick = self.config.queues[self.queue]['nickname']
                                        label = self.queue
                                        self.mon.notify(nick, label, output)
                else:
                        self.log.debug('Dry run mode - pilot submission supressed.')


        

        
