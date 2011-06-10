#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

import logging
import threading
import time
import pprint
from autopyfactory.factory import BatchSubmitInterface


class BatchSubmitPlugin(BatchSubmitInterface):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    
    '''
    def __init__(self, pandaqueue):
        self.log = logging.getLogger("main.condorsubmit")
        self.pandaqueue = pandaqueue
        
    
    
    def submitPilots(self):
        '''
        
        '''
    
    def _condorPilotSubmit(self, queue, cycleNumber=0, pilotNumber=1):
        now = time.localtime()
        logPath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2]) + queue.translate(string.maketrans('/:','__'))
        logDir = self.config.config.get('Pilots', 'baseLogDir') + logPath
        logUrl = self.config.config.get('Pilots', 'baseLogDirUrl') + logPath
        if not os.access(logDir, os.F_OK):
            try:
                os.makedirs(logDir)
                self.log.debug('Created directory %s', logDir)
            except OSError, (errno, errMsg):
                self.log.error('Failed to create directory %s (error %d): %s', logDir, errno, errMsg)
                self.log.error('Cannot submit pilots for %s', queue)
                return
        jdlFile = logDir + '/submitMe.jdl'
        error = self.writeJDL(queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber)
        if error != 0:
            self.log.error('Cannot submit pilots for %s', gatekeeper)
            return
        if not self.dryRun:
            (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + jdlFile)
            if exitStatus != 0:
                self.log.error('condor_submit command for %s failed (status %d): %s', queue, exitStatus, output)
            else:
                self.log.debug('condor_submit command for %s succeeded', queue)
                if isinstance(self.mon, Monitor):
                    nick = self.config.queues[queue]['nickname']
                    label = queue
                    self.mon.notify(nick, label, output)

        else:
            self.log.debug('Dry run mode - pilot submission supressed.')


    def writeJDL(self, queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber=0):
        # Encoding the wrapper in the script is a bit inflexible, but saves
        # nasty search and replace on a template file, and means one less 
        # dependency for the factory.
        try:
            JDL = open(jdlFile, "w")
        except IOError, (errno, errMsg) :
            self.log.error('Failed to open file %s (error %d): %s', jdlFile, errno, errMsg)
            return 1

        print >>JDL, "# Condor-G glidein pilot for panda"
        print >>JDL, "executable=%s" % self.config.config.get('Pilots', 'executable')
        print >>JDL, "Dir=%s/" % logDir
        print >>JDL, "output=$(Dir)/$(Cluster).$(Process).out"
        print >>JDL, "error=$(Dir)/$(Cluster).$(Process).err"
        print >>JDL, "log=$(Dir)/$(Cluster).$(Process).log"
        print >>JDL, "stream_output=False"
        print >>JDL, "stream_error=False"
        print >>JDL, "notification=Error"
        print >>JDL, "notify_user=%s" % self.config.config.get('Factory', 'factoryOwner')
        print >>JDL, "universe=grid"
        # Here we insert the switch for CREAM CEs. This is rather a hack for now, but will
        # improve once multiple backends are supported properly
        if self.config.queues[queue]['_isCream']:
            print >>JDL, "grid_resource=cream %s:%d/ce-cream/services/CREAM2 %s %s" % (
                 self.config.queues[queue]['_creamHost'], self.config.queues[queue]['_creamPort'], 
                 self.config.queues[queue]['_creamBatchSys'], self.config.queues[queue]['localqueue'])
        else:
            # GRAM resource
            print >>JDL, "grid_resource=gt2 %s" % self.config.queues[queue]['queue']
            print >>JDL, "globusrsl=(queue=%s)(jobtype=single)" % self.config.queues[queue]['localqueue']
        # Probably not so helpful to set these in the JDL
        #if self.config.queues[queue]['memory'] != None:
        #    print >>JDL, "(maxMemory=%d)" % self.config.queues[queue]['memory'],
        #if self.config.queues[queue]['wallClock'] != None:
        #    print >>JDL, "(maxWallTime=%d)" % self.config.queues[queue]['wallClock'],
        #print >>JDL
        #print >>JDL, '+MATCH_gatekeeper_url="%s"' % self.config.queues[queue]['queue']
        #print >>JDL, '+MATCH_queue="%s"' % self.config.queues[queue]['localqueue']
        print >>JDL, "x509userproxy=%s" % self.config.queues[queue]['gridProxy']
        print >>JDL, 'periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30)'
        print >>JDL, 'periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)'
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

