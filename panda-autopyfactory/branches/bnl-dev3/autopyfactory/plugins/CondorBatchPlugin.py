#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#
#
#
#
#
#
#
#
#

import logging
import threading
import time
import pprint
from autopyfactory.factory import BatchSubmitInterface, BatchStatusInterface

# This variable points to a single, global CondorStatusThread object, which has the current status output. 
STATUSTHREAD = None
# Used to make sure that one and only one copy of the thread is started. 
STATUSLOCK = threading.Lock()


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


class BatchStatusPlugin(BatchStatusInterface):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    The first time it is instantiated, 
    '''

    def __init__(self, pandaqueue):
            global STATUSTHREAD
            global STATUSLOCK
            self.log = logging.getLogger("main.condorstatus")
            self.pandaqueue = pandaqueue
            self.fconfig = pandaqueue.fcl.config          
            self.siteid = pandaqueue.siteid
            self.condoruser = pandaqueue.fcl.config.get('Factory', 'factoryUser')
            self.factoryid = pandaqueue.fcl.config.get('Factory', 'factoryId') 
            self.statuscycle = int(pandaqueue.qcl.config.get(self.siteid, 'batchCheckInterval'))
            self.submitcycle = int(pandaqueue.qcl.config.get(self.siteid, 'batchSubmitInterval'))
                        
            if not STATUSLOCK.acquire(False):
                # Somebody else has the lock, and will start the thread. 
                pass
            else:
                try:
                    if STATUSTHREAD:
                        self.log.debug("StatusThread already created. Ignoring.")
                    else:
                        self.log.debug("First to get lock. Creating StatusThread...")
                        # We are the first to get the lock. 
                        # Initialize the thread object. 
                        STATUSTHREAD = CondorStatusThread(self.condoruser, self.factoryid, self.statuscycle )
                        # Start the thread.
                        STATUSTHREAD.start()
                        
                finally:
                    STATUSLOCK.release()
                    
    def getInfo(self, queue):
        global STATUSTHREAD
        return STATUSTHREAD.getInfo()


  
        
        
class CondorStatusThread(threading.Thread):        
    '''
    This class is expected to have only one instance, and is shared by multiple CondorStatus 
    objects (one per PandaQueue object). 
    '''
    
    def __init__(self, condoruser, factoryid, cycletime=53 ):
        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger("main.condorstatusthread")
        self.stopevent = threading.Event()
        self.currentinfo = None
        self.newinfo = None
        self.condoruser = condoruser
        self.factoryid = factoryid
        self.cycletime = int(cycletime)
    
    def run(self):
        while not self.stopevent.isSet():
            self.newinfo = self._getStatus()
            pprint.pprint(self.currentinfo)
            time.sleep(self.cycletime)

    def join(self,timeout=None):
        """
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        """
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
    

            
    def getInfo(self):
        return "jobStatus=1 globusStatus=1 -None TMP=/tmp FACTORYUSER=user APFFID=BNL-gridui11-jhover APFMON=http://apfmon.lancs.ac.uk/mon/ APP=/usatlas/OSG APFCID=6974.0 PANDA_JSID=BNL-gridui11-jhover DATA=/usatlas/prodjob/share/ FACTORYQUEUE=ANALY_TEST-APF GTAG=http://gridui11.usatlas.bnl.gov:25880/2011-04-08/ANALY_TEST-APF/6974.0.out"        
        #return self.currentinfo
        

    def _getStatus(self):
        '''
        Query Condor for job status, validate and store info in newinfo, and 
        finally swap newinfo for currentinfo. 
        
        Condor-G query template example:
        
        condor_q -constr '(owner=="apf") && stringListMember("PANDA_JSID=BNL-gridui11-jhover",Environment, " ")'
            -format 'jobStatus=%d ' jobStatus -format 'globusStatus=%d ' GlobusStatus -format 'gkUrl=%s' MATCH_gatekeeper_url
            -format '-%s ' MATCH_queue -format '%s\n' Environment
        
        Condor-C query template example:
        
        
        '''
        self.log.debug("_getStatus called. Querying batch system...")      
        querycmd = "condor_q -constr '(owner==\"%s\") && " % self.condoruser
        querycmd += "stringListMember(\"PANDA_JSID=%s " % self.factoryid
        querycmd += "" 







    def _getCondorStatus(self):
        # We query condor for jobs running as us (owner) and this factoryId so that multiple 
        # factories can run on the same machine
        # Ask for the output from condor to be in the form of "key=value" pairs so we can easily 
        # convert to a dictionary
        condorQuery = '''condor_q -constr '(owner=="''' + self.config.config.get('Factory', 'condorUser') + \
            '''") && stringListMember("PANDA_JSID=''' + self.config.config.get('Factory', 'factoryId') + \
            '''", Environment, " ")' -format 'jobStatus=%d ' JobStatus -format 'globusStatus=%d ' GlobusStatus -format 'gkUrl=%s' MATCH_gatekeeper_url -format '-%s ' MATCH_queue -format '%s\n' Environment'''
        self.log.debug("condor query: %s" % (condorQuery))
        (condorStatus, condorOutput) = commands.getstatusoutput(condorQuery)
        if condorStatus != 0:
            raise CondorStatusFailure, 'Condor queue query returned %d: %s' % (condorStatus, condorOutput)
        # Count the number of queued pilots for each queue
        # For now simply divide into active and inactive pilots (JobStatus == or != 2)
        try:
            for queue in self.config.queues.keys():
                self.config.queues[queue]['pilotQueue'] = {'active' : 0, 'inactive' : 0, 'total' : 0,}
            for line in condorOutput.splitlines():
                statusItems = line.split()
                statusDict = {}
                for item in statusItems:
                    try:
                        (key, value) = item.split('=', 1)
                        statusDict[key] = value
                    except ValueError:
                        self.log.warning('Unexpected output from condor_q query: %s' % line)
                        continue
                # We have encoded the factory queue name in the environment
                try:
                    self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['total'] += 1                
                    if statusDict['jobStatus'] == '2':
                        self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['active'] += 1
                    else:
                        self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['inactive'] += 1
                except KeyError,e:
                    self.log.debug('Key error from unusual condor status line: %s %s' % (e, line))
            for queue, queueParameters in self.config.queues.iteritems():
                self.log.debug('Condor: %s, %s: pilot status: %s',  queueParameters['siteid'], 
                                           queue, queueParameters['pilotQueue'])
        except ValueError, errorMsg:
            raise CondorStatusFailure, 'Error in condor queue result: %s' % errorMsg
        
        
        
