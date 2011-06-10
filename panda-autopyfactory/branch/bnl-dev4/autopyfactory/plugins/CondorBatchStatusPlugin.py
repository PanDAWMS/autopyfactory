#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

import logging
import threading
import time
import pprint
from autopyfactory.factory import BatchSubmitInterface, BatchStatusInterface

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
        
        
        
