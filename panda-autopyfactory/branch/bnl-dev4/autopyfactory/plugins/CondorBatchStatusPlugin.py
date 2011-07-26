#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#

import commands
import logging
import time
import threading

from autopyfactory.factory import BatchStatusInterface
from autopyfactory.factory import Singleton 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class BatchStatusPlugin(threading.Thread, BatchStatusInterface):
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
        
        def __init__(self, wmsqueue):

                self.log = logging.getLogger("main.batchstatusplugin[%s]" %wmsqueue.siteid)
                self.log.info('BatchStatusPlugin: Initializing object...')

                self.wmsqueue = wmsqueue
                self.fconfig = wmsqueue.fcl.config          
                self.siteid = wmsqueue.siteid
                self.condoruser = wmsqueue.fcl.get('Factory', 'factoryUser')
                self.factoryid = wmsqueue.fcl.get('Factory', 'factoryId') 
                self.statuscycle = int(wmsqueue.qcl.get(self.siteid, 'batchCheckInterval'))
                self.submitcycle = int(wmsqueue.qcl.get(self.siteid, 'batchSubmitInterval'))

                # results of the condor_q query commands
                self.updated = False
                self.error = None
                self.output = None
                self.status = None  # result of analyzing self.output

                threading.Thread.__init__(self) # init the thread
                self.stopevent = threading.Event()
                # to avoid the thread to be started more than once
                self.__started = False

                self.log.info('BatchStatusPlugin: Object initialized.')

        def getInfo(self, queue):
                '''
                Returns a diccionary with the result of the analysis 
                over the output of a condor_q command
                '''
                
                self.log.debug('getInfo: Starting with input %s' %queue)

                while not self.updated:
                        time.sleep(1)
                if not self.error:
                        self.status = self.__analyzeoutput(self.output, 'jobStatus', queue)
                        out = self.status
                else:
                        out = {}

                self.log.debug('getInfo: Leaving with output %s' %out)
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
                        self.__update()
                        self.__sleep()

                self.log.debug('run: Leaving')

        def __update(self):
                '''        
                Query Condor for job status, 
                validate ?
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
                '''

                self.log.debug('__update: Starting.')

                querycmd = "condor_q"
                #querycmd += " -constr '(owner==\"%s\") && stringListMember(\"PANDA_JSID=%s\", Environment, \" \")'" %(self.factoryid, self.condoruser)
                querycmd += " -format ' jobStatus=%d' jobStatus"
                querycmd += " -format ' globusStatus=%d' GlobusStatus"
                querycmd += " -format ' MATCH_APF_QUEUE=%s' MATCH_APF_QUEUE"
                querycmd += " -format ' %s\n' Environment"
        
                self.log.debug('__update: Querying cmd = %s' %querycmd.replace('\n','\\n'))

                self.err, self.output = commands.getstatusoutput(querycmd)
                self.updated = True

                self.log.debug('__update: Leaving.')

        def __sleep(self):
                # FIXME: temporary solution

                self.log.debug('__sleep: Starting.')
                time.sleep(100)
                self.log.debug('__sleep: Leaving.')

        def __analyzeoutput(self, output, key, queue):
                '''
                ancilla method to analyze the output of the condor_q command
                        - output is the output of the command
                        - key is the pattern that counts
                '''

                self.log.debug('__analyzeoutput: Starting with inputs: output=%s key=%s queue=%s' %(output, key, queue))

                output_dic = {}

                if not output: 
                        # FIXME: temporary solution
                        self.log.debug('__analyzeoutput: Leaving and returning %s' %output_dic)
                        return output_dic        

                lines = output.split('\n')
                for line in lines:
                        dic = self.__line_to_dict(line)
                        # check that the line had everything we are interested in
                        if 'MATCH_APF_QUEUE' not in dic.keys():
                                continue
                        if key not in dic.keys():
                                continue
                        # if the line had everything, we keep searching
                        if dic['MATCH_APF_QUEUE'] == queue:
                                code = dic[key]
                                if code not in output_dic.keys():
                                        output_dic[code] = 1
                                else:
                                        output_dic[code] += 1 

                self.log.debug('__analyzeoutput: Leaving and returning %s' %output_dic)
                return output_dic

        def join(self, timeout=None):
                ''' 
                Stop the thread. Overriding this method required to handle Ctrl-C from console.
                ''' 

                self.log.debug('join: Starting with input %s' %timeout)

                self.stopevent.set()
                self.log.debug('Stopping thread....')
                threading.Thread.join(self, timeout)

                self.log.debug('join: Leaving')

                # ------------------------------------------------------------
                #  ancillas 
                # ------------------------------------------------------------

        def __line_to_dict(self, line):
                '''
                method ancilla to convert each line from the output of condor_q
                into a dictionary
                '''
                d = {}
                tokens = line.split()
                for token in tokens: 
                        key, value = token.split('=')
                        d[key] = value 
                return d
