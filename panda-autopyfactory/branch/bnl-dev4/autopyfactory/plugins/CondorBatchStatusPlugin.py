#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#


#  Here the list of authors


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
import logging
import time
import threading

from autopyfactory.factory import BatchStatusInterface
from autopyfactory.factory import Singleton 


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
        
        def __init__(self, pandaqueue):

                self.log = logging.getLogger("main.batchstatusplugin")
                self.log.info('Initializing object...')

                self.pandaqueue = pandaqueue
                self.fconfig = pandaqueue.fcl.config          
                self.siteid = pandaqueue.siteid
                self.condoruser = pandaqueue.fcl.get('Factory', 'factoryUser')
                self.factoryid = pandaqueue.fcl.get('Factory', 'factoryId') 
                self.statuscycle = int(pandaqueue.qcl.get(self.siteid, 'batchCheckInterval'))
                self.submitcycle = int(pandaqueue.qcl.get(self.siteid, 'batchSubmitInterval'))

                # results of the condor_q query commands
                self.error = None
                self.output = None
                self.status = None  # result of analyzing self.output

                threading.Thread.__init__(self) # init the thread
                self.stopevent = threading.Event()
                # to avoid the thread to be started more than once
                self.__started = False

                self.log.info('Object initialized.')

        def getInfo(self, queue):
                '''
                Returns a diccionary with the result of the analysis 
                over the output of a condor_q command
                '''

                while not self.output:
                        time.sleep(1)
                if not self.error:
                        self.status = self.__analyzeoutput(self.output, 'jobStatus')
                        return self.status
                return {}

        def start(self):
                '''
                We override method start() to prevent the thread
                to be started more than once
                '''
                if not self.__started:
                        self.log.debug("Creating Condor batch status thread...")
                        self.__started = True
                        threading.Thread.start(self)

        def run(self):
                '''
                Main loop
                '''
                while not self.stopevent.isSet():
                        self.__update()
                        self.__sleep()

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

                self.log.debug("_getStatus called. Querying batch system...")

                querycmd = "condor_q"
                querycmd += " -constr '(owner==\"%s\") && stringListMember(\"PANDA_JSID=%s\", Environment, \" \")'" %(self.factoryid, self.condoruser)
                querycmd += " -format ' jobStatus=%d' jobStatus"
                querycmd += " -format ' globusStatus=%d' GlobusStatus"
                querycmd += " -format ' APF_QUEUE=%s' MATCH_APF_QUEUE"
                querycmd += " -format ' %s\n' Environment"

                self.err, self.output = commands.getstatusoutput(querycmd)

        def __sleep(self):
                # FIXME: temporary solution
                time.sleep(100)

        def __analyzeoutput(self, output, key, queue):
                '''
                ancilla method to analyze the output of the condor_q command
                        - output is the output of the command
                        - key is the pattern that counts
                '''

                output_dic = {}

                if not output: 
                        # FIXME: temporary solution
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

                print '=== output_dic from Condor query = ', output_dic
                return output_dic

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

        def join(self, timeout=None):
                ''' 
                Stop the thread. Overriding this method required to handle Ctrl-C from console.
                ''' 
                self.stopevent.set()
                self.log.debug('Stopping thread....')
                threading.Thread.join(self, timeout)
