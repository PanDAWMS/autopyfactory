#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#

import commands
import logging
import time
import threading
import xml.dom.minidom

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

                self.log = logging.getLogger("main.batchstatusplugin[singleton created by %s]" %wmsqueue.apfqueue)
                self.log.info('BatchStatusPlugin: Initializing object...')

                self.wmsqueue = wmsqueue
                self.fconfig = wmsqueue.fcl.config          
                self.apfqueue = wmsqueue.apfqueue
                self.condoruser = wmsqueue.fcl.get('Factory', 'factoryUser')
                self.factoryid = wmsqueue.fcl.get('Factory', 'factoryId') 
                self.statuscycle = int(wmsqueue.qcl.get(self.apfqueue, 'batchCheckInterval'))
                self.submitcycle = int(wmsqueue.qcl.get(self.apfqueue, 'batchSubmitInterval'))

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
               
                self.log.debug('getInfo[%s]: Starting ' %queue)

                # if there is not any info yet available, return an empty dictionary
                if not self.updated:
                        self.log.debug('getInfo: no info yet')
                        return {}

                if not self.error:
                        self.status = self.__analyzeoutput(self.output, 'jobStatus', queue)
                        out = self.status
                else:
                        out = {}

                self.log.debug('getInfo[%s]: Leaving with output %s' %(queue, out))
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

                # removing temporarily (?) globusStatus from condor_q
                # it makes no sense with condor-C
                # until we figure out if we need two plugins or not
                # I just remove it
                #
                #querycmd += " -format ' globusStatus=%d' GlobusStatus"

                # removing temporarily (?) Environment from the query 
                #querycmd += " -format ' MATCH_APF_QUEUE=%s' MATCH_APF_QUEUE"
                #querycmd += " -format ' %s\n' Environment"
                querycmd += " -format ' MATCH_APF_QUEUE=%s' MATCH_APF_QUEUE"

                # I put jobStatus at the end, because all jobs have that variable
                # defined, so there is no risk is undefined and therefore the 
                # \n is never called
                querycmd += " -format ' jobStatus=%d\n' jobStatus"
                querycmd += " -xml"

                self.log.debug('__update: Querying cmd = %s' %querycmd.replace('\n','\\n'))

                self.err, self.output = commands.getstatusoutput(querycmd)
                self.updated = True

                self.log.debug('__update: Leaving.')

        def __sleep(self):
                # FIXME: temporary solution

                self.log.debug('__sleep: Starting.')
                sleeptime = self.wmsqueue.fcl.getint('Factory', 'batchstatussleep')
                time.sleep(sleeptime)
                self.log.debug('__sleep: Leaving.')

        def __analyzeoutput(self, output, key, queue):
                '''
                ancilla method to analyze the output of the condor_q command
                        - output is the output of the condor_q command, in XML format
                        - key is the pattern that counts, i.e. 'jobStatus'
                        - queue is the quename why want to analyze this time
                '''

                self.log.debug('__analyzeoutput[%s]: Starting with inputs: output=%s key=%s queue=%s' %(queue, output, key, queue))
 
                output_dic = {}
 
                if not output:
                        # FIXME: temporary solution
                        self.log.debug('__analyzeoutput: Leaving and returning %s' %output_dic)
                        return output_dic
 
                xmldoc = xml.dom.minidom.parseString(output).documentElement
 
                for c in self.__listnodesfromxml(xmldoc, 'c') :
                                node_dic = self.__node2dic(c)
 
                                if not node_dic.has_key('MATCH_APF_QUEUE'.lower()):
                                        continue
                                if not node_dic.has_key(key.lower()):
                                        continue
                                # if the line had everything, we keep searching
                                if node_dic['MATCH_APF_QUEUE'.lower()] == queue:
                                        code = node_dic[key.lower()]
                                        if code not in output_dic.keys():
                                                output_dic[code] = 1
                                        else:
                                                output_dic[code] += 1
                self.log.debug('__analyzeoutput[%s]: Leaving and returning %s' %(queue, output_dic))
                return output_dic
 
        def __listnodesfromxml(self, xmldoc, tag):
                return xmldoc.getElementsByTagName(tag)
 
        def __node2dic(self, node):
                '''
                parses a node in an xml doc, as it is generated by 
                xml.dom.minidom.parseString(xml).documentElement
                and returns a dictionary with the relevant info. 
                An example of output looks like
                        {'globusStatus':'32', 
                         'MATCH_APF_QUEUE':'UC_ITB', 
                         'jobStatus':'1'
                        }        
                '''

                dic = {}
                for child in node.childNodes:
                        if child.nodeType == child.ELEMENT_NODE:
                                key = child.attributes['n'].value
                                # the following 'if' is to protect us against
                                # all condor_q versions format, which is kind of 
                                # weird:
                                #       - there are tags with different format, with no data
                                #       - jobStatus doesn't exist. But there is JobStatus
                                if len(child.childNodes[0].childNodes) > 0:
                                        value = child.childNodes[0].firstChild.data
                                        dic[key.lower()] = str(value)
                return dic

###        def __analyzeoutput(self, output, key, queue):
###                '''
###                ancilla method to analyze the output of the condor_q command
###                        - output is the output of the command
###                        - key is the pattern that counts
###                '''
###        
###                self.log.debug('__analyzeoutput: Starting with inputs: output=%s key=%s queue=%s' %(output, key, queue))
###        
###                output_dic = {}
###        
###                if not output: 
###                        # FIXME: temporary solution
###                        self.log.debug('__analyzeoutput: Leaving and returning %s' %output_dic)
###                        return output_dic        
###        
###                lines = output.split('\n')
###                for line in lines:
###                        dic = self.__line_to_dict(line)
###                        # check that the line had everything we are interested in
###                        if 'MATCH_APF_QUEUE' not in dic.keys():
###                                continue
###                        if key not in dic.keys():
###                                continue
###                        # if the line had everything, we keep searching
###                        if dic['MATCH_APF_QUEUE'] == queue:
###                                code = dic[key]
###                                if code not in output_dic.keys():
###                                        output_dic[code] = 1
###                                else:
###                                        output_dic[code] += 1 
###
###                self.log.debug('__analyzeoutput: Leaving and returning %s' %output_dic)
###                return output_dic

###        def __line_to_dict(self, line):
###                '''
###                method ancilla to convert each line from the output of condor_q
###                into a dictionary
###                '''
###                d = {}
###                tokens = line.split()
###                for token in tokens: 
###                        if token.find('=') != -1: 
###                                #key, value = token.split('=')
###                                # we need to consider the possibility that the 
###                                # value includes itself the char '='
###                                # so we will consider as key the first word
###                                # after splitting, and as value the rest
###                                key = token.split('=')[0]
###                                value = '='.join(token.split('=')[1:])
###                                d[key] = value 
###                return d

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

