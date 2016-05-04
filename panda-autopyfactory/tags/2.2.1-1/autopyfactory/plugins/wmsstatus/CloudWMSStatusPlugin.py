#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#

import subprocess
import logging
import time
import threading
import traceback
import xml.dom.minidom

from autopyfactory.interfaces import BatchStatusInterface
from autopyfactory.factory import BatchStatusInfo
from autopyfactory.factory import QueueInfo
from autopyfactory.factory import Singleton 

from autopyfactory.info import InfoContainer
from autopyfactory.info import WMSQueueInfo


__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CloudWMSSatusPlugin(threading.Thread, BatchStatusInterface):
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
    
    def __init__(self, apfqueue):
        try:
            threading.Thread.__init__(self) # init the thread
            
            self.log = logging.getLogger("main.batchstatusplugin[singleton created by %s]" %apfqueue.apfqname)
            self.log.debug('BatchStatusPlugin: Initializing object...')
            self.stopevent = threading.Event()

            # to avoid the thread to be started more than once
            self.__started = False

            self.apfqueue = apfqueue
            self.fconfig = apfqueue.fcl.config          
            self.apfqname = apfqueue.apfqname
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchstatus.condor.sleep')
            self.currentinfo = None              

            # ================================================================
            #                     M A P P I N G S 
            # ================================================================
            
            self.jobstatus2info = {'0': 'ready',
                                   '1': 'ready',
                                   '2': 'running',
                                   '3': 'done',
                                   '4': 'done',
                                   '5': 'failed',
                                   '6': 'running'}

            # variable to record when was last time info was updated
            # the info is recorded as seconds since epoch
            self.lasttime = 0
            self._checkCondor()
            self.log.info('BatchStatusPlugin: Object initialized.')
        except Exception, ex:
            self.log.error("BatchStatusPlugin object initialization failed. Raising exception")
            raise ex

    def _checkCondor(self):
        '''
        Perform sanity check on condor environment.
        Does condor_q exist?
        Is Condor running?
        '''
        pass

    def getInfo(self, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a condor_q command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('getInfo: Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.debug('getInfo: Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.debug('getInfo: Info too old. Leaving and returning None.')
            return None
        else:                    
            self.log.debug('getInfo: Leaving and returning info of %d entries.' % len(self.currentinfo))
            return self.currentinfo


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
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.debug("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.debug('run: Leaving')

    def _update(self):
        '''        
        Query Condor for job status, validate ?, and populate BatchStatusInfo object.
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

        The JobStatus code indicates the current Condor status of the job.
        
                Value   Status                            
                0       U - Unexpanded (the job has never run)    
                1       I - Idle                                  
                2       R - Running                               
                3       X - Removed                              
                4       C -Completed                            
                5       H - Held                                 
                6       > - Transferring Output

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

        self.log.debug('_update: Starting.')
        
        try:
            strout = self._querycondor()
            outlist = self._parseoutput(strout)
            aggdict = self._aggregateinfo(outlist)
            newinfo = self._map2info(aggdict)
            self.log.info("Replacing old info with newly generated info.")
            self.currentinfo = newinfo
        except Exception, e:
            self.log.error("_update: Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        self.log.debug('__update: Leaving.')

    def _querycondor(self):
        '''
            Query condor for all job info and return xml representation string
            for further processing.
        
        '''
        self.log.debug('_querycondor: Starting.')
        querycmd = "condor_q"
        
        # removing temporarily (?) Environment from the query 
        #querycmd += " -format ' %s\n' Environment"
        querycmd += " -format ' MATCH_APF_QUEUE=%s' match_apf_queue"

        # I put jobStatus at the end, because all jobs have that variable
        # defined, so there is no risk is undefined and therefore the 
        # \n is never called
        querycmd += " -format ' JobStatus=%d\n' jobstatus"
        querycmd += " -format ' GlobusStatus=%d\n' globusstatus"
        querycmd += " -xml"

        self.log.debug('_querycondor: Querying cmd = %s' %querycmd.replace('\n','\\n'))

        # Run and time condor_q
        # XXXXXX FIXME
        # As condor_q can take a long time, we need to wrap this in a fully protected 
        # timed command
        # See http://stackoverflow.com/questions/1191374/subprocess-with-timeout
        #
        before = time.time()          
        p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
        out = None
        (out, err) = p.communicate()
        delta = time.time() - before
        self.log.debug('_querycondor: it took %s seconds to perform the query' %delta)
        self.log.info('Condor query: %s seconds to perform the query' %delta)
        if p.returncode == 0:
            self.log.debug('_querycondor: Leaving with OK return code.') 
        else:
            self.log.warning('_querycondor: Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
        self.log.debug('_querycondor: Leaving. Out is %s' % out)
        return out



    def _parseoutput(self, output):
        '''
        parses XML output of condor_q command with an arbitrary number of attribute -format arguments,
        and creates a Python List of Dictionaries of them. 
        
        Input:
        <!DOCTYPE classads SYSTEM "classads.dtd">
        <classads>
            <c>
                <a n="match_apf_queue"><s>BNL_ATLAS_1</s></a>
                <a n="jobstatus"><i>2</i></a>
            </c>
            <c>
                <a n="match_apf_queue"><s>BNL_ATLAS_1</s></a>
                <a n="jobstatus"><i>1</i></a>
            </c>
        </classads>                       
        
        Output:
        [ { 'match_apf_queue' : 'BNL_ATLAS_1',
            'jobstatus' : '2' },
          { 'match_apf_queue' : 'BNL_ATLAS_1',
            'jobstatus' : '1' }
        ]
        '''
        self.log.debug('_parseoutput: Starting.')                

        xmldoc = xml.dom.minidom.parseString(output).documentElement
        nodelist = []
        for c in self._listnodesfromxml(xmldoc, 'c') :
            node_dict = self._node2dict(c)
            nodelist.append(node_dict)            
        self.log.debug('_parseoutput: Leaving and returning list of %d entries.' %len(nodelist))
        self.log.info('Got list of %d entries.' %len(nodelist))
        return nodelist


    def _listnodesfromxml(self, xmldoc, tag):
        return xmldoc.getElementsByTagName(tag)

    def _node2dict(self, node):
        '''
        parses a node in an xml doc, as it is generated by 
        xml.dom.minidom.parseString(xml).documentElement
        and returns a dictionary with the relevant info. 
        An example of output looks like
               {'globusstatus':'32', 
                 'match_apf_queue':'UC_ITB', 
                 'jobstatus':'1'
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


    def _aggregateinfo(self, input):
        '''
        This function takes a list of job status dicts, and aggregates them by queue,
        ignoring entries without MATCH_APF_QUEUE
        
        Assumptions:
          -- Input has a single level of nesting, and consists of dictionaries.
          -- You are only interested in the *count* of the various attributes and value 
          combinations. 
         
        Example input:
        [ { 'match_apf_queue' : 'BNL_ATLAS_1',
            'jobstatus' : '2' },
          { 'match_apf_queue' : 'BNL_ATLAS_1',
            'jobstatus' : '1' }
        ]                        
        
        Output:
        { 'UC_ITB' : { 'jobstatus' : { '1': '17',
                                       '2' : '24',
                                       '3' : '17',
                                     },
                       'globusstatus' : { '1':'13',
                                          '2' : '26',
                                          }
                      },
        { 'BNL_TEST_1' :{ 'jobstatus' : { '1':  '7',
                                          '2' : '4',
                                          '3' : '6',
                                     },
                       'globusstatus' : { '1':'12',
                                          '2' : '46',
                                          }
                      },             
        '''
        self.log.debug('_aggregateinfo: Starting with list of %d items.' % len(input))
        queues = {}
        for item in input:
            if not item.has_key('match_apf_queue'):
                # This job is not managed by APF. Ignore...
                continue
            apfqname = item['match_apf_queue']
            # get current dict for this apf queue
            try:
                qdict = queues[apfqname]
            # Or create an empty one and insert it.
            except KeyError:
                qdict = {}
                queues[apfqname] = qdict    
            
            # Iterate over attributes and increment counts...
            for attrkey in item.keys():
                # ignore the match_apf_queue attrbute. 
                if attrkey == 'match_apf_queue':
                    continue
                attrval = item[attrkey]
                # So attrkey : attrval in joblist
                
                
                # Get current attrdict for this attribute from qdict
                try:
                    attrdict = qdict[attrkey]
                except KeyError:
                    attrdict = {}
                    qdict[attrkey] = attrdict
                
                try:
                    curcount = qdict[attrkey][attrval]
                    qdict[attrkey][attrval] = curcount + 1                    
                except KeyError:
                    qdict[attrkey][attrval] = 1
        self.log.debug('_aggregateinfo: Returning dict with %d queues.' % len(queues))            
        self.log.info('Aggregate: Created dict with %d queues.' % len(queues))
        return queues

    def _map2info(self, input):
        '''
        This takes aggregated info by queue, with condor/condor-g specific status totals, and maps them 
        to the backend-agnostic APF BatchStatusInfo object.
        
           APF             Condor-C/Local              Condor-G/Globus 
        .pending           Unexp + Idle                PENDING
        .running           Running                     RUNNING
        .suspended         Held                        SUSPENDED
        .done              Completed                   DONE
        .unknown           
        .error
        
        Primary attributes. Each job is in one and only one state:
            pending            job is queued (somewhere) but not running yet.
            running            job is currently active (run + stagein + stageout)
            error              job has been reported to be in an error state
            suspended          job is active, but held or suspended
            done               job has completed
            unknown            unknown or transient intermediate state
            
        Secondary attributes. Each job may be in more than one category. 
            transferring       stagein + stageout
            stagein
            stageout           
            failed             (done - success)
            success            (done - failed)
            ?
        
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
        Input:
          Dictionary of APF queues consisting of dicts of job attributes and counts.
          { 'UC_ITB' : { 'Jobstatus' : { '1': '17',
                                       '2' : '24',
                                       '3' : '17',
                                     },
                       'Globusstatus' : { '1':'13',
                                          '2' : '26',
                                          }
                      }
           }          
        Output:
            A BatchStatusInfo object which maps attribute counts to generic APF
            queue attribute counts. 
        '''


        self.log.debug('_map2info: Starting.')
        wmsstatusinfo = InfoContainer('wmscloud')
        for site in input.keys():
                qi = WMSQueueInfo()
                wmsstatusinfo[site] = qi
                attrdict = input[site]
               
                valdict = attrdict['jobstatus']
                qi.fill(valdict, mappings=self.jobstatus2info)
                        
        wmsstatusinfo.lasttime = int(time.time())
        self.log.debug('_map2info: Returning WMSStatusInfo: %s' % wmsstatusinfo)
        for site in wmsstatusinfo.keys():
            self.log.debug('_map2info: Queue %s = %s' % (site, wmsstatusinfo[site]))           
        return wmsstatusinfo

#        self.log.debug('_map2info: Starting.')
#        bsi = BatchStatusInfo()
#        for site in input.keys():
#            qi = QueueInfo()
#            bsi.queues[site] = qi
#            attrdict = input[site]
#            
#            # use finer-grained globus statuses in preference to local summaries. 
#            if 'globusstatus' in attrdict.keys():
#                valdict = attrdict['globusstatus']
#                for status in valdict.keys():
#                    valct = valdict[status]
#                    if status == '1':
#                        qi.pending += valct                        
#                    elif status == '2':
#                        qi.running += valct
#                    elif status == '4':
#                        # FAILED jobs are done
#                        qi.done += valct
#                    elif status == '8':
#                        # DONE jobs are done
#                        qi.done += valct
#                    elif status == '16':
#                        qi.suspended += valct
#                    elif status == '32':
#                        # UNSUBMITTED jobs are pending
#                        qi.pending += valct
#                    elif status == '64':
#                        # STAGE-IN jobs are running
#                        qi.running += valct
#                    elif status == '128':
#                        # STAGE-OUT jobs are running
#                        qi.running += valct                
#                
#            # must be a local-only job.
#            else:
#                valdict = attrdict['jobstatus']
#                for status in valdict.keys():
#                    valct = valdict[status]
#                    if status == '0':
#                        # Unexpanded jobs are pending
#                        qi.pending += valct                        
#                    elif status == '1':
#                        qi.pending += valct
#                    elif status == '2':
#                        qi.running += valct
#                    elif status == '3':
#                        # Removed jobs are done. 
#                        qi.done += valct
#                    elif status == '4':
#                        qi.done += valct
#                    elif status == '5':
#                        # Held jobs are suspended
#                        qi.suspended += valct
#                    elif status == '6':
#                        # Transferring jobs are running
#                        qi.running += valct
#        bsi.lasttime = int(time.time())
#        self.log.debug('_map2info: Returning BatchStatusInfo: %s' % bsi)
#        for site in bsi.queues.keys():
#            self.log.debug('_map2info: Queue %s = %s' % (site, bsi.queues[site]))           
#        return bsi


    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving')


def test():
    list =  [ { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '2' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '1' },
                           { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '1' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '1' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '2' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' }
            ] 
    
if __name__=='__main__':
    pass



