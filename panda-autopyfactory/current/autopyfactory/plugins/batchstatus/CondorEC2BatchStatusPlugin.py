#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
# Dedicated to handling VM job submissions and VM pool startds. 
#   

import commands
import subprocess
import logging
import os
import time
import threading
import traceback
import xml.dom.minidom
import sys

from datetime import datetime
from pprint import pprint
from autopyfactory.interfaces import BatchStatusInterface
from autopyfactory.factory import BatchStatusInfo
from autopyfactory.factory import QueueInfo
from autopyfactory.factory import Singleton, CondorSingleton
from autopyfactory.info import InfoContainer
from autopyfactory.info import BatchStatusInfo
import autopyfactory.utils as utils

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorEC2BatchStatusPlugin(threading.Thread, BatchStatusInterface):
    '''
    BatchStatusPlugin intended to handle CloudInstances, i.e. a combination of a 
    submitted VM job AND startd information gathered from 'condor_status -master' output. 

    It adds new statuses: Retiring and Retired. 
    It adds correlation between VM jobs and startds in pool so that the startd status (Idle, 
    Retiring, Retired) appears in VM job attributes in the info object.  

    '''   
    __metaclass__ = CondorSingleton 
    
    def __init__(self, apfqueue, **kw):
        threading.Thread.__init__(self) # init the thread
        
        self.log = logging.getLogger("main.batchstatusplugin[singleton created by %s with condor_q_id %s]" %(apfqueue.apfqname, kw['condor_q_id']))
        self.log.debug('BatchStatusPlugin: Initializing object...')
        self.stopevent = threading.Event()

        # to avoid the thread to be started more than once
        self.__started = False

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        self.sleeptime = 30
        self.queryargs = ""
        self.condoruser = "apf"
        self.factoryid = "apf-mock-test"
        
        try:
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchstatus.condor.sleep')
            self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs', logger=self.log) 

        except AttributeError:
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")

        # This is per-job info
        # Information is in the form of a dictionary of lists of EC2JobInfo objecsts. 
        # the key of the dictionary is the APF_MATCH_QUEUE
        #
        self.currentjobs = None      
        
        # This is job statistic info, a BatshStatusInfo object. 
        self.currentinfo = None

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================          
       
        self.jobstatus2info = {'0': 'pending',
                               '1': 'pending',
                               '2': 'running',
                               '3': 'done',
                               '4': 'done',
                               '5': 'suspended',
                               '6': 'running'}

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        self._checkCondor()
        self.log.info('BatchStatusPlugin: Object initialized.')
        
        #except Exception, ex:
        #    self.log.error("BatchStatusPlugin object initialization failed. Raising exception")
        #    raise ex

    def _checkCondor(self):
        '''
        Perform sanity check on condor environment.
        Does condor_q exist?
        Is Condor running?
        '''
        
        # print condor version
        self.log.debug('_checkCondor: condor version is: \n%s' %commands.getoutput('condor_version'))       

        # check env var $CONDOR_CONFIG
        CONDOR_CONFIG = os.environ.get('CONDOR_CONFIG', None)
        if CONDOR_CONFIG:
            self.log.debug('_checkCondor: environment variable CONDOR_CONFIG set to %s' %CONDOR_CONFIG)
        else:
            condor_config = '/etc/condor/condor_config'
            if os.path.isfile(condor_config):
                self.log.debug('_checkCondor: using condor config file: %s' %condor_config)
            else:
                condor_config = '/usr/local/etc/condor_config'
                if os.path.isfile(condor_config):
                    self.log.debug('_checkCondor: using condor config file: %s' %condor_config)
                else:
                    condor_config = os.path.expanduser('~condor/condor_config')
                    if os.path.isfile(condor_config):
                        self.log.debug('_checkCondor: using condor config file: %s' %condor_config)

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

    def getJobInfo(self, maxtime=0):
        '''
        Returns a list of CondorEC2JobInfo objects which include startd information. 

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('getInfo: Starting with maxtime=%s' % maxtime)
        
        if self.currentjobs is None:
            self.log.debug('getInfo: Not initialized yet. Returning None.')
            return None
        #elif maxtime > 0 and (int(time.time()) - self.currentjobs.lasttime) > maxtime:
        #    self.log.debug('getInfo: Info too old. Leaving and returning None.')
        #    return None
        else:                    
            self.log.debug('getInfo: Leaving and returning info of %d entries.' % len(self.currentinfo))
            return self.currentjobs


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

    def _querycondorxml(self):
        '''
        Return human readable info about startds. 
        '''
        cmd = 'condor_q -xml'
        self.log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
        before = time.time()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out = None
        (out, err) = p.communicate()
        delta = time.time() - before
        self.log.debug('It took %s seconds to perform the query' %delta)
        self.log.info('%s seconds to perform the query' %delta)
        if p.returncode == 0:
            self.log.debug('Leaving with OK return code.')
        else:
            self.log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
        #log.debug('Leaving. Out is %s' % out)
        return out

    def _statuscondorxml(self):
        '''
        Return human readable info about startds. 
        '''
        log = logging.getLogger()
        cmd = 'condor_status -xml'
        log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
        before = time.time()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out = None
        (out, err) = p.communicate()
        delta = time.time() - before
        log.debug('It took %s seconds to perform the query' %delta)
        log.info('%s seconds to perform the query' %delta)
        if p.returncode == 0:
            log.debug('Leaving with OK return code.')
        else:
            log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
        #log.debug('Leaving. Out is %s' % out)
        return out


    def _update(self):
        '''        
            Query Condor for job status, create JobInfo objects. 
            Query condor_status for startd status, add to relevant JobInfo by instanceID
                Update currentjobs (list of JobInfo objects)
            
            Aggregate resulting objects for statistics, creating BatchStatusInfo object 
            update currentinfo
                
        '''
        self.log.debug('_update: Starting.')
       
        if not utils.checkDaemon('condor'):
            self.log.warning('_update: condor daemon is not running. Doing nothing')
        else:
            try:
                xmlout = self._querycondorxml()
                if not xmlout:
                    self.log.warning('_update: output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
                else:
                    dictlist = self._parseoutput(xmlout)
                    jl = self._dicttojoblist(dictlist)
                    self.log.debug("Created indexed joblist of length %d" % len(jl))
                    self.currentjobs = jl

            except Exception, e:
                self.log.error("_update: Exception: %s" % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc())            
            
            try:
                xmlout = self._statuscondorxml()
                if not xmlout:
                    self.log.warning('_update: output of _statuscondor is not valid. Not parsing it. Skip to next loop.') 
                else:
                    dictlist = self._parseoutput(xmlout)
                    sl = self._dicttoslotlist(dictlist)
                    self.log.debug("Created SlotInfo list of length %d" % len(sl))
                    stdlist = self._slotlisttostartdlist(sl)
                    # XXXXX
                    
                    self.log.info("Replacing old info with newly generated info.")

            except Exception, e:
                self.log.error("_update: Exception: %s" % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc()) 
    
        self.log.debug('__update: Leaving.')

    def _dicttojoblist(self,nodelist):
        '''
        Takes in list of dictionaries:
        
           [ { a:b,
               b:c,
               },
               {n:m,
                x:y}
            ]
        and returns a dictionary of EC2JobInfo objects, indexed by 'match_apf_queue' value. 
            { 'queue1' : [ EC2JobInfo, EC2JobInfo,],
              'queue2' : [ EC2JobInfo, EC2JobInfo,],
            }
        '''
        joblist = []
        qd = {}
        if len(nodelist) > 0:
            for n in nodelist:
                j = CondorEC2JobInfo(n)
                joblist.append(j)
            
            indexhash = {}
            for j in joblist:
                try:
                    i = j.match_apf_queue
                    indexhash[i] = 1
                except:
                    # We don't care about jobs not from APF
                    pass
    
            for k in indexhash.keys():
            # Make a list for jobs for each apfqueue
                qd[k] = []
            
            # We can now safely do this..
            for j in joblist:
                try:
                    index = j.match_apf_queue
                    qjl = qd[index]
                    qjl.append(j)
                except:
                    # again we don't care about non-APF jobs
                    pass    
                
        self.log.info("Made job list of length %d" % len(joblist))
        self.log.info("Made a job info dict of length %d" % len(qd))
        return qd

    def _dicttoslotlist(self, nodelist):
        '''
        Takes the list of dicts of all jobslots (from condor_status) and constructs
        CondorStartdInfo objects, one per startd.         
        '''
        
        
        slotlist = []
        for n in nodelist:
            try:
                ec2iid = n['ec2instanceid']
                state = n['state']
                act = n['activity']
                slots = n['totalslots']
                machine = n['machine']
                j = CondorSlotInfo(ec2iid, machine, state, act)
                slotlist.append(j)
            except Exception, e:
                log.error("Bad node. Error: %s" % str(e))
        return slotlist

    def _slotlisttostartdlist(self, slotlist):
        '''
        Take a list of slotinfo objects and returns a dict of StartdInfo objects, with their instanceID as the key.        
        '''
        startdlist = {}
        for si in slotlist:
            try:
                stdinfo = startdlist[si.instanceid]
                stdinfo.add(si)
            except:
                startdlist[si.instanceid] = CondorStartdInfo(si)
        self.log.info("Created startdlist of length %d" % len(startdlist))
        return startdlist


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



    def _xml2nodelist(input):
        '''
        
        
        '''
        xmldoc = xml.dom.minidom.parseString(input).documentElement
        nodelist = []
        for c in self.listnodesfromxml(xmldoc, 'c') :
            node_dict = self.node2dict(c)
            nodelist.append(node_dict)
        log.debug('_xml2nodelist: Leaving and returning list of %d entries.' %len(nodelist))
        log.info('Got list of %d entries.' %len(nodelist))
        return nodelist



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
                if len(child.childNodes[0].childNodes) > 0:
                    value = child.childNodes[0].firstChild.data
                    dic[key.lower()] = str(value)
        return dic

    def _indexbyqueue(self, input):
        '''
        This function takes a list of job status dicts, and indexes them by queue 
        
        Example input:
        [ { 'match_apf_queue' : 'BNL_ATLAS_1',
            'jobstatus' : '2' },
          { 'match_apf_queue' : 'BNL_ATLAS_1',
            'jobstatus' : '1' },
           { 'match_apf_queue' : 'BNL_ATLAS_2',
            'jobstatus' : '1' },        
        ]                        
                
        ''' 

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
            running            job is currently active (run + stagein + stageout + retiring)
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
            retiring
        
          The JobStatus code indicates the current status of the job.
            
                    Value   Status
                    0       Unexpanded (the job has never run)
                    1       Idle
                    2       Running
                    3       Removed
                    4       Completed
                    5       Held
                    6       Transferring Output

        Input:
          Dictionary of APF queues consisting of dicts of job attributes and counts.
          { 'UC_ITB' : { 'Jobstatus' : { '1': '17',
                                       '2' : '24',
                                       '3' : '17',
                                     },
                      }
           }          
        
        Output:
            A BatchStatusInfo object which maps attribute counts to generic APF
            queue attribute counts. 
        '''
        self.log.debug('_map2info: Starting.')
        batchstatusinfo = InfoContainer('batch', BatchStatusInfo())
        for site in input.keys():
            qi = BatchStatusInfo()
            batchstatusinfo[site] = qi
            attrdict = input[site]
            
            # use finer-grained globus statuses in preference to local summaries, if they exist. 
            if 'globusstatus' in attrdict.keys():
                valdict = attrdict['globusstatus']
                qi.fill(valdict, mappings=self.globusstatus2info)
            # must be a local-only job or other. 
            else:
                valdict = attrdict['jobstatus']
                qi.fill(valdict, mappings=self.jobstatus2info)
                    
        batchstatusinfo.lasttime = int(time.time())
        self.log.debug('_map2info: Returning BatchStatusInfo: %s' % batchstatusinfo)
        for site in batchstatusinfo.keys():
            self.log.debug('_map2info: Queue %s = %s' % (site, batchstatusinfo[site]))           
        return batchstatusinfo

    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving')



########################################################################
# New classes and functions for correlating VM Jobs and Cloud startds. 
#
########################################################################3

class CondorEC2JobInfo(object):
    '''
    This object represents an EC2 Condor job resulting in a startd connecting back to 
    the local pool. It is only relevant to this Status Plugin.     
        
    '''

    def __init__(self, dict):
        '''
        Creates JobInfo object from arbitrary dictionary of attributes. 
        '''
        log = logging.getLogger()
        self.jobattrs = []
        for k in dict.keys():
            self.__setattr__(k,dict[k])
            self.jobattrs.append(k)
        self.jobattrs.sort()
        log.debug("Made CondorJobInfo object with %d attributes" % len(self.jobattrs))    
        
    def __str__(self):
        attrstoprint = ['ec2instancename',
                        'ec2instanctype',
                        'enteredcurrentstatus',
                        'jobstatus',
                        'match_apf_queue',
                        ]   
               
        s = "[ CondorJob: %s.%s] " % (self.clusterid, self.autoclusterid)
        for k in self.jobattrs:
            if k in attrstoprint:
                s += " %s=%s " % ( k, self.__getattribute__(k))
        return s
    
    def __repr__(self):
        s = str(self)
        return s

class CondorSlotInfo(object):
    '''
    Info object to represent a slot. 
    '''
    def __init__(self, instanceid, machine, state, activity):
        '''
        instanceID is self-explanatory
        machine is the full internal/local hostname (to allow condor_off)

        States: Owner 
                Matched 
                Claimed 
                Unclaimed 
                Preempting 
                Backfill
        Activities: 
                Busy 
                Idle 
                Retiring 
                Suspended

        '''
        self.id = instanceid
        self.machine = machine
        self.state = state
        self.activity = activity
      
    def __str__(self):
        s = "CondorSlotInfo: %s %s %s %s\n" % (self.id, self.machine,self.state, self.activity)
        return s

    def __repr__(self):
        s = str(self)
        return s    




class CondorStartdInfo(object):
    '''
    Info object to represent a startd on the cloud. 
    If it has multiple slots, we need to calculate overall state/activity carefully. 
    
           
    '''
    def __init__(self, slotinfo):
        '''
        instanceID is self-explanatory
        machine is the full internal/local hostname (to allow condor_off)

        States: Owner 
                Matched 
                Claimed 
                Unclaimed 
                Preempting 
                Backfill
        Activities: 
                Busy 
                Idle 
                Retiring 
                Suspended

        '''
        self.id = instanceid
        self.machine = machine
        self.state = {}
        self.activity = {}
        self.state[state] = 1
        self.activity[activity] = 1
    
    def add(self, slotinfo):
        '''
        Add the information for a slot to this startd. 
        
        '''    
        if self.id == slotinfo.id and self.machine == slotinfo.machine:
            try:
                self.state[slotinfo.state] += 1
            except:
                self.state[slotinfo.state] = 1
            try:
                self.activity[slotinfo.activity] += 1
            except:
                self.activity[slotinfo.activity] = 1            
        else:
            self.log.warning("Attempt to add a mismatched slotinfo to an existing StartdInfo object.")
        
        
    def __str__(self):
        s = "CondorStartdInfo: %s %s" % (self.id, self.machine)
        stk =  self.state.keys()
        stk.sort()
        for st in stk:
            s += " %s: %d" % (st, self.state[st])
        ack =  self.activity.keys()
        ack.sort()
        for ac in ack:
            s += " %s: %d" % (ac, self.activity[ac])
        s += "\n" 
        return s

    def __repr__(self):
        s = str(self)
        return s    

class MockAPFQueue(object):

    def __init__(self, apfqname):
        self.apfqname = apfqname
        



def statuscondor():
    '''
    Return human readable info about startds. 
    '''
    log = logging.getLogger()
    cmd = 'condor_status -xml'
    log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)
    log.info('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    #log.debug('Leaving. Out is %s' % out)
    return out


def querycondorxml():
    '''
    Return human readable info about startds. 
    '''
    log = logging.getLogger()
    cmd = 'condor_q -xml'
    log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)
    log.info('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    #log.debug('Leaving. Out is %s' % out)
    return out


def xml2nodelist(input):
    log = logging.getLogger()
    xmldoc = xml.dom.minidom.parseString(input).documentElement
    nodelist = []
    for c in listnodesfromxml(xmldoc, 'c') :
        node_dict = node2dict(c)
        nodelist.append(node_dict)
    log.debug('_parseoutput: Leaving and returning list of %d entries.' %len(nodelist))
    log.info('Got list of %d entries.' %len(nodelist))
    return nodelist

def listnodesfromxml( xmldoc, tag):
    return xmldoc.getElementsByTagName(tag)


def node2dict( node):
    '''
 
    '''
    dic = {}
    for child in node.childNodes:
        if child.nodeType == child.ELEMENT_NODE:
            key = child.attributes['n'].value
            if len(child.childNodes[0].childNodes) > 0:
                value = child.childNodes[0].firstChild.data
                dic[key.lower()] = str(value)
    return dic
   

def getJobInfo():
        log = logging.getLogger()
        xml = querycondorxml()
        nl = xml2nodelist(xml)
        log.info("Got node list of length %d" % len(nl))
        joblist = []
        qd = {}
        if len(nl) > 0:
            for n in nl:
                j = CondorEC2JobInfo(n)
                joblist.append(j)
            
            indexhash = {}
            for j in joblist:
                try:
                    i = j.match_apf_queue
                    indexhash[i] = 1
                except:
                    # We don't care about jobs not from APF
                    pass
    
            for k in indexhash.keys():
            # Make a list for jobs for each apfqueue
                qd[k] = []
            
            # We can now safely do this..
            for j in joblist:
                try:
                    index = j.match_apf_queue
                    qjl = qd[index]
                    qjl.append(j)
                except:
                    # again we don't care about non-APF jobs
                    pass    
                
        log.info("Made job list of length %d" % len(joblist))
        log.info("Made a job info dict of length %d" % len(qd))
        return qd

def getStartdInfoByEC2Id():
    log = logging.getLogger()
    out = statuscondor()
    nl = xml2nodelist(out)
    infolist = {}
    for n in nl:
        #print(n)
        try:
            ec2iid = n['ec2instanceid']
            state = n['state']
            act = n['activity']
            slots = n['totalslots']
            machine = n['machine']
            j = CondorStartdInfo(ec2iid, machine, state, act)
            #log.debug("Created csdi: %s" % j)
            j.slots = slots
            infolist[ec2iid] = j
        except Exception, e:
            log.error("Bad node. Error: %s" % str(e))
    return infolist
    

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)

def test2():
    a = MockAPFQueue('BNL_CLOUD-ec2-spot')
    bsp = CondorEC2BatchStatusPlugin(a, condor_q_id='local')
    bsp.start()
    while True:
        try:
            time.sleep(15)
        except KeyboardInterrupt:
            bsp.stopevent.set()
            sys.exit(0)
    

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    test2()




