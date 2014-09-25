#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
# Dedicated to handling VM job submissions and VM pool startds. 
   

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
from autopyfactory.factory import Singleton, CondorSingleton
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo

from autopyfactory.condor import checkCondor, querycondorxml, statuscondor, statuscondormaster
from autopyfactory.condor import parseoutput, xml2nodelist, node2dict 
from autopyfactory.condor import listnodesfromxml, aggregateinfo, killids
from autopyfactory.condor import mincondorversion

import autopyfactory.utils as utils

mincondorversion(8,1,1)

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
            self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs') 

        except AttributeError:
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")

        self.currentjobs = None      
        self.currentinfo = None

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================          
       
        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDOREC2BATCHSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)

        ###self.jobstatus2info = {'0': 'pending',
        ###                       '1': 'pending',
        ###                       '2': 'running',
        ###                       '3': 'done',
        ###                       '4': 'done',
        ###                       '5': 'suspended',
        ###                       '6': 'running'}

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        self.log.info('BatchStatusPlugin: Object initialized.')

    def getInfo(self, queue=None, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a condor_q command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                return self.currentinfo[queue]
            else:                    
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentinfo))
                return self.currentinfo

    def getJobInfo(self, queue=None, maxtime=0):
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
        elif maxtime > 0 and (int(time.time()) - self.currentjobs.lasttime) > maxtime:
            self.log.debug('getInfo: Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                try:
                    i =  self.currentjobs[queue]
                    self.log.debug('getInfo: Leaving and returning queue-specific JobInfo list of %d entries.' % len(i))
                    return i 
                except KeyError:
                    return None
            else:
                self.log.debug('getInfo: Leaving and returning all jobinfo w/ %d entries.' % len(self.currentjobs))
                return self.currentjobs


    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.debug('Starting')

        if not self.__started:
                self.log.debug("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.debug('Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.debug('Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.debug("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.debug('Leaving')


    def _update(self):
        '''        
            Query Condor for job status, create JobInfo objects. 
            Query condor_status -master for execute host info
            Query condor_status for startd status, adding that info to executeInfo
            
            Aggregate resulting objects for statistics, creating BatchStatusInfo object 
            update currentinfo
                
        '''
        self.log.debug('Starting.')

        exelist = None
        slotlist = None
        joblist = None

       
        if not utils.checkDaemon('condor'):
            self.log.warning('condor daemon is not running. Doing nothing')
        else:
            try:
                exelist = self._makeexelist()
                self.log.debug("exelist: %s" % exelist)
                slotlist = self._makeslotlist()
                self.log.debug("slotlist: %s" % slotlist)

                # Query condor once
                xmlout = querycondorxml()
                dictlist = parseoutput(xmlout)

                # use it to for stats and job-by-job processing...
                newinfo = self._makeinfolist(dictlist)
                self.log.debug("rawinfo: %s" % newinfo)
                
                joblist = self._makejoblist(dictlist)
                self.log.debug("rawjoblist: %s" % joblist)
                
                #Make hash of SlotInfo objects by instanceid 
                slotsbyec2id =  self._indexobjectsby(slotlist, 'instanceid')
                self.log.debug("indexed slotlist: %s" % slotsbyec2id)
                
                for exe in exelist:
                    ec2id = exe.instanceid
                    try:
                        slots = slotsbyec2id[ec2id]
                        exe.slotinfolist = slots
                    except KeyError:
                        self.log.debug("Failed to find slotinfo for ec2id %s." % ec2id)
                        # Not necessarily a problem, if node is retired. 

                                 
                # Make hash of of CondorExecuteInfo objects, indexed
                exebyec2id = self._indexobjectsby(exelist, 'instanceid')
                self.log.debug("indexed exelist: %s" % exebyec2id)
                
                # Now, add exeinfo to correct jobs, by ec2instanceid...

                for aq in joblist.keys():
                    self.log.debug("Adding exeinfo to jobs in apfqueue %s" % aq)
                    for job in joblist[aq]:
                        #self.log.debug("Handling job %s" % job) 
                        try:
                            ec2id = job.ec2instancename
                            self.log.debug("Adding exeinfo to job for ec2id: %s" % ec2id )
                            try:
                                exeinfo = exebyec2id[ec2id][0]
                                self.log.debug("Retrieved exeinfo from indexed hash for ec2id: %s" % ec2id)
                                # Should only be one per job
                                job.executeinfo = exeinfo
                                exestat = job.executeinfo.getStatus() 
                                self.log.debug("Job with exeinfo, checking status=%s" % exestat)
                                if exestat == 'retiring':
                                    self.log.debug("Found retiring, adjusting newinfo")
                                    newinfo[aq].retiring += 1
                                    newinfo[aq].running -= 1
                                elif exestat == 'retired':
                                    self.log.debug("Found retired, adjusting newinfo")
                                    newinfo[aq].retired += 1
                                    newinfo[aq].running -= 1
                                else:
                                    self.log.debug("No change to newinfo")
                                self.log.debug("Assigned exeinfo: %s to job %s" % (exeinfo, job))
                            except KeyError:
                                # New VM jobs will not have exeinfo until they start 
                                # and connect back to the pool. This is OK.  
                                pass
                        except AttributeError:
                            pass
                            #self.log.exception("Got AttributeError during exeinfo. Could be OK.")
                            # OK, not all jobs will be ec2 jobs. 
                               
                # Update current info references
                # curr
                self.currentjobs = joblist
                self.currentinfo = newinfo
            
            except Exception, e:
                self.log.exception("Problem handling Condor info.")

        self.log.debug('_ Leaving.')


    def _makeexelist(self):
        '''
        Create and return a list of CondorExecutInfo objects based on the output
        of condor_status -master -xml
        
        '''
        exelist = []
        xmlout = statuscondormaster()
        if not xmlout:
            self.log.warning('output of statuscondormaster() is not valid. Not parsing it. Skip to next loop.') 
        else:
            dictlist = parseoutput(xmlout)
            exelist = self._dicttoexelist(dictlist)
            self.log.debug("Created CondorExecuteInfo list of length %d" % len(exelist))
        return exelist
        
        
    def _makeslotlist(self):
        slotlist = []
        xmlout = statuscondor()
        if not xmlout:
            self.log.warning('output of statuscondor() is not valid. Not parsing it. Skip to next loop.') 
        else:
            dictlist = parseoutput(xmlout)
            slotlist = self._dicttoslotlist(dictlist)
            self.log.debug("Created CondorSlotInfo list of length %d" % len(slotlist))
        return slotlist
   
    def _makejoblist(self, dictlist):
        joblist = {}
        if not dictlist:
            self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
        else:
            joblist = self._dicttojoblist(dictlist)
            self.log.debug("Created indexed joblist of length %d" % len(joblist))
            self.currentjobs = joblist
        return joblist


    def _makeinfolist(self, dictlist):
        '''
        Makes list of CondorEC2JobInfo objects 
        Input may be None        
        '''
        newinfo = None
        if not dictlist:
            self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
        else:
            aggdict = aggregateinfo(dictlist)
            newinfo = self._map2info(aggdict)
        return newinfo


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
            
        Note: ONLY creates EC2JobInfo objects from jobs that have an EC2InstanceName attribute!!    
        '''
        joblist = []
        qd = {}
        if len(nodelist) > 0:
            for n in nodelist:
                try:
                    ec2in = n['ec2instancename']
                    j = CondorEC2JobInfo(n)
                    joblist.append(j)
                    self.log.debug("Found EC2 job with instancename %s" % ec2in)
                except KeyError:
                    self.log.debug("Discarding non-EC2 job...")
            
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
                self.log.error("Bad node. Error: %s" % str(e))
        return slotlist

    def _dicttoexelist(self, nodelist):
        '''
        Takes the list of dicts of all masters (from condor_status) and constructs
        CondorExecuteInfo objects, one per startd.         
        '''
        
        exelist = []
        for n in nodelist:
            try:
                ec2iid = n['ec2instanceid']
                machine = n['machine']
                hostname = n['ec2publicdns']
                j = CondorExecuteInfo(ec2iid, machine, hostname)
                self.log.debug("Creating CondorExecuteInfo: %s" % j)
                exelist.append(j)
            except Exception, e:
                self.log.warning("Bad node. May be OK since not all nodes ec2: %s" % str(e))
        return exelist

    def _slotlisttostartdlist(self, slotlist):
        '''
        Take a list of slotinfo objects and returns a dict of StartdInfo objects, 
        with their instanceID as the key.        
        '''
        exelist = {}
        for si in slotlist:
            try:
                stdinfo = startdlist[si.instanceid]
                self.log.debug("Found existing CondorStartdInfo object, adding slotinfo...")
                stdinfo.add(si)
            except KeyError:
                self.log.debug("KeyError. Creating new CondorStartdInfo object...")
                startdlist[si.instanceid] = CondorStartdInfo(si)
        self.log.info("Created startdlist of length %d" % len(startdlist))
        return startdlist

    def _indexobjectsby(self, objlist, idxattr):
        '''
        Takes a list of any object, and returns a hash of lists of those
        objects. 
        If the objects don't have the idxattribute, they are left out. 
        If objlist is empty, return empty hash
                
        '''
        hash = {}
        for o in objlist:
            try:
                idx = getattr( o , idxattr)
                try:
                    olist = hash[idx]
                except KeyError:
                    olist = []
                olist.append(o)
                hash[idx] = olist
            except KeyError:
                pass
        self.log.debug("Constructed indexed hash: %s" % hash)
        return hash
        

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
        self.log.debug('Starting.')
        batchstatusinfo = BatchStatusInfo()
        for site in input.keys():
            qi = QueueInfo()
            batchstatusinfo[site] = qi
            attrdict = input[site]
            valdict = attrdict['jobstatus']
            qi.fill(valdict, mappings=self.jobstatus2info)
                    
        batchstatusinfo.lasttime = int(time.time())
        self.log.debug('Returning BatchStatusInfo: %s' % batchstatusinfo)
        for site in batchstatusinfo.keys():
            self.log.debug('Queue %s = %s' % (site, batchstatusinfo[site]))           
        return batchstatusinfo

    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('Leaving')



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
        ec2instancename -> ec2instanceid
        
        '''
        self.log = logging.getLogger()
        self.jobattrs = []
        for k in dict.keys():
            self.__setattr__(k,dict[k])
            self.jobattrs.append(k)
        self.jobattrs.sort()
        self.executeinfo = None
        #self.log.debug("Made CondorJobInfo object with %d attributes" % len(self.jobattrs))    
        
    def __str__(self):
        attrstoprint = ['match_apf_queue',
                        'ec2instancename',
                        'ec2instancetype',
                        'enteredcurrentstatus',
                        'jobstatus',
                        'ec2remotevirtualmachinename',
                        'ec2securitygroups',
                        'ec2spotprice',
                        'gridjobstatus'                        
                        ]  
               
        s = "CondorEC2JobInfo: %s.%s " % (self.clusterid, 
                                      self.procid)
        for k in self.jobattrs:
            if k in attrstoprint:
                s += " %s=%s " % ( k, self.__getattribute__(k))
        if self.executeinfo:
            s += "%s" % self.executeinfo
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
        self.log = logging.getLogger()
        self.instanceid = instanceid
        self.machine = machine
        self.state = state
        self.activity = activity

      
    def __str__(self):
        s = "CondorSlotInfo: %s %s %s %s" % (self.instanceid, 
                                               self.machine, 
                                               self.state, 
                                               self.activity
                                               )
        return s

    def __repr__(self):
        s = str(self)
        return s    




class CondorExecuteInfo(object):
    '''
    Info object to represent an execute host on the cloud. 
    If it has multiple slots, we need to calculate overall state/activity carefully. 
    If it has retired, it will not appear in condor_status, only condor_status -master. 
        So execute hosts that only appear in master will have empty slotinfolist
    
    '''
    def __init__(self, instanceid, machine, publicdns ):
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
        self.log = logging.getLogger()
        # EC2 instance id
        self.instanceid = instanceid
        # Condor Machine name, usually internal hostname
        self.machine = machine
        # "Contact-able" hostname, usually EC2PublicDNS
        self.hostname = publicdns
        self.slotinfolist = []
        self.log.debug("Created new CondorExecuteInfo: %s %s %s" % (self.instanceid, 
                                                           self.machine,
                                                           self.hostname))   
    
    def addslotinfo(self, slotinfo):
        '''
                 
        '''
        self.slotinfolist.append(slotinfo)
        self.log.debug("Adding slotinfo %s to list." % slotinfo)

    def getStatus(self):
        '''
        Calculate master/startd status based on contents of slotinfo list:
        Node is 'busy' if any slots are 'busy'
        Node is 'idle' only if all slots are 'idle'.
        Node is 'retiring' if any slot is 'retiring'.
        Node is 'retired' if no slots appear in condor_status.   
        
        Valid statuses:
           idle
           running
           retiring
           retired
        
        '''
        overall = None
        if len(self.slotinfolist) == 0:
            overall = 'retired'
        else:
            busy = False
            idle = True
            retiring = False
            for si in self.slotinfolist:
                act = si.activity.lower()
                self.log.debug("slotinfo activity is %s" % act)
                
                if act == 'busy':
                    busy = True
                    idle = False
                    retiring = False
                    break
                elif act == 'retiring':
                    busy = False
                    idle = False
                    retiring = True
                    break
                elif act == 'idle':
                    pass
            # Calculate overall state
            if busy:
                overall = 'busy'
            elif idle:
                overall = 'idle'
            elif retiring:
                overall = 'retiring'
            else:
                self.log.warning('Difficulty calculating status for %s ' % self.instanceid)
            self.log.debug("[%s:%s] executeinfo overall is %s" % (self.machine, self.instanceid, overall))
        return overall
            
        
    def __str__(self):
        s = "CondorExecuteInfo: %s %s %s %s %d" % (self.instanceid, 
                                         self.machine,
                                         self.hostname,
                                         self.getStatus(),
                                         len(self.slotinfolist)
                                         )
         
        return s

    def __repr__(self):
        s = str(self)
        return s    
    


def test2():
    from autopyfactory.test import MockAPFQueue
    
    a = MockAPFQueue('BNL_CLOUD-ec2-xle1-sl6')
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




