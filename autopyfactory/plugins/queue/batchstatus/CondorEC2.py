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
from autopyfactory.interfaces import Singleton, CondorSingleton
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo

from autopyfactory.condor import checkCondor, querycondorxml, statuscondor, statuscondormaster
from autopyfactory.condor import parseoutput
from autopyfactory.condor import listnodesfromxml, aggregateinfo, killids
from autopyfactory.condor import mincondorversion

from autopyfactory.mappings import map2info


import autopyfactory.utils as utils

mincondorversion(8,1,1)

class CondorEC2(threading.Thread, BatchStatusInterface):
    '''
    BatchStatusPlugin intended to handle CloudInstances, i.e. a combination of a 
    submitted VM job AND startd information gathered from 'condor_status -master' output. 

    It adds new statuses: Retiring and Retired. 
    It adds correlation between VM jobs and startds in pool so that the startd status (Idle, 
    Retiring, Retired) appears in VM job attributes in the info object.  

    '''   
    __metaclass__ = CondorSingleton 
    
    def __init__(self, apfqueue, config, section):
        threading.Thread.__init__(self) # init the thread
        
        self.log = logging.getLogger("main.batchstatusplugin[singleton created by %s with condor_q_id %s]" %(apfqueue.apfqname, kw['condor_q_id']))
        self.log.trace('BatchStatusPlugin: Initializing object...')
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
            self.maxage = apfqueue.fcl.generic_get('Factory', 'batchstatus.condor.maxtime', default_value=360)
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
        self.log.trace('Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.trace('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.trace('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                try:
                    cq = self.currentinfo[queue]
                except:
                    self.log.warn('Problem getting info for queue: %s from valid currentinfo.' % queue)
                self.log.debug('Returning valid batchinfo for queue: %s' % queue)
                return cq
            else:                    
                self.log.trace('Leaving and returning info of %d entries.' % len(self.currentinfo))
                return self.currentinfo

    def getJobInfo(self, queue=None, maxtime=0):
        '''
        Returns a list of CondorEC2JobInfo objects which include startd information. 

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.trace('getInfo: Starting with maxtime=%s' % maxtime)
        
        if self.currentjobs is None:
            self.log.trace('getInfo: Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentjobs.lasttime) > maxtime:
            self.log.trace('getInfo: Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                try:
                    i =  self.currentjobs[queue]
                    self.log.trace('getInfo: Leaving and returning queue-specific JobInfo list of %d entries.' % len(i))
                    return i 
                except KeyError:
                    return None
            else:
                self.log.trace('getInfo: Leaving and returning all jobinfo w/ %d entries.' % len(self.currentjobs))
                return self.currentjobs


    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.trace('Starting')

        if not self.__started:
                self.log.trace("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.trace('Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.trace('Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.trace("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.trace('Leaving')


    def _update(self):
        '''        
            Query Condor for job status, create JobInfo objects. 
            Query condor_status -master for execute host info
            Query condor_status for startd status, adding that info to executeInfo
            
            Aggregate resulting objects for statistics, creating BatchStatusInfo object 
            update currentinfo
                
        '''
        self.log.trace('Starting.')

        exelist = None
        slotlist = None
        joblist = None

       
        if not utils.checkDaemon('condor'):
            self.log.warning('condor daemon is not running. Doing nothing')
        else:
            try:
                exelist = self._makeexelist()
                self.log.trace("exelist: %s" % exelist)
                self.log.debug("Made exelist with %d entries." % len(exelist))
                
                slotlist = self._makeslotlist()
                self.log.trace("slotlist: %s" % slotlist)
                self.log.debug("Made slotlist with %d entries." % len(slotlist))
                
                # Query condor once
                xmlout = querycondorxml()
                # With no jobs, xmlout is empty string. 
                dictlist = parseoutput(xmlout)
                # With no jobs, dictlist is empty list '[]'


                # use it to for stats and job-by-job processing...
                newinfo = self._makeinfolist(dictlist)
                self.log.trace("rawinfo: %s" % newinfo)
                self.log.debug("infolist with %d entries" % len(newinfo))
                
                joblist = self._makejoblist(dictlist)
                self.log.trace("rawjoblist: %s" % joblist)
                self.log.debug("joblist with %d entries" % len(joblist))
                
                #Make hash of SlotInfo objects by instanceid 
                slotsbyec2id =  self._indexobjectsby(slotlist, 'instanceid')
                self.log.trace("indexed slotlist: %s" % slotsbyec2id)
                self.log.debug("indexed slotlist with %d index entries." % len(slotsbyec2id.keys()))
                
                for exe in exelist:
                    ec2id = exe.instanceid
                    try:
                        slots = slotsbyec2id[ec2id]
                        exe.slotinfolist = slots
                    except KeyError:
                        self.log.trace("Failed to find slotinfo for ec2id %s." % ec2id)
                        # Not necessarily a problem, if node is retired. 

                                 
                # Make hash of of CondorExecuteInfo objects, indexed
                exebyec2id = self._indexobjectsby(exelist, 'instanceid')
                self.log.trace("indexed exelist: %s" % exebyec2id)
                self.log.debug("indexed exelist with %d index entries." % len(exebyec2id.keys()))
                
                # Now, add exeinfo to correct jobs, by ec2instanceid...

                for aq in joblist.keys():
                    self.log.trace("Adding exeinfo to jobs in apfqueue %s" % aq)
                    for job in joblist[aq]:
                        #self.log.trace("Handling job %s" % job) 
                        try:
                            ec2id = job.ec2instancename
                            self.log.trace("Adding exeinfo to job for ec2id: %s" % ec2id )
                            try:
                                exeinfo = exebyec2id[ec2id][0]
                                self.log.trace("Retrieved exeinfo from indexed hash for ec2id: %s" % ec2id)
                                # Should only be one per job
                                job.executeinfo = exeinfo
                                exestat = job.executeinfo.getStatus() 
                                self.log.trace("Job with exeinfo, checking status=%s" % exestat)
                                if exestat == 'retiring':
                                    self.log.trace("Found retiring, adjusting newinfo")
                                    newinfo[aq].retiring += 1
                                    newinfo[aq].running -= 1
                                elif exestat == 'retired':
                                    self.log.trace("Found retired, adjusting newinfo")
                                    newinfo[aq].retired += 1
                                    newinfo[aq].running -= 1
                                else:
                                    self.log.trace("No change to newinfo")
                                self.log.trace("Assigned exeinfo: %s to job %s" % (exeinfo, job))
                            except KeyError:
                                # New VM jobs will not have exeinfo until they start 
                                # and connect back to the pool. This is OK.  
                                pass
                        except AttributeError:
                            pass
                            #self.log.exception("Got AttributeError during exeinfo. Could be OK.")
                            # OK, not all jobs will be ec2 jobs. 
                               
                # Update current info references
                self.currentjobs = joblist
                self.currentinfo = newinfo
            
            except Exception, e:
                self.log.exception("Problem handling Condor info.")

        self.log.trace('_ Leaving.')


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
            self.log.trace("Created CondorExecuteInfo list of length %d" % len(exelist))
        return exelist
        
        
    def _makeslotlist(self):
        slotlist = []
        xmlout = statuscondor()
        if not xmlout:
            self.log.warning('output of statuscondor() is not valid. Not parsing it. Skip to next loop.') 
        else:
            dictlist = parseoutput(xmlout)
            slotlist = self._dicttoslotlist(dictlist)
            self.log.trace("Created CondorSlotInfo list of length %d" % len(slotlist))
        return slotlist
   
    def _makejoblist(self, dictlist):
        joblist = {}
        if not dictlist:
            self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
        else:
            joblist = self._dicttojoblist(dictlist)
            self.log.trace("Created indexed joblist of length %d" % len(joblist))
            self.currentjobs = joblist
        return joblist


    def _makeinfolist(self, dictlist):
        '''
        Makes list of CondorEC2JobInfo objects 
        Input may be None        
        '''
        newinfo = None
        if dictlist is None:
            self.log.warning('dictlist argument is None, Something wrong.') 
        else:
            aggdict = aggregateinfo(dictlist)
            # Output of empty list is emptly dictionary
            newinfo = map2info(aggdict, BatchStatusInfo(), self.jobstatus2info)
            
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
                    self.log.trace("Found EC2 job with instancename %s" % ec2in)
                except KeyError:
                    self.log.trace("Discarding non-EC2 job...")
            
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
                
        self.log.debug("Made job list of length %d" % len(joblist))
        self.log.debug("Made a job info dict of length %d" % len(qd))
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
                self.log.trace("Creating CondorExecuteInfo: %s" % j)
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
                self.log.trace("Found existing CondorStartdInfo object, adding slotinfo...")
                stdinfo.add(si)
            except KeyError:
                self.log.trace("KeyError. Creating new CondorStartdInfo object...")
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
        self.log.trace("Constructed indexed hash: %s" % hash)
        return hash
        

    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.trace('Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.trace('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.trace('Leaving')



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
        self.log = logging.getLogger('main.condorec2jobinfo')
        self.jobattrs = []
        for k in dict.keys():
            self.__setattr__(k,dict[k])
            self.jobattrs.append(k)
        self.jobattrs.sort()
        self.executeinfo = None
        #self.log.trace("Made CondorJobInfo object with %d attributes" % len(self.jobattrs))    
        
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
        self.log = logging.getLogger('main.condorslotinfo')
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
        self.log = logging.getLogger('main.condorexecuteinfo')
        # EC2 instance id
        self.instanceid = instanceid
        # Condor Machine name, usually internal hostname
        self.machine = machine
        # "Contact-able" hostname, usually EC2PublicDNS
        self.hostname = publicdns
        self.slotinfolist = []
        self.log.trace("Created new CondorExecuteInfo: %s %s %s" % (self.instanceid, 
                                                           self.machine,
                                                           self.hostname))   
    
    def addslotinfo(self, slotinfo):
        '''
                 
        '''
        self.slotinfolist.append(slotinfo)
        self.log.trace("Adding slotinfo %s to list." % slotinfo)

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
                self.log.trace("slotinfo activity is %s" % act)
                
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
            self.log.trace("[%s:%s] executeinfo overall is %s" % (self.machine, self.instanceid, overall))
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




