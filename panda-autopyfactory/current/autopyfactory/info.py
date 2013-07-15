#! /usr/bin/env python
#

import datetime
import logging
import logging.handlers
import threading
import time
import traceback
import os
import pwd
import sys

from pprint import pprint

from autopyfactory.apfexceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.logserver import LogServer


major, minor, release, st, num = sys.version_info

        

class BaseInfo(object):
    '''
    -----------------------------------------------------------------------
    Interface for all Info classes.
    The key thing is the class attribute valid. Each class inherited from
    InfoBase must give a list of values to valid. This list of values
    will be the list of attributes that that class will handle. 
    -----------------------------------------------------------------------
    Public Interface:
            reset()
            fill(dictionary, mappings=None, reset=True)
    -----------------------------------------------------------------------
    '''
    def __init__(self):
        pass

    def fill(self, dictionary, mappings=None, reset=True):
        '''
        method to fill object attributes with values comming from a dictionary.

        Each key of the dictionary is supposed 
        to be one attribute in the object.
        
        For example, if object is instance of class
                class C():
                        def __init__(self):
                                self.x = ...
                                self.y = ...
        then, the dictionary should look like
                d = {'x': ..., 'y':...}
        
        In case the dictionary keys and object attributes
        do not match, a dictionary mapping can be passed. 
        For example, the object is instance of class
                class C():
                        def __init__(self):
                                self.x = ...
                                self.y = ...
        and the dictionary look like
                d = {'a': ..., 'b':...}
        then, the mapping must be like
                mapping = {'a':'x', 'b':'y'}

        If reset is True, new values override whatever the attributes had.
        If reset is False, the new values are added to the previous value.
        '''
        usedk = []
        for k,v in dictionary.iteritems():
            try:
                if mappings:
                    if mappings.has_key(k):
                        k = mappings[k]
            except KeyError, e:
                log = logging.getLogger('main.info')
                log.error("fill(): Exception: %s" % str(e))
                log.debug("Stack Trace: %s " % traceback.format_exc()) 
                log.debug("k: %s v: %s dictionary: %s mappings: %s" % (k,v, dictionary, mappings))
            

            # if the key is new, then ...
            #       if no reset: we add the value to the old one
            #       if reset: we do nothing, so the final value will be the new one
            # if the key is not new...
            #       we just add the value to the stored one
            if k not in usedk:
                usedk.append(k)
                if not reset:
                    v = self.__dict__[k] + v
            else:
                v = self.__dict__[k] + v
            self.__dict__[k] = v




class CloudInfo(BaseInfo):
    '''
    -----------------------------------------------------------------------
    Empty anonymous placeholder for attribute-based cloud information.
    One per cloud. 

    Note: most probably not all attributes are really needed. 
    Once we decided which ones should stay we can clean up the valid list.
    -----------------------------------------------------------------------
    '''

    def __init__(self):
        self.tier1 = ""
        self.status = ""
        self.fasttrack = ""
        self.transtimehi = ""
        self.name = ""
        self.weight = ""
        self.transtimelo = ""
        self.dest = ""
        self.countries = ""
        self.relocation = ""
        self.sites = ""
        self.server = ""
        self.waittime = ""
        self.source = ""
        self.tier1SE = ""
        self.pilotowners = ""
        self.mcshare = ""
        self.validation = ""
        self.nprestage = ""

    def __str__(self):
        s = "CloudInfo" #FIXME: here we need something more
        return s


class BatchStatusInfo(BaseInfo):
    '''
    Information returned by BatchStatusPlugin getInfo() calls. 

    Logically consists of a dictionary of dictionaries, where the top-level keys are APF
    queue names, and the second-level is a Python dictionary containing 
       
    '''

    def __init__(self):
        self.data = {}


    def __str__(self):
        s = "BatchStatusInfo: APF Queues: "
        for k in self.data.keys():
            s += " %s " % k 
        return s

    def __getitem__(self, key):
        try:
            item = self.data[key]
        except KeyError:
            return QueueInfo()

    def __iter__(self):
        return self.data.itervalues()



class BatchStatusJobsInfo(BaseInfo):
    '''
    Information returned by BatchStatusPlugin getJobInfo() calls. 

    Logically consists of a dictionary of dictionaries, where the top-level keys are APF
    queue names, and the second-level is a Python list containing dictionaries of attributes, one per job. 
       
    '''
    def __init__(self):
        self.data = {}

    def __str__(self):
        s = "BatchStatusJobsInfo: APF Queues: "
        for k in self.data.keys():
            s += " %s " % k 
        return s

    def __getitem__(self, key):
        try:
            item = self.data[key]
        except KeyError:
            return JobsInfo()

    def __iter__(self):
        return self.data.itervalues()



class WMSQueueInfo(BaseInfo):
    '''
    -----------------------------------------------------------------------
    Empty anonymous placeholder for attribute-based WMS job information.
    One per WMS queue (for example, one per siteid in PanDA)

    Attributes are:
        - notready
        - ready
        - running
        - done
        - failed
        - unknown
   
    Note: eventually, a new class (or this one modified) will have 
          a valid list of attributes for statuses with labels (PanDA ProdSourceLabel)

    -----------------------------------------------------------------------
    '''

    def __init__(self):
        self.log = logging.getLogger()

    def __getattr__(self, name):
        '''
        Return 0 for non-existent attributes, otherwise behave normally.         
        '''
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return 0        

    def __str__(self):
        s = "WMSQueueInfo: notready=%s, ready=%s, running=%s, done=%s, failed=%s, unknown=%s" %\
            (self.notready,
             self.ready,
             self.running,
             self.done,
             self.failed,
             self.unknown
            )
        return s


class JobInfo(BaseInfo):
    '''
    Abstract representation of job in APF. 
    At a minimum we need
        jobid          Typically Condor cluster.proc ID, but could be VM instanceid
        state          APF job state: submitted, pending, running, done, failed, held
        inittime       datetime.datetime object
        
    '''
    
    def __init__(self, jobid, state, inittime):
        self.jobid = jobid
        self.state = state
        self.inittime = inittime


class JobsInfo(list):
    '''
    Data structure to contain info on all jobs for a single APF queue. 
        
    '''

    def __init__(self):
        self.log = logging.getLogger()
        
    def __str__(self):
        s = "JobsInfo object containing %d jobs" % len(self)
        
    

class SiteInfo(BaseInfo):
    '''
    -----------------------------------------------------------------------
    Empty anonymous placeholder for attribute-based site information.
    One per site. 

    Note: most probably not all attributes are really needed. 
    Once we decided which ones should stay we can clean up the valid list.
    -----------------------------------------------------------------------
    '''


    def __init__(self):
        self.comment = ""
        self.gatekeeper = ""
        self.cloudlist = ""
        self.defaulttoken = ""
        self.priorityoffset = ""
        self.cloud = ""
        self.accesscontrol = ""
        self.retry = ""
        self.maxinputsize = ""
        self.space = ""
        self.sitename = ""
        self.allowdirectaccess = ""
        self.seprodpath = ""
        self.ddm = ""
        self.memory = ""
        self.setokens = ""
        self.type = ""
        self.lfcregister = ""
        self.status = ""  # only thing we care about...
        self.lfchost = ""
        self.releases = ""
        self.statusmodtime = ""
        self.maxtime = ""
        self.nickname = ""
        self.dq2url = ""
        self.copysetup = ""
        self.cachedse = ""
        self.cmtconfig = ""
        self.allowedgroups = ""
        self.queue = ""
        self.localqueue = ""
        self.glexec = ""
        self.validatedreleases = ""
        self.se = ""

    def __str__(self):
        s = "SiteInfo" #FIXME: here we need something more
        return s
  

class InfoContainer(dict):
    '''
    -----------------------------------------------------------------------
    Class to collect info from different Status Plugins
    
    In a nutshell, the class is a dictionary of some Info type objects
    -----------------------------------------------------------------------
    Public Interface:
            valid()
    -----------------------------------------------------------------------
    '''
    def __init__(self, infotype, default):
        '''
        Info for each info type is retrieved, set, and adjusted via the corresponding label
        For example, for a container of BatchStatusInfo objects:

            numrunning = info['BNL_ATLAS_1'].running
            info['BNL_ITB_1'].pending = 17
            info['BNL_ITB_1'].finished += 1

        For a container of WMSQueueInfo objects:

            jobswaiting = info['BNL_ATLAS_1'].activated
            info['BNL_ITB_1'].running = 17
            info['BNL_ITB_1'].done += 1
            
        The type of info that the Container is going to store
        can be gently passed thru infotype variable.

        default is a default object that will be used to
        return something when someone asks for a key that
        does not exist in the dictionary.
        It is expected to be an empty instance
        from whichever class the dictionary is storing objects.
        For example, BatchStatusInfo() or WMSQueueInfo(), etc.

        Any alteration access updates the info.mtime attribute. 
        '''
        
        self.log = logging.getLogger('main.batchstatus')
        self.log.debug('Status: Initializing object...')
        self.type = infotype # this can be things like "BatchStatusInfo", "SiteInfo", "WMSQueueInfo"...
        self.default = default
        self.lasttime = None
        self.log.info('Status: Object Initialized')

    def __str__(self):
        s = "InfoContainer containing %d objects of type %s" % (self.__len__(), self.type)
        return s

    def __getitem__(self, k):
        '''
        overrides the default __getitem__ 
        to check if the key is one of the 
        queues stored in the dictionary. 
        If it is a new key, 
        then it returns self.default

        It is a way to force using 

            dict.get(k, [default])
        '''
        if k in self.keys():
            return dict.__getitem__(self, k)
        else:
            return self.default


class WMSStatusInfo(object):
        '''
        -----------------------------------------------------------------------
        Class to represent info from a WMS Status Plugin 
        -----------------------------------------------------------------------
        '''
        def __init__(self):

            self.log = logging.getLogger('main.wmsstatus')
            self.log.debug('Status: Initializing object...')

            self.cloud = None
            self.site = None
            self.jobs = None
            self.lasttime = None

            self.log.info('Status: Object Initialized')


        def __len__(self):
            length = 3
            if self.cloud is None:
                length -= 1
            if self.site is None:
                length -= 1
            if self.jobs is None:
                length -= 1
            return length



class QueueInfo(object):
    '''
    -----------------------------------------------------------------------
     Empty anonymous placeholder for aggregated queue information for a single APF queue.  
     
    '''
    def __init__(self):
        self.log = logging.getLogger()
    

    def __getattr__(self, name):
        '''
        Return 0 for non-existent attributes, otherwise behave normally.         
        '''
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return 0

            
    def __str__(self):
        s = "QueueInfo: pending=%d, running=%d, suspended=%d" % (self.pending, 
                                                                 self.running, 
                                                                 self.suspended)
        return s

  
