#! /usr/bin/env python

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

'''
General info scheme:

    BatchStatusPlugin
       getInfo   ->    BatchStatusInfo[apfqname] -> BatchQueueInfo(QueueInfo)
                                                       .state1  -> 0
                                                       .state2  -> 123
       getJobInfo ->   BatchStatusInfo[apfqname] -> 
                  
    WMSStatusPlugin
       getInfo   ->     WMSStatusInfo[wmsqname]  ->  JobsInfo(QueueInfo)
                                                           .state1 -> 0
                                                           .state2 -> 123
                          
       getSiteInfo  ->  WMSStatusInfo[sitename]  ->  SiteInfo(QueueInfo)
       getCloudInfo ->  WMSStatusInfo[cloudname] ->  CloudInfo(QueueInfo)
      
             
Inheritance:

    BaseAPFInfo           BaseQueueInfo
        |                       |
        V                       V
    BatchStatusInfo       BatchQueueInfo
    WMSStatusInfo         WMSQueueInfo
  
'''
    


class BaseAPFInfo(dict):
    '''
    Base for top-level Info classes with second-level Info objects indexed 
    by APF/WMS queue names.
        
    '''
    
    def __init__(self):
        dict.__init__(self)
    
    def __getitem__(self, k):
        '''
        Just ensure that if info for a queue is requested return None rather
        than trigger a KeyError exception. 
    
        '''
        if k in self.keys():
            return dict.__getitem__(self, k)
        else:
            default_cls = self.default
            # Make a new instance of whatever class is the default
            return default_cls()
  

class BaseQueueInfo(object):
    '''
    Base for aggregate (attribute-oriented) Info classes which are used per APF/WMS queue.
    Public Interface:
    
            fill(dictionary, mappings=None, reset=True)                
    '''

    def fill(self, dictionary, mappings=None, reset=True):
        '''
        method to fill object attributes with values from a dictionary.

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
                    try:
                        v = self.__dict__[k] + v
                    except KeyError:
                        pass
                    self.__dict__[k] = v
            else:
                try:
                    v = self.__dict__[k] + v
                except KeyError:
                    # missing keys no longer handled. 
                    pass
            self.__dict__[k] = v

    def __getattr__(self, name):
        '''
        Return 0 for non-existent attributes, otherwise behave normally.         
        '''
        try:
            return int(self.__getattribute__(name))
        except AttributeError:
            return 0


class BatchStatusInfo(BaseAPFInfo):
    '''
    Information returned by BatchStatusPlugin getInfo() calls. 
    Contains objects indexed by APF/WMS queue name. 
    '''
    
    def __init__(self):
        self.log = logging.getLogger()
        self.default = QueueInfo

    def __str__(self):
        s = "BatchStatusInfo: %d queues." % len(self)
        return s


class WMSStatusInfo(BaseAPFInfo):
    '''
    Information returned by WMSStatusPlugin getInfo() calls. 
    Contains objects indexed by APF/WMS queue name.    
    
    '''
    def __init__(self):
        self.log = logging.getLogger()
        self.default = WMSQueueInfo

    def __str__(self):
        s = "WMSStatusInfo: %d queues." % len(self)
        return s


class CloudStatusInfo(BaseAPFInfo):
    '''
    Information returned by WMSStatusPlugin getCloudInfo() calls. 
    Contains objects indexed by APF/WMS queue name.  
    '''
def __init__(self):
        self.log = logging.getLogger()



class CloudInfo(BaseQueueInfo):
    '''
    Attribute-based class containing WMS info about (WMS) clouds. 
    '''
def __init__(self):
        self.log = logging.getLogger()



class WMSQueueInfo(BaseQueueInfo):
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


class JobInfo(object):
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

    def __str__(self):
        s = "JobInfo: jobid=%s state=%s" % (self.jobid, self.state)
        return s


class SiteStatusInfo(BaseAPFInfo):
    '''
    Information returned by WMSStatusPlugin getSiteInfo() calls. 
    Contains objects indexed by APF/WMS queue name.  
    '''    
def __init__(self):
        self.log = logging.getLogger()    

class SiteInfo(BaseQueueInfo):
    '''
    Placeholder for attribute-based site information.
    One per site. 
    '''
def __init__(self):
        self.log = logging.getLogger()


class QueueInfo(BaseQueueInfo):
    '''
    Empty anonymous placeholder for aggregated queue information for a single APF queue.  

    Returns 0 as value for any un-initialized attribute. 
    
    '''
    def __init__(self):
        self.log = logging.getLogger()
    
    def __str__(self):
        s = "QueueInfo: pending=%d, running=%d, suspended=%d" % (self.pending, 
                                                                 self.running, 
                                                                 self.suspended)
        return s
  
