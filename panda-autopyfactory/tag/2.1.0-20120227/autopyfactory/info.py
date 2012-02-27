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
from autopyfactory.configloader2 import Config
from autopyfactory.logserver import LogServer

__author__ = "Jose Caballero"
__copyright__ = "2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

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
            dict()
    -----------------------------------------------------------------------
    '''
    valid = []
    def __init__(self, default=0):
        self.__dict__['default']  = default
        self.reset()

    def reset(self):
        '''
        gives an initial value to all attributes in the object.
        The entire list of attributes comes from the class attribute valid.
        The initial value was passed thru __init__()
        '''
        for x in self.__class__.valid:
            self.__dict__[x] = self.default

    def __setattr__(self, name, value):
        '''
        we override __setattr__ just to be sure that no attribute other
        than those listed in class attribute is given a value.
        '''
        if name in self.__class__.valid: 
            self.__dict__[name] = value

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
            
            if k not in usedk:
                usedk.append(k)
                if not reset:
                    v = self.__dict__[k] + v
            else:
                v = self.__dict__[k] + v
            self.__dict__[k] = v

    def dict(self):
        '''
        returns a dictionary with the stored info. 
        Keys are the list of variables in valid.
        '''
        d = {}
        for k in self.__class__.valid:
            d[k] = self.__dict__[k]
        return d

    def getConfig(self, section):
        '''
        converts the internal dictionary into 
        a Config object
        '''
        conf = Config()
        conf.add_section(section)
        dic = self.dict()
        for k,v in dic.iteritems():
                if v != None:
                        conf.set(section, k ,v)
        return conf


class BatchQueueInfo(BaseInfo):
    '''
    -----------------------------------------------------------------------
     Empty anonymous placeholder for attribute-based queue information.
     One per queue. 
     
        Primary attributes are:
            pending            job is queued (somewhere) but not running yet.
            running            job is currently active (run + stagein + stageout)
            error              job has been reported to be in an error state
            suspended          job is active, but held or suspended
            done               job has completed
            unknown            unknown or transient intermediate state

        Secondary attributes are:
            transferring       stagein + stageout
            stagein
            stageout           
            failed             (done - success)
            success            (done - failed)
            ?
    -----------------------------------------------------------------------
    '''
    valid = ['pending', 'running', 'error', 'suspended', 'done', 'unknown']

    def __init__(self):
        # default value 0
        super(BatchQueueInfo, self).__init__(0)
        

    def __str__(self):
        s = "BatchQueueInfo: pending=%d, running=%d, suspended=%d" % (self.pending, 
                                                                 self.running, 
                                                                 self.suspended)
        return s

    # property to return the total number of pilots, irrespective their state
    total = property(lambda self: sum([self.__dict__[i] for i in self.valid]))


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
    valid = ['notready', 'ready', 'running', 'done', 'failed', 'unknown']

    def __init__(self):
        # default value 0
        super(WMSQueueInfo, self).__init__(0)

    def __str__(self):
        s = "WMSJobInfo: notready=%s, ready=%s, running=%s, done=%s, failed=%s, unknown=%s" %(self.notready,
                                                                                  self.ready,
                                                                                  self.running,
                                                                                  self.done,
                                                                                  self.failed,
                                                                                  self.unknown)
        return s

    # property to return the total number of jobs, irrespective their state
    total = property(lambda self: sum([self.__dict__[i] for i in self.valid]))

class CloudInfo(BaseInfo):
    '''
    -----------------------------------------------------------------------
    Empty anonymous placeholder for attribute-based cloud information.
    One per cloud. 

    Note: most probably not all attributes are really needed. 
    Once we decided which ones should stay we can clean up the valid list.
    -----------------------------------------------------------------------
    '''
    valid = ['tier1', 'status', 'fasttrack', 'transtimehi', 'name', 'weight', 'transtimelo', 'dest', 'countries', 'relocation', 'sites', 'server', 'waittime', 'source', 'tier1SE', 'pilotowners', 'mcshare', 'validation', 'nprestage']

    def __init__(self):
        # default value None
        super(CloudInfo, self).__init__(None)

    def __str__(self):
        s = "CloudInfo" #FIXME: here we need something more
        return s
        

class SiteInfo(BaseInfo):
    '''
    -----------------------------------------------------------------------
    Empty anonymous placeholder for attribute-based site information.
    One per site. 

    Note: most probably not all attributes are really needed. 
    Once we decided which ones should stay we can clean up the valid list.
    -----------------------------------------------------------------------
    '''

    valid = ['comment', 'gatekeeper', 'cloudlist', 'defaulttoken', 'priorityoffset', 'cloud', 'accesscontrol', 'retry', 'maxinputsize', 'space', 'sitename', 'allowdirectaccess', 'seprodpath', 'ddm', 'memory', 'setokens', 'type', 'lfcregister', 'status', 'lfchost', 'releases', 'statusmodtime', 'maxtime', 'nickname', 'dq2url', 'copysetup', 'cachedse', 'cmtconfig', 'allowedgroups', 'queue', 'localqueue', 'glexec', 'validatedreleases', 'se']

    def __init__(self):
        # default value None
        super(SiteInfo, self).__init__(None)

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
        For example, for a container of BatchQueueInfo objects:

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
        For example, BatchQueueInfo() or WMSQueueInfo(), etc.

        Any alteration access updates the info.mtime attribute. 
        '''
        
        self.log = logging.getLogger('main.batchstatus')
        self.log.debug('Status: Initializing object...')
        self.type = infotype # this can be things like "BatchQueueInfo", "SiteInfo", "WMSQueueInfo"...
        self.default = default
        self.lasttime = None
        self.log.info('Status: Object Initialized')

    def valid(self):
        '''
        checks if all attributes have a valid value, or
        some of them is None and therefore the collected info 
        is not reliable
        '''
        self.log.debug('valid: Starting.')

        out = True  # default
        #if self.batch == None:
        #    out = False 

        #self.log.info('valid: Leaving with output %s.' %out)
        return out

    def __str__(self):
        s = "InfoContainer containing %d objects of type %s" % (self.__len__(), self.type)
        return s

    def __getitem__(self, k):
        '''
        overrides the default __getitem__ 
        to check if the key is one of the 
        queues stored in the dictionary. 
        If it is a new key, then it returns
        self.default
        '''
        if k in self.keys():
            return dict.__getitem__(self, k)
        else:
            return self.default

class WMSStatusInfo(object):
        '''
        -----------------------------------------------------------------------
        Class to collect info from WMS Status Plugin 
        -----------------------------------------------------------------------
        Public Interface:
                valid()
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

        def valid(self):
            '''
            checks if all attributes have a valid value, or
            some of them is None and therefore the collected info 
            is not reliable
            '''
            self.log.debug('valid: Starting.')

            out = True  # default
            if self.cloud == None:
                out = False 
            if self.site == None:
                out = False 
            if self.jobs == None:
                out = False 

            self.log.debug('valid: Leaving with output %s.' %out)
            return out

        def __len__(self):
            length = 3
            if self.cloud is None:
                length -= 1
            if self.site is None:
                length -= 1
            if self.jobs is None:
                length -= 1
            return length
