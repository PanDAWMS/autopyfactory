#! /usr/bin/env python
#
# $Id: factory.py 7688 2011-04-08 22:15:52Z jhover $
#
'''
    Main module for autopyfactory, a pilot factory for PanDA
'''

import datetime
import logging
import threading
import time
import os
import sys

from pprint import pprint

from ConfigParser import ConfigParser

from autopyfactory.apfexceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.configloader import FactoryConfigLoader, QueueConfigLoader
from autopyfactory.cleanLogs import CleanCondorLogs
from autopyfactory.logserver import LogServer
from autopyfactory.proxymanager import ProxyManager

import userinterface.Client as Client

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010,2011 John Hover; 2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"
          
class Factory(object):
    '''
    -----------------------------------------------------------------------
    Class implementing the main loop. 
    The class has two main goals:
            1. load the config files
            2. launch a new thread per queue 

    Information about queues created and running is stored in a 
    WMSQueuesManager object.

    Actions are triggered by method update() 
    update() can be invoked at the beginning, from __init__,
    or when needed. For example, is an external SIGNAL is received.
    When it happens, update() does:
            1. calculates the new list of queues from the config file
            2. updates the WMSQueuesManager object 
    -----------------------------------------------------------------------
    Public Interface:
            __init__(fcl)
            mainLoop()
            update()
    -----------------------------------------------------------------------
    '''

    def __init__(self, fcl):
        '''
        fcl is a FactoryConfigLoader object. 
        '''

        self.log = logging.getLogger('main.factory')
        self.log.info('Factory: Initializing object...')

        self.fcl = fcl
        
        self.log.info("queueConf file(s) = %s" % fcl.get('Factory', 'queueConf'))
        self.qcl = QueueConfigLoader(fcl.get('Factory', 'queueConf').split(','))
      
        # Handle ProxyManager
        usepman = fcl.get('Factory', 'proxymanager.enabled')
        if usepman:
                            
            pconfig = ConfigParser()
            pconfig_file = fcl.get('Factory','proxyConf')
            got_config = pconfig.read(pconfig_file)
            self.log.debug("Read config file %s, return value: %s" % (pconfig_file, got_config)) 
            self.proxymanager = ProxyManager(pconfig)
            self.log.debug('ProxyManager initialized. Starting...')
            self.proxymanager.start()
            self.log.debug('ProxyManager thread started.')
        else:
            self.log.info("ProxyManager disabled.")
       
        # WMS Queues Manager 
        self.wmsmanager = WMSQueuesManager(self)
        
        # Set up LogServer
        ls = self.fcl.get('Factory', 'logserver.enabled')
        lsidx = self.fcl.get('Factory','logserver.index')
        if ls:
            logpath = self.fcl.get('Factory', 'baseLogDir')
            if not os.path.exists(logpath):
                os.makedirs(logpath)
        
            self.logserver = LogServer(port=self.fcl.get('Factory', 'baseLogHttpPort'),
                           docroot=logpath, index=lsidx
                           )

            self.log.debug('LogServer initialized. Starting...')
            self.logserver.start()
            self.log.debug('LogServer thread started.')
        else:
            self.log.info('LogServer disabled. Not running.')
             
        self.log.info("Factory: Object initialized.")

    def mainLoop(self):
        '''
        Main functional loop of overall Factory. 
        Actions:
                1. Creates all queues and starts them.
                2. Wait for a termination signal, and
                   stops all queues when that happens.
        '''

        self.log.debug("mainLoop: Starting.")
        self.log.info("Starting all Queue threads...")

        self.update()
        
        try:
            while True:
                mainsleep = int(self.fcl.get('Factory', 'factory.sleep'))
                time.sleep(mainsleep)
                self.log.debug('Checking for interrupt.')
                        
        except (KeyboardInterrupt): 
            logging.info("Shutdown via Ctrl-C or -INT signal.")
            logging.debug(" Shutting down all threads...")
            self.log.info("Joining all Queue threads...")
            self.wmsmanager.join()
            self.log.info("All Queue threads joined. Exitting.")

        self.log.debug("mainLoop: Leaving.")

    def update(self):
        '''
        Method to update the status of the WMSQueuesManager object.
        This method will be used every time the 
        status of the queues changes: 
                - at the very beginning
                - when the config files change
        That means this method will be invoked by the regular factory
        main loop code or from any method capturing specific signals.
        '''

        self.log.debug("update: Starting")

        newqueues = self.qcl.config.sections()
        self.wmsmanager.update(newqueues) 

        self.log.debug("update: Leaving")


# ==============================================================================                                
#                       QUEUES MANAGEMENT
# ==============================================================================                                

class WMSQueuesManager(object):
    '''
    -----------------------------------------------------------------------
    Container with the list of WMSQueue objects.
    -----------------------------------------------------------------------
    Public Interface:
            __init__(factory)
            update(newqueues)
            join()
    -----------------------------------------------------------------------
    '''
    def __init__(self, factory):
        '''
        Initializes a container of WMSQueue objects
        '''

        self.log = logging.getLogger('main.wmsquuesmanager')
        self.log.info('WMSQueuesManager: Initializing object...')

        self.queues = {}
        self.factory = factory

        self.log.info('WMSQueuesManager: Object initialized.')

# ----------------------------------------------------------------------
#            Public Interface
# ---------------------------------------------------------------------- 
    def update(self, newqueues):
        '''
        Compares the new list of queues with the current one
                1. creates and starts new queues if needed
                2. stops and deletes old queues if needed
        '''

        self.log.debug("update: Starting with input %s" %newqueues)

        currentqueues = self.queues.keys()
        queues_to_remove, queues_to_add = \
                self._diff_lists(currentqueues, newqueues)
        self._addqueues(queues_to_add) 
        self._delqueues(queues_to_remove)
        self._refresh()

        self.log.debug("update: Leaving")

    def join(self):
        '''
        Joins all WMSQueue objects
        QUESTION: should the queues also be removed from self.queues ?
        '''

        self.log.debug("join: Starting")

        count = 0
        for q in self.queues.values():
            q.join()
            count += 1
        self.log.info('join: %d queues joined' %count)

        self.log.debug("join: Leaving")
    
    # ----------------------------------------------------------------------
    #  private methods
    # ----------------------------------------------------------------------

    def _addqueues(self, apfqueues):
        '''
        Creates new WMSQueue objects
        '''

        self.log.debug("_addqueues: Starting with input %s" %apfqueues)

        count = 0
        for apfqueue in apfqueues:
            self._add(apfqueue)
            count += 1
        self.log.info('_addqueues: %d queues in the config file' %count)

        self.log.debug("_addqueues: Leaving")

    def _add(self, apfqueue):
        '''
        Creates a single new WMSQueue object and starts it
        '''

        self.log.debug("_add: Starting with input %s" %apfqueue)

        enabled = self.factory.qcl.getboolean(apfqueue, 'enabled') 
        if enabled:
            qobject = WMSQueue(apfqueue, self.factory)
            self.queues[apfqueue] = qobject
            qobject.start()
            self.log.info('_add: %s enabled.' %apfqueue)
        else:
            self.log.info('_add: %s not enabled.' %apfqueue)

        self.log.debug("_add: Leaving")
            
    def _delqueues(self, apfqueues):
        '''
        Deletes WMSQueue objects
        '''

        self.log.debug("_delqueues: Starting with input %s" %apfqueues)

        count = 0
        for apfqueue in apfqueues:
            q = self.queues[apfqueue]
            q.join()
            self.queues.pop(apfqueue)
            count += 1
        self.log.info('_delqueues: %d queues joined and removed' %count)

        self.log.debug("_delqueues: Leaving")

    def _del(self, apfqueue):
        '''
        Deletes a single queue object from the list and stops it.
        '''

        self.log.debug("_del: Starting with input %s" %apfqueue)

        qobject = self._get(apfqueue)
        qname.join()
        self.queues.pop(apfqueue)

        self.log.debug("_del: Leaving")
    
    def _refresh(self):
        '''
        Calls method refresh() for all WMSQueue objects
        '''

        self.log.debug("_refresh: Starting")

        count = 0
        for q in self.queues.values():
            q.refresh()
            count += 1
        self.log.info('_refresh: %d queues refreshed' %count)

        self.log.debug("_refresh: Leaving")

    # ----------------------------------------------------------------------
    #  ancillary functions 
    # ----------------------------------------------------------------------

    def _diff_lists(self, l1, l2):
        '''
        Ancillary method to calculate diff between two lists
        '''
        d1 = [i for i in l1 if not i in l2]
        d2 = [i for i in l2 if not i in l1]
        return d1, d2
 

class WMSQueue(threading.Thread):
    '''
    -----------------------------------------------------------------------
    Encapsulates all the functionality related to servicing each queue (i.e. siteid, i.e. site).
    -----------------------------------------------------------------------
    Public Interface:
            The class is inherited from Thread, so it has the same public interface.
    -----------------------------------------------------------------------
    '''
    
    def __init__(self, apfqueue, factory):
        '''
        siteid is the name of the section in the queueconfig, 
        i.e. the queue name, 
        factory is the Factory object who created the queue 
        '''

        # recording moment the object was created
        self.inittime = datetime.datetime.now()

        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.wmsqueue[%s]' %apfqueue)
        self.log.info('WMSQueue: Initializing object...')

        self.stopevent = threading.Event()

        # apfqueue is the APF queue name, i.e. the section heading in queues.conf
        self.apfqueue = apfqueue
        #self.siteid = siteid          # Queue section designator from config
        self.factory = factory
        self.fcl = self.factory.fcl 
        self.qcl = self.factory.qcl 

        if self.qcl.has_option(apfqueue, 'siteid'):
            self.siteid = self.qcl.get(apfqueue, 'siteid')
        else:
            # if siteid is not in the specs, then
            # the very APF QUEUE name is teh siteid, as default
            self.siteid = apfqueue
        self.nickname = self.qcl.get(apfqueue, 'nickname')
        self.cloud = self.qcl.get(apfqueue, 'cloud')
        self.cycles = self.fcl.get("Factory", 'cycles' )
        self.sleep = int(self.qcl.get(apfqueue, 'wmsqueue.sleep'))
        self.cyclesrun = 0
        
        # Handle sched plugin
        self.scheduler = self._getplugin('sched', self)

        # Monitor
        if self.fcl.has_option('Factory', 'monitorURL'):
            self.log.info('Instantiating a monitor...')
            from autopyfactory.monitor import Monitor
            args = dict(self.fcl.items('Factory'))
            self.monitor = Monitor(**args)

        # Condor logs cleaning
        self.clean = CleanCondorLogs(self)
        self.clean.start()

        # Handle status and submit batch plugins. 
        self.batchstatus = self._getplugin('batchstatus', self)
        self.batchstatus.start()                # starts the thread
        self.wmsstatus = self._getplugin('wmsstatus', self)
        self.wmsstatus.start()                  # starts the thread
        self.batchsubmit = self._getplugin('batchsubmit', self)
        self.log.info('WMSQueue: Object initialized.')

        

    def _getplugin(self, action, *k, **kw):
        '''
        Generic private method to find out the specific plugin
        to be used for this queue, depending on the action.
        Action can be:
                - sched
                - batchstatus
                - wmsstatus
                - batchsubmit
        *k and *kw are inputs for the plugin class __init__() method

        Steps taken are:
                1. The name of the item in the config file is calculated.
                   It is supposed to have format <action>plugin.
                   For example:  schedplugin, batchstatusplugin, ...
                2. The name of the plugin module is calculated.
                   It is supposed to have format <config item><prefix>Plugin.
                   The prefix is taken from a map.
                   For example: SimpleSchedPlugin, CondorBatchStatusPlugin
                3. The plugin module is imported, using __import__
                4. The plugin class is retrieved. 
                   The name of the class is supposed to have format
                   <prefix>Plugin
                   For example: SchedPlugin(), BatchStatusPlugin()
        '''

        self.log.debug("__getplugin: Starting with inputs %s and %s" %( k, kw))

        plugin_prefixes = {
                'sched' : 'Sched',
                'wmsstatus': 'WMSStatus',
                'batchstatus': 'BatchStatus',
                'batchsubmit': 'BatchSubmit'
        }

        plugin_config_item = '%splugin' %action
        plugin_prefix = plugin_prefixes[action] 
        schedclass = self.qcl.get(self.apfqueue, plugin_config_item)
        plugin_module_name = '%s%sPlugin' %(schedclass, plugin_prefix)
        
        self.log.info("_getplugin: Attempting to import derived classname: autopyfactory.plugins.%s"
                        % plugin_module_name)

        plugin_module = __import__("autopyfactory.plugins.%s" % plugin_module_name, 
                                   globals(), 
                                   locals(),
                                   ["%s" % plugin_module_name])
        plugin_class = '%sPlugin' %plugin_prefix

        self.log.info("_getplugin: Attempting to return plugin with classname %s" %plugin_class)

        self.log.debug("_getplugin: Leaving with plugin named %s" %plugin_class)
        return getattr(plugin_module, plugin_class)(*k, **kw)

# Run methods

    def run(self):
        '''
        Method called by thread.start()
        Main functional loop of this WMSQueue. 
        '''        

        self.log.debug("run: Starting" )

        while not self.stopevent.isSet():
            try:
                nsub = self.scheduler.calcSubmitNum(self.status)
                self._submitpilots(nsub)
                self._monitor_shout()
                self._exitloop()
                self._reporttime()
                time.sleep(self.sleep)
            
            except Exception, e:
                self.log.error("Caught exception: %s" % str(e))
                self.log.debug("Exception: %s" % sys.exc_info()[0])

        self.log.debug("run: Leaving")

    def _updatestatus(self):
            '''
            update batch info and panda info
            '''
            self.log.debug("__updatestatus: Starting")

            # checking if factory.conf has attributes to say
            # how old the status info (batch, wms) can be
            batchstatusmaxtime = 0
            if self.fcl.has_option('Factory', 'batchstatus.maxtime'):
                batchstatusmaxtime = self.fcl.get('Factory', 'batchstatus.maxtime')

            wmsstatusmaxtime = 0
            if self.fcl.has_option('Factory', 'wmsstatus.maxtime'):
                wmsstatusmaxtime = self.fcl.get('Factory', 'wmsstatus.maxtime')

            # getting the info
            self.status.batch = self.batchstatus.getInfo(self.siteid, batchstatusmaxtime)
            self.status.cloud = self.wmsstatus.getCloudInfo(self.cloud, wmsstatusmaxtime)
            self.status.site = self.wmsstatus.getSiteInfo(self.siteid, wmsstatusmaxtime)
            self.status.jobs = self.wmsstatus.getJobsInfo(self.siteid, wmsstatusmaxtime)

            self.log.debug("__updatestatus: Leaving")


    def _submitpilots(self, nsub):
        '''
        submit using this number
        '''

        self.log.debug("__submitpilots: Starting")
        # message for the monitor
        msg = 'Attempt to submit %d pilots for queue %s' %(nsub, self.siteid)
        self.__monitor_note(msg)

        (status, output) = self.batchsubmit.submitPilots(self.siteid, nsub, self.fcl, self.qcl)
        if output:
            if status == 0:
                self._monitor_notify(output)

        self.cyclesrun += 1

        self.log.debug("__submitpilots: Leaving")

    # Monitor-releated methods

    def _monitor_shout(self):
        '''
        call monitor.shout() method
        '''

        self.log.debug("__monitor_shout: Starting.")
        if hasattr(self, 'monitor'):
            self.monitor.shout(self.siteid, self.cyclesrun)
        else:
            self.log.debug('__monitor_shout: no monitor instantiated')
        self.log.debug("__monitor_shout: Leaving.")

    def _monitor_note(self, msg):
        '''
        collects messages for the Monitor
        '''

        self.log.debug('__monitor_note: Starting.')

        if hasattr(self, 'monitor'):
            nick = self.qcl.get(self.apfqueue, 'nickname')
            self.monitor.msg(nick, self.siteid, msg) # FIXME?
        else:
            self.log.debug('__monitor_note: no monitor instantiated')
                
        self.log.debug('__monitor__note: Leaving.')

    def _monitor_notify(self, output):
        '''
        sends all collected messages to the Monitor server
        '''

        self.log.debug('__monitor_notify: Starting.')

        if hasattr(self, 'monitor'):
            nick = self.qcl.get(self.apfqueue, 'nickname')
            label = self.siteid #FIXME?
            self.monitor.notify(nick, label, output)
        else:
            self.log.debug('__monitor_notify: no monitor instantiated')

        self.log.debug('__monitor_notify: Leaving.')


    def _exitloop(self):
        '''
        Exit loop if desired number of cycles is reached...  
        '''

        self.log.debug("__exitloop: Starting")

        self.log.debug("__exitloop. Checking to see how many cycles to run.")
        if self.cycles and self.cyclesrun >= self.cycles:
                self.log.info('__exitloop: stopping the thread because high cyclesrun')
                self.stopevent.set()                        
        self.log.debug("__exitloop. Incrementing cycles...")

        self.log.debug("__exitloop: Leaving")

    def _reporttime(self):
        '''
        report the time passed since the object was created
        '''

        self.log.debug("__reporttime: Starting")

        now = datetime.datetime.now()
        delta = now - self.inittime
        days = delta.days
        seconds = delta.seconds
        hours = seconds/3600
        minutes = (seconds%3600)/60
        total_seconds = days*86400 + seconds
        average = total_seconds/self.cyclesrun

        self.log.info('__reporttime: up %d days, %d:%d, %d cycles, ~%d s/cycle' %(days, hours, minutes, self.cyclesrun, average))
        
        self.log.debug("__reporttime: Leaving")

    # End of run-related methods

    def refresh(self):
        '''
        Method to reload, when requested, the config file
        '''
        pass 
        # TO BE IMPLEMENTED
                      
    def join(self,timeout=None):
        '''
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        '''

        self.log.debug("join: Starting")

        self.stopevent.set()
        self.log.info('Stopping thread...')
        threading.Thread.join(self, timeout)

        self.log.debug("join: Leaving")
                 
                            

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
            self.log.info('Status: Initializing object...')

            self.cloud = {}
            self.site = {}
            self.jobs = {}
            self.lasttime = None

            self.log.info('Status: Object Initialized')

        def valid(self):
            '''
            checks if all attributes have a valid value, or
            some of them is None and therefore the collected info 
            is not reliable
            '''
            self.log.info('valid: Starting.')

            out = True  # default
            if self.cloud == None:
                out = False 
            if self.site == None:
                out = False 
            if self.jobs == None:
                out = False 

            self.log.info('valid: Leaving with output %s.' %out)
            return out

#
# At some point it would be good to encapsulate a non-Panda-specific vocabulary
# for WMSStatusInfo information. For now, the existing Python Dictionaries are 
# good enough. 

class CloudInfo(object):
    def __init__(self):
        pass
    
class SiteInfo(object):
    def __init__(self):
        pass

    
class JobInfo(object):
    def __init__(self):
        pass


class BatchStatusInfo(object):
    '''
    -----------------------------------------------------------------------
    Class to collect info from Batch Status Plugin 
    -----------------------------------------------------------------------
    Public Interface:
            valid()
    -----------------------------------------------------------------------
    '''
    def __init__(self):
        '''
        Info for each queue is retrieved, set, and adjusted via APF queuename, e.g.
            numrunning = info.BNL_ATLAS_1.running
            info.BNL_ITB_1.pending = 17
            info.BNL_ITB_1.finished += 1
            
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
        
        Any alteration access updates the info.mtime attribute. 
        
        '''
        
        self.log = logging.getLogger('main.batchstatus')
        self.log.info('Status: Initializing object...')
        self._queues = {}
        self.lasttime = None
        self.log.info('Status: Object Initialized')

   

    def valid(self):
        '''
        checks if all attributes have a valid value, or
        some of them is None and therefore the collected info 
        is not reliable
        '''
        self.log.info('valid: Starting.')

        out = True  # default
        #if self.batch == None:
        #    out = False 

        #self.log.info('valid: Leaving with output %s.' %out)
        return out


class QueueInfo(object):
    '''
     Empty anonymous placeholder for attribute-based queue information.
     One per queue. 
     
     Makes sure that only valid attributes can be set.         
            .pending          
            .running           
            .suspended  
            .done        
            .unknown           
            .error
    
    '''
    def __init__(self):
        self.pending = 0
        self.running = 0
        self.suspended = 0
        self.done = 0
        self.unknown = 0
        self.error = 0

# --------------------------------------------------------------------------- 

class Singleton(type):
    '''
    -----------------------------------------------------------------------
    Ancillary class to be used as metaclass to make other classes Singleton.
    -----------------------------------------------------------------------
    '''
    def __init__(cls, name, bases, dct):
        cls.__instance = None 
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw): 
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance

# ==============================================================================                                
#                      INTERFACES & TEMPLATES
# ==============================================================================  

class SchedInterface(object):
    '''
    -----------------------------------------------------------------------
    Calculates the number of jobs to be submitted for a given queue. 
    -----------------------------------------------------------------------
    Public Interface:
            calcSubmitNum()
    -----------------------------------------------------------------------
    '''
    def calcSubmitNum(self):
        '''
        Calculates number of jobs to submit for the associated APF queue. 
        '''
        raise NotImplementedError

class BatchStatusInterface(object):
    '''
    -----------------------------------------------------------------------
    Interacts with the underlying batch system to get job status. 
    Should return information about number of jobs currently on the desired queue. 
    -----------------------------------------------------------------------
    Public Interface:
            getInfo()
    
    Returns BatchStatusInfo object
     
    -----------------------------------------------------------------------
    '''
    def getInfo(self, maxtime=0):
        '''
        Returns aggregate info about jobs in batch system. 
        '''
        raise NotImplementedError


class WMSStatusInterface(object):
    '''
    -----------------------------------------------------------------------
    Interface for all WMSStatus plugins. 
    Should return information about cloud status, site status and jobs status. 
    -----------------------------------------------------------------------
    Public Interface:
            getCloudInfo()
            getSiteInfo()
            getJobsInfo()
    -----------------------------------------------------------------------
    '''
    def getCloudInfo(self, cloud, maxtime=0):
        '''
        Method to get and updated picture of the cloud status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError

    def getSiteInfo(self, site, maxtime=0):
        '''
        Method to get and updated picture of the site status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError

    def getJobsInfo(self, site, maxtime=0):
        '''
        Method to get and updated picture of the jobs status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError


class BatchSubmitInterface(object):
    '''
    -----------------------------------------------------------------------
    Interacts with underlying batch system to submit jobs. 
    It should be instantiated one per queue. 
    -----------------------------------------------------------------------
    Public Interface:
            submitPilots(number)
    -----------------------------------------------------------------------
    '''
    def submitPilots(self, queue, number, fcl, qcl):
        '''
        Method to submit pilots 
        '''
        raise NotImplementedError

