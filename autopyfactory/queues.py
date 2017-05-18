#! /usr/bin/env python

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010-2015 John Hover; 2010-2015 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.4.2"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

"""
    Main module for autopyfactory. 
"""

import datetime
import logging
import logging.handlers
import threading
import time
import traceback
import os
import platform
import pwd
import smtplib
import socket
import sys

from pprint import pprint
from optparse import OptionParser
from ConfigParser import ConfigParser

try:
    from email.mime.text import MIMEText
except:
    from email.MIMEText import MIMEText


# FIXME: many of these import are not needed. They are legacy...
from autopyfactory.apfexceptions import FactoryConfigurationFailure, PandaStatusFailure, ConfigFailure
from autopyfactory.apfexceptions import CondorVersionFailure, CondorStatusFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.cleanlogs import CleanLogs
from autopyfactory.logserver import LogServer
###from autopyfactory.pluginsmanagement import QueuePluginDispatcher
from autopyfactory.pluginmanager import PluginManager
from autopyfactory.interfaces import _thread


class APFQueuesManager(_thread):
    """
    -----------------------------------------------------------------------
    Container with the list of APFQueue objects.
    -----------------------------------------------------------------------
    Public Interface:
            __init__(factory)
            update(newqueues)
            join()
    -----------------------------------------------------------------------
    """
    def __init__(self, factory):
        """
        Initializes a container of APFQueue objects
        """

        _thread.__init__(self)
        factory.threadsregistry.add("core", self)
        self._thread_loop_interval = factory.fcl.generic_get('Factory','config.interval', 'getint', default_value=3600)

        self.log = logging.getLogger('autopyfactory')
        self.queues = {}
        self.factory = factory
        self.log.debug('APFQueuesManager: Object initialized.')

# ----------------------------------------------------------------------
#            Public Interface
# ---------------------------------------------------------------------- 

    def getConfig(self):
        """
        get updated configuration from the Factory Config plugins
        """
        newqcl = Config()
        for config_plugin in self.factory.config_plugins:
            tmpqcl = config_plugin.getConfig()
            newqcl.merge(tmpqcl)
        return newqcl
            

    def update(self, newqcl):
        """
        Compares the new list of queues with the current one
                1. creates and starts new queues if needed
                2. stops and deletes old queues if needed
        """
        self.log.debug("Performing queue update...")
        qcldiff = self.factory.qcl.compare(newqcl)
        #qcldiff is a dictionary like this
        #    {'REMOVED': [ <list of removed queues> ],
        #     'ADDED':   [ <list of new queues> ],
        #     'EQUAL':   [ <list of queues that did not change> ],
        #     'MODIFIED':[ <list of queues that changed> ] 
        #    }

        self.factory.qcl = newqcl

        self._delqueues(qcldiff['REMOVED'])
        self._addqueues(qcldiff['ADDED'])
        self._delqueues(qcldiff['MODIFIED'])
        self._addqueues(qcldiff['MODIFIED'])

        self.log.debug("Starting all queues.")
        self.startAPFQueues() #starts all threads
        

    def startAPFQueues(self):
        """
        starts all APFQueue threads.
        We do it here, instead of one by one at the same time the object is created (old style),
        so we can control which APFQueue threads are started and which ones are not
        in a more clear way
        """
        self.log.debug("%d queues exist. Starting all queue threads, if not running." % len(self.queues))
        for q in self.queues.values():
            self.log.debug("Checking queue %s" % q.apfqname)
            if not q.isAlive():
                self.log.debug("Starting queue %s." % q.apfqname)
                q.start()
            else:
                self.log.debug("Queue %s already running." % q.apfqname)

    ### Is this method being used by anyone???
    ### def join(self):
    ###     """
    ###     Joins all APFQueue objects
    ###     QUESTION: should the queues also be removed from self.queues ?
    ###     """
    ###     count = 0
    ###     for q in self.queues.values():
    ###         q.join()
    ###         count += 1
    ###     self.log.debug('%d queues joined' %count)



    # ----------------------------------------------------------------------
    #  private methods
    # ----------------------------------------------------------------------

    def _run(self):
        self.log.debug('Starting')
        newqcl = self.getConfig()
        self.update(newqcl)
        self.log.debug('Leaving')

    def _addqueues(self, apfqnames):
        """
        Creates new APFQueue objects
        """
        count = 0
        for apfqname in apfqnames:
            self._add(apfqname)
            count += 1
        self.log.debug('%d queues in the configuration.' %count)

    def _add(self, apfqname):
        """
        Creates a single new APFQueue object and starts it
        """
        queueenabled = self.factory.qcl.generic_get(apfqname, 'enabled', 'getboolean')
        globalenabled = self.factory.fcl.generic_get('Factory', 'enablequeues', 'getboolean', default_value=True)
        enabled = queueenabled and globalenabled
        
        if enabled:
            try:
                qobject = APFQueue(apfqname, self.factory)
                self.queues[apfqname] = qobject
                #qobject.start()
                self.log.info('Queue %s enabled.' %apfqname)
            except Exception, ex:
                self.log.exception('Exception captured when initializing [%s]. Queue omitted. ' %apfqname)
        else:
            self.log.debug('Queue %s not enabled.' %apfqname)
            

    ### Is this method being used by anyone???
    ### def start(self):
    ###     """
    ###     starts all APFQueue objects from here
    ###     """
    ###     self.log.debug('Starting')
    ###     for qobject in self.queues.values():
    ###         qobject.start()
    ###     self.log.debug('Leaving')


    def _delqueues(self, apfqnames):
        """
        Deletes APFQueue objects
        """

        count = 0
        for apfqname in apfqnames:
            q = self.queues[apfqname]
            q.join()
            self.queues.pop(apfqname)
            count += 1
        self.log.debug('%d queues joined and removed' %count)


    # ----------------------------------------------------------------------
    #  ancillary functions 
    # ----------------------------------------------------------------------

    def _diff_lists(self, l1, l2):
        """
        Ancillary method to calculate diff between two lists
        """
        d1 = [i for i in l1 if not i in l2]
        d2 = [i for i in l2 if not i in l1]
        return d1, d2
 

class APFQueue(_thread):
    """
    -----------------------------------------------------------------------
    Encapsulates all the functionality related to servicing each queue (i.e. siteid, i.e. site).
    -----------------------------------------------------------------------
    Public Interface:
            The class is inherited from Thread, so it has the same public interface.
    -----------------------------------------------------------------------
    """
    
    def __init__(self, apfqname, factory):
        """
        apfqname is the name of the section in the queueconfig, 
        i.e. the queue name, 
        factory is the Factory object who created the queue 
        """

        _thread.__init__(self)
        factory.threadsregistry.add("queue", self)

        # recording moment the object was created
        self.inittime = datetime.datetime.now()

        self.log = logging.getLogger('autopyfactory.queue.%s' %apfqname)
        self.log.debug('APFQueue: Initializing object...')

        # apfqname is the APF queue name, i.e. the section heading in queues.conf
        self.apfqname = apfqname
        self.factory = factory
        self.fcl = self.factory.fcl 
        self.qcl = self.factory.qcl 
        self.qcl = self.qcl.getSection(self.apfqname)  # so self.qcl only has one section (this queue) instead of all sections
        self.mcl = self.factory.mcl

        self.log.debug('APFQueue init: initial configuration:\n%s' %self.qcl.getSection(apfqname).getContent())
   
        try: 
            self.wmsqueue = self.qcl.generic_get(apfqname, 'wmsqueue')

            #self.cycles = self.fcl.generic_get("Factory", 'cycles' ,'getint')
            cycles = self.fcl.generic_get("Factory", 'cycles')
            if cycles != None:
                cycles = int(cycles)
            self.cycles = cycles
            self.cyclesrun = 0

            self.sleep = self.qcl.generic_get(apfqname, 'apfqueue.sleep', 'getint')
            self._thread_loop_interval =  self.sleep
           
        except Exception, ex:
            self.log.exception('APFQueue: exception captured while reading configuration variables to create the object.')
            raise ex

        try:
            self._plugins()
        
        except CondorVersionFailure, cvf:
            self.log.exception('APFQueue: No condor or bad version')
            raise cvf
        
        except Exception, ex:
            self.log.exception('APFQueue: Exception getting plugins' )
            raise ex
        
        self.log.debug('APFQueue: Object initialized.')


    def _run(self):
        """
        Method called by thread.start()
        Main functional loop of this APFQueue. 
        """        
        self._wait_for_info_services()
        self._callscheds()
        self._submit()
        self._monitor()
        self._exitloop()
        self._logtime() 


    def _wait_for_info_services(self):
        """
        wait for the info plugins to have valid content
        before doing actions
        """
        self.log.debug('starting')
        timeout = 1800 # 30 minutes
        start = int(time.time())
        
        # Only worry about plugins that have been defined...
        pluginstowait = []
        if self.wmsstatus_plugin is not None:
            pluginstowait.append(self.wmsstatus_plugin)
        if self.batchstatus_plugin is not None:
            pluginstowait.append(self.batchstatus_plugin)
                
        loop = True
        while loop:
            self.log.debug("Checking for info plugin valid content...")
            now = int(time.time())
            if (now - start) > timeout:
                loop = False
            else:
                infolist = []
                for p in pluginstowait:
                    info = p.getInfo()
                    infolist.append(info)
                if None not in infolist:
                    loop = False
                else:
                    time.sleep(10)
        self.log.debug('leaving')


    def _plugins(self):
        """
        get all the plugins needed by APFQueues
        """
        
        pluginmanager = PluginManager()


        schedpluginnames = self.qcl.get(self.apfqname, 'schedplugin')
        schedpluginnameslist = [i.strip() for i in schedpluginnames.split(',')]
        self.scheduler_plugins = pluginmanager.getpluginlist(self, ['autopyfactory', 'plugins', 'queue', 'sched'], schedpluginnameslist, self.qcl, self.apfqname)     # a list of 1 or more plugins

        wmsstatuspluginname = self.qcl.generic_get(self.apfqname, 'wmsstatusplugin')
        if wmsstatuspluginname is not None:
            self.wmsstatus_plugin = pluginmanager.getplugin(self, ['autopyfactory', 'plugins', 'queue', 'wmsstatus'], wmsstatuspluginname, self.qcl, self.apfqname)  # a single WMSStatus plugin
            self.wmsstatus_plugin.start() # start the thread
        else:
            self.wmsstatus_plugin = None

        batchsubmitpluginname = self.qcl.get(self.apfqname, 'batchsubmitplugin')
        self.batchsubmit_plugin = pluginmanager.getplugin(self, ['autopyfactory', 'plugins', 'queue', 'batchsubmit'], batchsubmitpluginname, self.qcl, self.apfqname)   # a single BatchSubmit plugin

        batchstatuspluginname = self.qcl.get(self.apfqname, 'batchstatusplugin')
        self.batchstatus_plugin = pluginmanager.getplugin(self, ['autopyfactory', 'plugins', 'queue', 'batchstatus'], batchstatuspluginname, self.qcl, self.apfqname)   # a single BatchStatus plugin
        self.batchstatus_plugin.start() # start the thread

        self.monitor_plugins = []
        if self.qcl.has_option(self.apfqname, 'monitorsection'):
            monitorsections = self.qcl.generic_get(self.apfqname, 'monitorsection')
            if monitorsections is not None:
                monitorsectionslist = [i.strip() for i in monitorsections.split(',')]
                for monitorsection in monitorsectionslist:
                    monitorpluginname = self.mcl.get(monitorsection, 'monitorplugin')
                    monitor_plugin = pluginmanager.getplugin(self, ['autopyfactory', 'plugins', 'queue', 'monitor'], monitorpluginname, self.mcl, monitorsection)        # a list of 1 or more plugins
                    self.monitor_plugins.append(monitor_plugin)


    def _callscheds(self, nsub=0):
        """
        calls the sched plugins 
        and calculates the number of pilot to submit
        """
        fullmsg = ""
        self.log.debug("APFQueue [%s] run(): Calling sched plugins..." % self.apfqname)
        for sched_plugin in self.scheduler_plugins:
            (nsub, msg) = sched_plugin.calcSubmitNum(nsub)
            if msg:
                if fullmsg:
                    fullmsg = "%s;%s" % (fullmsg, msg)
                else:
                    fullmsg = msg
        self.log.debug("APFQueue[%s]: All Sched plugins called. Result nsub=%s" % (self.apfqname, nsub))
        #return nsub, fullmsg
        self.nsub = nsub
        self.fullmsg = fullmsg

    def _submit(self):
        """
        submit using this number
        call for cleanup
        """
        self.log.debug("Starting")
        msg = 'Attempt to submit %s pilots for queue %s' %(self.nsub, self.apfqname)
        jobinfolist = self.batchsubmit_plugin.submit(self.nsub)
        self.log.debug("Attempted submission of %d pilots and got jobinfolist %s" % (self.nsub, jobinfolist))
        self.batchsubmit_plugin.cleanup()
        self.cyclesrun += 1
        self.log.debug("APFQueue[%s]: Submitted jobs. Joblist is %s" % (self.apfqname, jobinfolist))
        #return jobinfolist
        self.jobinfolist = jobinfolist

    def _monitor(self):

        for m in self.monitor_plugins:
            self.log.debug('APFQueue[%s] run(): calling registerJobs for monitor plugin %s' % (self.apfqname, m))
            m.registerJobs(self, self.jobinfolist)
            if self.fullmsg:
                self.log.debug('APFQueue[%s] run(): calling updateLabel for monitor plugin %s' % (self.apfqname, m))
                m.updateLabel(self.apfqname, self.fullmsg)


    def _exitloop(self):
        """
        Exit loop if desired number of cycles is reached...  
        """
        self.log.debug("__exitloop. Checking to see how many cycles to run.")
        if self.cycles and self.cyclesrun >= self.cycles:
                self.log.debug('_ stopping the thread because high cyclesrun')
                self.stopevent.set()                        
        self.log.debug("__exitloop. Incrementing cycles...")

    def _logtime(self):
        """
        report the time passed since the object was created
        """

        self.log.debug("__reporttime: Starting")

        now = datetime.datetime.now()
        delta = now - self.inittime
        days = delta.days
        seconds = delta.seconds
        hours = seconds/3600
        minutes = (seconds%3600)/60
        total_seconds = days*86400 + seconds
        average = total_seconds/self.cyclesrun

        self.log.debug('__reporttime: up %d days, %d:%d, %d cycles, ~%d s/cycle' %(days, hours, minutes, self.cyclesrun, average))
        self.log.info('Up %d days, %d:%d, %d cycles, ~%d s/cycle' %(days, hours, minutes, self.cyclesrun, average))
        self.log.debug("__reporttime: Leaving")


    def wmsstatus(self):
        """
        method to make this APFQueue to 
        be a valid WMS Status plugin for another APFQueue
        """
        return 0 # for now...


    # End of run-related methods

                 



