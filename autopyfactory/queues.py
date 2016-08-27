#! /usr/bin/env python

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010-2015 John Hover; 2010-2015 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.4.2"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

'''
    Main module for autopyfactory. 
'''

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
from autopyfactory.pluginsmanagement import QueuePluginDispatcher


class APFQueuesManager(object):
    '''
    -----------------------------------------------------------------------
    Container with the list of APFQueue objects.
    -----------------------------------------------------------------------
    Public Interface:
            __init__(factory)
            update(newqueues)
            join()
    -----------------------------------------------------------------------
    '''
    def __init__(self, factory):
        '''
        Initializes a container of APFQueue objects
        '''

        self.log = logging.getLogger('main.apfqueuesmanager')
        self.queues = {}
        self.factory = factory
        self.log.debug('APFQueuesManager: Object initialized.')

# ----------------------------------------------------------------------
#            Public Interface
# ---------------------------------------------------------------------- 
    def update(self, newqcl):
        '''
        Compares the new list of queues with the current one
                1. creates and starts new queues if needed
                2. stops and deletes old queues if needed
        '''

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
        self._refresh()  # right now it does not do anything...

        self._start() #starts all threads
        

    def _start(self):
        '''
        starts all APFQueue threads.
        We do it here, instead of one by one at the same time the object is created (old style),
        so can control which APFQueue threads are started and which ones are not
        in a more clear way
        '''

        for q in self.queues.values():
            if not q.isAlive():
                q.start()


    def join(self):
        '''
        Joins all APFQueue objects
        QUESTION: should the queues also be removed from self.queues ?
        '''
        count = 0
        for q in self.queues.values():
            q.join()
            count += 1
        self.log.debug('%d queues joined' %count)

    
    # ----------------------------------------------------------------------
    #  private methods
    # ----------------------------------------------------------------------

    def _addqueues(self, apfqnames):
        '''
        Creates new APFQueue objects
        '''
        count = 0
        for apfqname in apfqnames:
            self._add(apfqname)
            count += 1
        self.log.debug('%d queues in the configuration.' %count)

    def _add(self, apfqname):
        '''
        Creates a single new APFQueue object and starts it
        '''
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
            

    def start(self):
        '''
        starts all APFQueue objects from here
        '''
        self.log.debug('Starting')
        for qobject in self.queues.values():
            qobject.start()
        self.log.debug('Leaving')


    def _delqueues(self, apfqnames):
        '''
        Deletes APFQueue objects
        '''

        count = 0
        for apfqname in apfqnames:
            q = self.queues[apfqname]
            q.join()
            self.queues.pop(apfqname)
            count += 1
        self.log.debug('%d queues joined and removed' %count)


    #def _del(self, apfqname):
    #    '''
    #    Deletes a single queue object from the list and stops it.
    #    '''
    #    qobject = self._get(apfqname)
    #    qname.join()
    #    self.queues.pop(apfqname)

    
    def _refresh(self):
        '''
        Calls method refresh() for all APFQueue objects
        '''
        count = 0
        for q in self.queues.values():
            q.refresh()
            count += 1
        self.log.debug('%d queues refreshed' %count)



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
 

class APFQueue(threading.Thread):
    '''
    -----------------------------------------------------------------------
    Encapsulates all the functionality related to servicing each queue (i.e. siteid, i.e. site).
    -----------------------------------------------------------------------
    Public Interface:
            The class is inherited from Thread, so it has the same public interface.
    -----------------------------------------------------------------------
    '''
    
    def __init__(self, apfqname, factory):
        '''
        apfqname is the name of the section in the queueconfig, 
        i.e. the queue name, 
        factory is the Factory object who created the queue 
        '''

        # recording moment the object was created
        self.inittime = datetime.datetime.now()

        threading.Thread.__init__(self) # init the thread
        self.log = logging.getLogger('main.apfqueue[%s]' %apfqname)
        self.log.debug('APFQueue: Initializing object...')

        self.stopevent = threading.Event()

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
            #self.batchqueue = self.qcl.generic_get(apfqname, 'batchqueue')
            #self.cloud = self.qcl.generic_get(apfqname, 'cloud')
            self.cycles = self.fcl.generic_get("Factory", 'cycles' ,'getint')
            self.sleep = self.qcl.generic_get(apfqname, 'apfqueue.sleep', 'getint')
            self.cyclesrun = 0
            
            #self.batchstatusmaxtime = self.fcl.generic_get('Factory', 'batchstatus.maxtime', default_value=0)
            #self.wmsstatusmaxtime = self.fcl.generic_get('Factory', 'wmsstatus.maxtime', default_value=0)
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

    def _plugins(self):
        '''
         method just to instantiate the plugin objects
        '''

        pd = QueuePluginDispatcher(self)
        self.scheduler_plugins = pd.schedplugins        # a list of 1 or more plugins
        self.wmsstatus_plugin = pd.wmsstatusplugin      # a single WMSStatus plugin
        self.batchsubmit_plugin = pd.submitplugin       # a single BatchSubmit plugin
        self.batchstatus_plugin = pd.batchstatusplugin  # a single BatchStatus plugin
        self.monitor_plugins = pd.monitorplugins        # a list of 1 or more plugins

        
# Run methods

    def run(self):
        '''
        Method called by thread.start()
        Main functional loop of this APFQueue. 
        '''        

        # give information gathering, and proxy generation enough time to perhaps have info
        time.sleep(15)
        while not self.stopevent.isSet():
            self.log.debug("APFQueue [%s] run(): Beginning submit cycle." % self.apfqname)
            try:

                self._callscheds()
                self._submitpilots()
                self._monitor()
                self._exitloop()
                self._logtime() 
                          
            except Exception, e:
                ms = str(e)
                self.log.error("APFQueue[%s] run(): Caught exception: %s " % (self.apfqname, ms))
                self.log.debug("APFQueue[%s] run(): Exception: %s" % (self.apfqname, traceback.format_exc()))
            time.sleep(self.sleep)


    def _callscheds(self):
        '''
        calls the sched plugins 
        and calculates the number of pilot to submit
        '''
        nsub = 0
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

    def _submitpilots(self):
        '''
        submit using this number
        call for cleanup
        '''
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
        '''
        Exit loop if desired number of cycles is reached...  
        '''
        self.log.debug("__exitloop. Checking to see how many cycles to run.")
        if self.cycles and self.cyclesrun >= self.cycles:
                self.log.debug('_ stopping the thread because high cyclesrun')
                self.stopevent.set()                        
        self.log.debug("__exitloop. Incrementing cycles...")

    def _logtime(self):
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

        self.log.debug('__reporttime: up %d days, %d:%d, %d cycles, ~%d s/cycle' %(days, hours, minutes, self.cyclesrun, average))
        self.log.info('Up %d days, %d:%d, %d cycles, ~%d s/cycle' %(days, hours, minutes, self.cyclesrun, average))
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
        self.stopevent.set()
        self.log.debug('Stopping thread...')
        threading.Thread.join(self, timeout)

                 



