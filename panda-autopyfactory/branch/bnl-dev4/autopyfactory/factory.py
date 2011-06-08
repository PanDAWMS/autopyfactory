#! /usr/bin/env python
#
# Simple(ish) python condor_g factory for panda pilots
#
# $Id: factory.py 7688 2011-04-08 22:15:52Z jhover $

#
#  Somehow we need to normalize the author list, and add Jose Caballero ;)
#

#
#
#  Copyright (C) 2007,2008,2009,2010 Graeme Andrew Stewart
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.



import logging
import threading
import time

from autopyfactory.configloader import FactoryConfigLoader, QueueConfigLoader
from autopyfactory.exceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.monitor import Monitor

import userinterface.Client as Client
          
class Factory:
        '''
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
        '''

        def __init__(self, fcl):
                '''
                fcl is a FactoryConfigLoader object. 
                '''
                self.log = logging.getLogger('main.factory')
                self.log.debug('Factory initializing...')
                self.fcl = fcl
                #self.dryRun = fcl.config.get("Factory", "dryRun")
                #self.cycles = fcl.config.get("Factory", "cycles")
                #self.sleep = fcl.config.get("Factory", "sleep")
                self.log.debug("queueConf file(s) = %s" % fcl.config.get('Factory', 'queueConf'))
                self.qcl = QueueConfigLoader(fcl.config.get('Factory', 'queueConf').split(','))
                #self.queuesConfigParser = self.qcl.config
                self.wmsmanager = WMSQueuesManager(self)
 
                self.log.debug("Factory initialized.")

        def mainLoop(self):
                '''
                Main functional loop of overall Factory. 
                Actions:
                        1. Creates all queues and starts them.
                        2. Wait for a termination signal, and
                           stops all queues when that happens.
                '''

                self.log.info("Starting all Queue threads...")
                self.update()
                
                try:
                        while True:
                                time.sleep(10)
                                self.log.debug('Checking for interrupt.')
                                
                except (KeyboardInterrupt): 
                        logging.info("Shutdown via Ctrl-C or -INT signal.")
                        logging.debug(" Shutting down all threads...")
                        self.log.info("Joining all Queue threads...")
                        self.wmsmanager.join()
                        self.log.info("All Queue threads joined. Exitting.")

        def update(self):
                """method to update the status of the WMSQueuesManager object.
                This method will be used every time the 
                status of the queues changes: 
                        - at the very beginning
                        - when the config files change
                That means this method will be invoked by the regular factory
                main loop code or from any method capturing specific signals.
                """
                newqueues = self.qcl.config.sections()
                self.wmsmanager.update(newqueues) 


# ==============================================================================                                
#                       QUEUES MANAGEMENT
# ==============================================================================                                

class WMSQueuesManager(object):
        """container with the list of WMSQueue objects
        Public Interface:
                __init__(factory)
                update(newqueues)
                join()
        """
        def __init__(self, factory):
                """
                """
                self.queues = {}
                self.factory = factory

        # --------------------
        #  public interface
        # --------------------
        
        def update(self, newqueues):
                """compares the new list of queues with the current one
                        1. creates and starts new queues if needed
                        2. stops and deletes old queues if needed
                """
                currentqueues = self.queues.keys()
                queues_to_remove, queues_to_add = \
                        self.__diff_lists(currentqueues, newqueues)
                self.__addqueues(queues_to_add) 
                self.__delqueues(queues_to_remove)
                self.__refresh()

        def join(self):
                """joins all WMSQueue objects
                QUESTION: should the queues also be removed from self.queues ?
                """
                for q in self.queues.values():
                        q.join()
        
        # --------------------
        #  private methods
        # --------------------

        def __addqueues(self, queues):
                """creates new WMSQueue objects
                """
                for qname in queues:
                        self.__add(qname)

        def __add(self, qname):
                """creates a single new WMSQueue object
                and starts it
                """
                qobject = WMSQueue(qname, self.factory)
                self.queues[qname] = qobject
                qobject.start()
                
        def __delqueues(self, queues):
                """deletes WMSQueue objects
                """
                for qname in queues:
                        q = self.queues[qname]
                        q.join()
                        self.queues.pop(qname)

        def __del(self, qname):
                """deletes a single queue object from the list
                and stops it
                """
                qobject = self.__get(qname)
                qname.join()
                self.queues.pop(qname)
        
        def __refresh(self):
                """calls method refresh() for all WMSQueue objects
                """
                for q in self.queues.values():
                        q.refresh()

        # --------------------
        #  ancillas 
        # --------------------

        def __diff_lists(self, l1, l2):
                """ancilla method to calculate diff between two lists
                """
                d1 = [i for i in l1 if not i in l2]
                d2 = [i for i in l2 if not i in l1]
                return d1, d2
 

class WMSQueue(threading.Thread):
        '''
        Encapsulates all the functionality related to servicing each queue (i.e. siteid, i.e. site).
        '''
        
        def __init__(self, siteid, factory):
                '''
                siteid is the name of the section in the queueconfig
                fcl is a the Factory object who created the queue 
                '''

                threading.Thread.__init__(self) # init the thread
                self.log = logging.getLogger('main.pandaqueue')
                self.stopevent = threading.Event()

                self.siteid = siteid          # Queue section designator from config
                self.factory = factory
                self.fcl = self.factory.fcl 
                self.qcl = self.factory.qcl 

                ##self.specs = self.factory.getQueueConfig(siteid)

                #self.nickname = self.qcl.config.get(siteid, "nickname")
                self.dryrun = self.fcl.config.get("Factory", "dryRun")
                self.cycles = self.fcl.config.get("Factory", "cycles" )
                self.sleep = int(self.fcl.config.get("Factory", "sleep"))
                self.cyclesrun = 0
                
                # Handle sched plugin
                self.scheduler = self.__getscheduler()

                # FIXME !!
                # Handle status and submit batch plugins. 
                self.batchstatus = self.__getbatchstatusplugin()

                # Handle status and submit batch plugins. 
                self.batchsubmit = self.__getbatchsubmitplugin()

               
        def __getscheduler(self): 
                """
                private method to find out the specific Sched Plugin
                to be used for this queue.
                """
                schedclass = self.qcl.config.get(self.siteid, "schedplugin")
                self.log.debug("[%s] Attempting to import derived classname: \
                                autopyfactory.plugins.%sSchedPlugin.%sSchedPlugin"
                                % (self.siteid,schedclass,schedclass))                                
                _temp = __import__("autopyfactory.plugins.%sSchedPlugin" % (schedclass), 
                                                                 fromlist=["%sSchedPlugin" % schedclass])
                SchedPlugin = _temp.SchedPlugin
                # all plugins have a class SchedPlugin
                return SchedPlugin()

        def __getbatchstatusplugin(self):
                """
                private method to find out the specific Batch Status Plugin
                to be used for this queue.
                """

                batchstatclass = self.qcl.config.get(self.siteid, "batchplugin")

                _temp =  __import__("autopyfactory.plugins.%sBatchPlugin" % batchstatclass, 
                                                                 fromlist=["%sBatchStatusPlugin" %batchstatclass ])
                BatchStatusPlugin = _temp.BatchStatusPlugin
                return BatchStatusPlugin(self)
                
        def __getbatchsubmitplugin(self):
                """
                private method to find out the specific Batch Submit Plugin
                to be used for this queue.
                """

                batchsubmitclass = self.qcl.config.get(self.siteid, "batchplugin")

                _temp =  __import__("autopyfactory.plugins.%sBatchSubmitPlugin" % batchsubmitclass, 
                                                                 fromlist=["%sBatchSubmitPlugin" %batchsubmitclass ])
                BatchSubmitPlugin = _temp.BatchSubmitPlugin
                self.log.debug("[%s] WMSQueue initialization done." % self.siteid)
                return BatchSubmitPlugin(self)


        def run(self):
                '''
                Method called by thread.start()
                Main functional loop of this Queue. 
                '''        
                while not self.stopevent.isSet():
                        self.log.debug("[%s] Would be grabbing Batch info relevant to this queue." % self.siteid)
                        # update batch info
                        # update panda info
                        self.log.debug("[%s] Would be getting panda info relevant to this queue."% self.siteid)
                        
                        self.log.debug("[%s] Would be calculating number to submit."% self.siteid)
                        # calculate number to submit
                        #nsub = self.scheduler.calcSubmitNum()
                        # submit using this number
                        self.log.debug("[%s] Would be submitting jobs for this queue."% self.siteid)
                        #self.submitPilots(nsub)
                        # Exit loop if desired number of cycles is reached...  
                        self.log.debug("[%s] Checking to see how many cycles to run."% self.siteid)
                        if self.cycles and self.cyclesrun >= self.cycles:
                                self.stopevent.set()                        
                        self.log.debug("[%s] Incrementing cycles..."% self.siteid)
                        self.cyclesrun += 1
                        # sleep interval
                        self.log.debug("[%s] Sleeping for %d seconds..." % (self.siteid, self.sleep))
                        time.sleep(self.sleep)

        def refresh(self):
                """ method to reload, when requested, the config file
                """
                pass 
                # TO BE IMPLEMENTED
                          
        def join(self,timeout=None):
                """
                Stop the thread. Overriding this method required to handle Ctrl-C from console.
                """
                self.stopevent.set()
                self.log.debug('[%s] Stopping thread...' % self.siteid )
                threading.Thread.join(self, timeout)
                 

# ==============================================================================                                
#                      INTERFACES & TEMPLATES
# ==============================================================================                                

class Status(object):
        """ancilla class to collect all relevant status variables
        in a single object.
        """
        def __init__(self):
                # I use None instead of 0 to distinguish between
                #       - value has been provided and it is 0
                #       - value not provided 
                self.activated = None
                self.failed = None
                self.running = None
                self.transferring = None


class SchedInterface(object):
        '''
        Calculates the number of jobs to submit for a queue. 
        '''
        def calcSubmitNum(self, status):
                '''
                Calculates and exact number of new pilots to submit, 
                based on provided Panda site info
                and whatever relevant parameters are in config.
                All Panda info, not all relevant:    
                'activated': 0,
                'assigned': 0,
                'cancelled': 0,
                'defined': 0,
                'failed': 4
                'finished': 493,
                'holding' : 3,
                'running': 18,
                'transferring': 38},
                '''
                raise NotImplementedError

