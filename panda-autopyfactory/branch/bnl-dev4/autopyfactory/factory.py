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

        Actions are triggered method __refresh().
        __refresh() can be invoked at the beginning, from __init__,
        or when needed. For example, is an external SIGNAL is received.
        When it happens, __refresh() does several things:
                1. reloads the config file 
                2. inspects if there are new queues, or old queues have been 
                   removed.
                        2.1. creates the new WMSQueue objects for the new queues
                        2.2. kills and deletes the dissapeared queues
                3. tells all WMSQueue object to refresh

        Information about queues created and running is stored in a dictionary
                1. keys are the queue names
                2. values are the actual WMSQueue objects
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
                #self.log.debug("queueConf file(s) = %s" % fcl.config.get('Factory', 'queueConf'))
                #self.qcl = QueueConfigLoader(fcl.config.get('Factory', 'queueConf').split(','))
                #self.queuesConfigParser = self.qcl.config
                
                # Create all WMSQueue objects
                self.queues = {} # a dictionary {qname:WMSQueue object}
                self.__refresh()               
 
                self.log.debug("Factory initialized.")

        def __refresh(self):
                self.log.debug("queueConf file(s) = %s" % fcl.config.get('Factory', 'queueConf'))
                self.qcl = QueueConfigLoader(fcl.config.get('Factory', 'queueConf').split(','))
                self.__checkqueues()
                self.__refreshqueues()

        def __checkqueues(self):
                """internal method to create all WMSQueue queue objects
                Each queue to be created is a section in the queues cofig file.
                Inputs for each queue are:
                        - the name of the queue object, 
                          which is the name of the section in the config file
                        - a reference to the Factory itself.
                """
                newqueues = self.qcl.config.sections()
                currentqueues = self.queues.keys()
                queues_to_remove, queues_to_add = self.__diff_lists(currentqueues, newqueues)
                self.__addqueues(queues_to_add) 
                self.__delqueues(queues_to_remove)

        def __addqueues(self, queues):
                """creates new WMSQueue objects
                """
                for qname in queues:
                        q = WMSQueue(qname, self)
                        self.queues[qname, q]

        def __delqueues(self, queues):
                """deletes WMSQueue objects
                """
                for qname in queues:
                        q = self.queues[qname]
                        q.join()
                        self.queues.pop(qname)

        def __refreshqueues(self):
                """calls method refresh() for all WMSQueue objects
                """
                for q in self.queues.values():
                        q.refresh()
                                
        def __diff_lists(self, l1, l2):
                """ancilla method to calculate diff between two lists
                """
                d1 = [i for i in l1 if not i in l2]
                d2 = [i for i in l2 if not i in l1]
                return d1, d2


        def mainLoop(self):
                '''
                Main functional loop of overall Factory. 
                Actions:
                        1. creates all queues
                        2. periodically re-check the config files 
                           (or any other source of information)
                           and update the internal information.
                           Next time the queue object queries Factory
                           the new information will be pulled.  
                '''

                self.log.info("Starting all Queue threads...")
                for q in self.queues:
                        q.start()
                
                try:
                        while True:
                                time.sleep(10)
                                self.log.debug('Checking for interrupt.')
                                
                except (KeyboardInterrupt): 
                        logging.info("Shutdown via Ctrl-C or -INT signal.")
                        logging.debug(" Shutting down all threads...")
                        
                        self.log.info("Joining all Queue threads...")
                        for q in self.queues:
                                q.join()
                        
                        self.log.info("All Queue threads joined. Exitting.")
                               


 
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
                self.siteid = siteid                 # Queue section designator from config
                self.factory = factory
                self.specs = factory.getQueueConfig(siteid)
                #self.nickname = self.qcl.config.get(siteid, "nickname")
                #self.dryrun = self.fcl.config.get("Factory", "dryRun")
                self.cycles = self.fcl.config.get("Factory", "cycles" )
                self.sleep = int(self.fcl.config.get("Factory", "sleep"))
                self.cyclesrun = 0
                
                # Handle sched plugin
                schedclass = self.qcl.config.get(self.siteid, "schedplugin")
                self.log.debug("[%s] Attempting to import derived classname: autopyfactory.plugins.%sSchedPlugin.%sSchedPlugin" % (self.siteid,schedclass,schedclass))                                
                _temp = __import__("autopyfactory.plugins.%sSchedPlugin" % (schedclass), 
                                                                 fromlist=["%sSchedPlugin" % schedclass])
                SchedPlugin = _temp.SchedPlugin
                self.scheduler = SchedPlugin()
                
                # Handle status and submit batch plugins. 
                batchclass = self.qcl.config.get(self.siteid, "batchplugin")
                
                _temp =  __import__("autopyfactory.plugins.%sBatchPlugin" % batchclass, fromlist=["%sBatchPlugin" % batchclass])
                BatchStatusPlugin = _temp.BatchStatusPlugin
                self.batchstatus = BatchStatusPlugin(self)

                BatchSubmitPlugin = _temp.BatchSubmitPlugin
                self.batchsubmit = BatchSubmitPlugin(self)
                self.log.debug("[%s] WMSQueue initialization done." % self.siteid)
                
                
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
                 
