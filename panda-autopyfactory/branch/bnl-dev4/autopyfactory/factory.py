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
from autopyfactory.apfexceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.monitor import Monitor

import userinterface.Client as Client
          
class Factory:
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
                self.log.info('Factory initializing...')
                self.fcl = fcl
                self.dryRun = fcl.get("Factory", "dryRun")
                #self.cycles = fcl.get("Factory", "cycles")
                #self.sleep = fcl.get("Factory", "sleep")
                self.log.info("queueConf file(s) = %s" % fcl.get('Factory', 'queueConf'))
                self.qcl = QueueConfigLoader(fcl.get('Factory', 'queueConf').split(','))
                #self.queuesConfigParser = self.qcl.config
                self.wmsmanager = WMSQueuesManager(self)
 
                self.log.info("Factory initialized.")

        def mainLoop(self):
                '''
                Main functional loop of overall Factory. 
                Actions:
                        1. Creates all queues and starts them.
                        2. Wait for a termination signal, and
                           stops all queues when that happens.
                '''
                self.log.debug("Starting Factory.mainLoop")
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

                self.log.debug("Leaving Factory.mainLoop")

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
                self.log.debug("Starting Factory.update")
                newqueues = self.qcl.config.sections()
                self.wmsmanager.update(newqueues) 
                self.log.debug("Leaving Factory.update")


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
                """
                """
                self.log = logging.getLogger('main.wmsquuesmanager')
                self.queues = {}
                self.factory = factory

        # --------------------
        #  public interface
        # --------------------
        
        def update(self, newqueues):
                '''
                Compares the new list of queues with the current one
                        1. creates and starts new queues if needed
                        2. stops and deletes old queues if needed
                '''
                self.log.debug("Starting WMSQueuesManager.update with input ", newqueues)
                currentqueues = self.queues.keys()
                queues_to_remove, queues_to_add = \
                        self.__diff_lists(currentqueues, newqueues)
                self.__addqueues(queues_to_add) 
                self.__delqueues(queues_to_remove)
                self.__refresh()
                self.log.debug("Leaving WMSQueuesManager.update")

        def join(self):
                '''
                Joins all WMSQueue objects
                QUESTION: should the queues also be removed from self.queues ?
                '''
                self.log.debug("Starting WMSQueuesManager.join")
                for q in self.queues.values():
                        q.join()
                self.log.debug("Leaving WMSQueuesManager.join")
        
        # --------------------
        #  private methods
        # --------------------

        def __addqueues(self, queues):
                '''
                Creates new WMSQueue objects
                '''
                self.log.debug("Starting WMSQueuesManager.__addqueues with input ", queues)
                for qname in queues:
                        self.__add(qname)
                self.log.debug("Leaving WMSQueuesManager.__addqueues")

        def __add(self, qname):
                '''
                Creates a single new WMSQueue object and starts it
                '''
                self.log.debug("Starting WMSQueuesManager.__add with input ", qname)
                qobject = WMSQueue(qname, self.factory)
                self.queues[qname] = qobject
                qobject.start()
                self.log.debug("Leaving WMSQueuesManager.__add")
                
        def __delqueues(self, queues):
                '''
                Deletes WMSQueue objects
                '''
                self.log.debug("Starting WMSQueuesManager.__delqueues with input ", queues)
                for qname in queues:
                        q = self.queues[qname]
                        q.join()
                        self.queues.pop(qname)
                self.log.debug("Leaving WMSQueuesManager.__delqueues")

        def __del(self, qname):
                '''
                Deletes a single queue object from the list and stops it.
                '''
                self.log.debug("Starting WMSQueuesManager.__del with input ", qname)
                qobject = self.__get(qname)
                qname.join()
                self.queues.pop(qname)
                self.log.debug("Leaving WMSQueuesManager.__del")
        
        def __refresh(self):
                '''
                Calls method refresh() for all WMSQueue objects
                '''
                self.log.debug("Starting WMSQueuesManager.__refresh")
                for q in self.queues.values():
                        q.refresh()
                self.log.debug("Leaving WMSQueuesManager.__refresh")

        # --------------------
        #  ancillas 
        # --------------------

        def __diff_lists(self, l1, l2):
                '''
                Ancilla method to calculate diff between two lists
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
        
        def __init__(self, siteid, factory):
                '''
                siteid is the name of the section in the queueconfig, 
                i.e. the queue name, 
                factory is the Factory object who created the queue 
                '''

                threading.Thread.__init__(self) # init the thread
                self.log = logging.getLogger('main.wmsqueue')
                self.stopevent = threading.Event()

                self.siteid = siteid          # Queue section designator from config
                self.factory = factory
                self.fcl = self.factory.fcl 
                self.qcl = self.factory.qcl 

                self.nickname = self.qcl.get(siteid, "nickname")
                self.cloud = self.qcl.get(siteid, "cloud")
                self.dryRun = self.fcl.get("Factory", "dryRun")
                self.cycles = self.fcl.get("Factory", "cycles" )
                self.sleep = int(self.fcl.get("Factory", "sleep"))
                self.cyclesrun = 0
                
                # object Status to handle the whole system status
                self.status = Status()

                # Handle sched plugin
                self.scheduler = self.__getplugin('sched')

                # Handle status and submit batch plugins. 
                self.batchstatus = self.__getplugin('batchstatus', self)
                self.batchstatus.start()                # starts the thread
                self.wmsstatus = self.__getplugin('wmsstatus')
                self.wmsstatus.start()                  # starts the thread
                self.batchsubmit = self.__getplugin('batchsubmit')

        def __getplugin(self, action, *k, **kw):
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

                self.log.debug("Starting WMSQueue.__getplugin with inputs ", k, kw)

                plugin_prefixes = {
                        'sched' : 'Sched',
                        'wmsstatus': 'WMSStatus',
                        'batchstatus': 'BatchStatus',
                        'batchsubmit': 'BatchSubmit'
                }

                plugin_config_item = '%splugin' %action
                plugin_prefix = plugin_prefixes[action] 
                schedclass = self.qcl.get(self.siteid, plugin_config_item)
                plugin_module_name = '%s%sPlugin' %(schedclass, plugin_prefix)
                
                self.log.debug("[%s] Attempting to import derived classname: \
                                autopyfactory.plugins.%s"
                                % (self.siteid, plugin_module_name)) 

                plugin_module = __import__("autopyfactory.plugins.%s" % plugin_module_name, 
                                fromlist=["%s" % plugin_module_name])

                plugin_class = '%sPlugin' %plugin_prefix
                self.log.debug("Leaving WMSQueue.__getplugin with plugin named ", plugin_class)
                return getattr(plugin_module, plugin_class)(*k, **kw)

        # ----------------------------------------------
        #       run methods start here
        # ----------------------------------------------

        def run(self):
                '''
                Method called by thread.start()
                Main functional loop of this WMSQueue. 
                '''        
                self.log.debug("Starting WMSQueue.run")
                while not self.stopevent.isSet():
                        self.__updatestatus()
                        nsub = self.__calculatenumberofpilots()
                        self.__submitpilots(nsub)
                        self.__exitloop()
                        self.__sleep()
                self.log.debug("Leaving WMSQueue.run")

        def __updatestatus(self):
                '''
                update batch info and panda info
                '''
                self.log.debug("Starting WMSQueue.__updatestatus")
                self.log.debug("[%s] Would be grabbing Batch info relevant to this queue." % self.siteid)
                self.status.batch = self.batchstatus.getInfo(self.siteid)  # FIXME : is siteid the correct input ??
                self.log.debug("[%s] Would be getting WMS info relevant to this queue."% self.siteid)

                self.status.cloud = self.wmsstatus.getCloudInfo(self.cloud)
                self.status.site = self.wmsstatus.getSiteInfo(self.siteid) 
                self.status.jobs = self.wmsstatus.getJobsInfo(self.siteid)
                self.log.debug("Leaving WMSQueue.__updatestatus")

        def __calculatenumberofpilots(self):
                '''
                calculate number to submit
                '''
                self.log.debug("Starting WMSQueue.__calculatenumberofpilots")
                self.log.debug("[%s] Would be calculating number to submit."% self.siteid)
                nsub = self.scheduler.calcSubmitNum(self.status)
                self.log.debug("Leaving WMSQueue.__calculatenumberofpilots with output ", nsub)
                return nsub

        def __submitpilots(self, nsub):
                '''
                submit using this number
                '''
                self.log.debug("Starting WMSQueue.__submitpilots")
                self.log.debug("[%s] Would be submitting jobs for this queue."% self.siteid)
                self.batchsubmit.submitPilots(self.siteid, nsub, self.fcl, self.qcl)
                self.log.debug("Leaving WMSQueue.__submitpilots")

        def __exitloop(self):
                '''
                Exit loop if desired number of cycles is reached...  
                '''
                self.log.debug("Starting WMSQueue.__exitloop")
                self.log.debug("[%s] Checking to see how many cycles to run."% self.siteid)
                if self.cycles and self.cyclesrun >= self.cycles:
                        self.stopevent.set()                        
                self.log.debug("[%s] Incrementing cycles..."% self.siteid)
                self.cyclesrun += 1
                self.log.debug("Leaving WMSQueue.__exitloop")

        def __sleep(self):
                '''
                sleep interval
                '''
                self.log.debug("Starting WMSQueue.__sleep")
                self.log.debug("[%s] Sleeping for %d seconds..." % (self.siteid, self.sleep))
                time.sleep(self.sleep)
                self.log.debug("Leaving WMSQueue.__sleep")

        # ----------------------------------------------
        #       run methods end here
        # ----------------------------------------------

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
                self.log.debug("Starting WMSQueue.join")
                self.stopevent.set()
                self.log.debug('[%s] Stopping thread...' % self.siteid )
                threading.Thread.join(self, timeout)
                self.log.debug("Leaving WMSQueue.join")
                 

# ==============================================================================                                
#                      INTERFACES & TEMPLATES
# ==============================================================================                                

class Status(object):
        '''
        -----------------------------------------------------------------------
        Ancilla class to collect all relevant status variables in a single object.
        -----------------------------------------------------------------------
        '''
        def __init__(self):
                # I use None instead of 0 to distinguish between
                #       - value has been provided and it is 0
                #       - value not provided 

                self.cloud = {}
                self.site = {}
                self.jobs = {}
                self.batch = {}

                # example:
                self.jobs['activated'] = None
                self.jobs['failed'] = None
                self.jobs['running'] = None
                self.jobs['transferring'] = None


class Singleton(type):
        '''
        -----------------------------------------------------------------------
        Ancilla class to be used as metaclass to make other classes Singleton.
        -----------------------------------------------------------------------
        '''
        def __init__(cls, name, bases, dct):
                cls.__instance = None 
                type.__init__(cls, name, bases, dct)
        def __call__(cls, *args, **kw): 
                if cls.__instance is None:
                        cls.__instance = type.__call__(cls, *args,**kw)
                return cls.__instance



class SchedInterface(object):
        '''
        -----------------------------------------------------------------------
        Calculates the number of jobs to be submitted for a given queue. 
        -----------------------------------------------------------------------
        Public Interface:
                calcSubmitNum(status)
        -----------------------------------------------------------------------
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


class BatchStatusInterface(object):
        '''
        -----------------------------------------------------------------------
        Interacts with the underlying batch system to get job status. 
        Should return information about number of jobs currently on the desired queue. 
        -----------------------------------------------------------------------
        Public Interface:
                getInfo(queue)
                getJobInfo(queue) 
        -----------------------------------------------------------------------
        '''
        def getInfo(self, queue):
                '''
                Returns aggregate info about jobs on queue in batch system. 
                '''
                raise NotImplementedError

        def getJobInfo(self, queue):
                '''
                Returns a list of JobStatus objects, one for each job. 
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
        def getCloudInfo(self, cloud):
                '''
                Method to get and updated picture of the cloud status. 
                It returns a dictionary to be inserted directly into an
                Status object.
                '''
                raise NotImplementedError

        def getSiteInfo(self, site):
                '''
                Method to get and updated picture of the site status. 
                It returns a dictionary to be inserted directly into an
                Status object.
                '''
                raise NotImplementedError

        def getJobsInfo(self, site):
                '''
                Method to get and updated picture of the jobs status. 
                It returns a dictionary to be inserted directly into an
                Status object.
                '''
                raise NotImplementedError

        #def getInfo(self):
        #        '''
        #        Method to get and updated picture of the WMS status. 
        #        It returns a dictionary to be inserted directly into an
        #        Status object.
        #        '''
        #        raise NotImplementedError



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

