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

from autopyfactory.apfexceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.configloader import FactoryConfigLoader, QueueConfigLoader

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
                self.log.info('Factory: Initializing object...')

                self.fcl = fcl
                self.dryRun = fcl.get("Factory", "dryRun")
                #self.sleep = fcl.get("Factory", "sleep")
                
                self.log.info("queueConf file(s) = %s" % fcl.get('Factory', 'queueConf'))
                self.qcl = QueueConfigLoader(fcl.get('Factory', 'queueConf').split(','))
                #self.queuesConfigParser = self.qcl.config
                self.wmsmanager = WMSQueuesManager(self)
 
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
                                time.sleep(10)
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
        #  public interface
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
                        self.__diff_lists(currentqueues, newqueues)
                self.__addqueues(queues_to_add) 
                self.__delqueues(queues_to_remove)
                self.__refresh()

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

        def __addqueues(self, queues):
                '''
                Creates new WMSQueue objects
                '''

                self.log.debug("__addqueues: Starting with input %s" %queues)

                count = 0
                for qname in queues:
                        self.__add(qname)
                        count += 1
                self.log.info('__addqueues: %d queues added' %count)

                self.log.debug("__addqueues: Leaving")

        def __add(self, qname):
                '''
                Creates a single new WMSQueue object and starts it
                '''

                self.log.debug("__add: Starting with input %s" %qname)

                qobject = WMSQueue(qname, self.factory)
                self.queues[qname] = qobject
                qobject.start()

                self.log.debug("__add: Leaving")
                
        def __delqueues(self, queues):
                '''
                Deletes WMSQueue objects
                '''

                self.log.debug("__delqueues: Starting with input %s" %queues)

                count = 0
                for qname in queues:
                        q = self.queues[qname]
                        q.join()
                        self.queues.pop(qname)
                        count += 1
                self.log.info('__delqueues: %d queues joined and removed' %count)

                self.log.debug("__delqueues: Leaving")

        def __del(self, qname):
                '''
                Deletes a single queue object from the list and stops it.
                '''

                self.log.debug("__del: Starting with input %s" %qname)

                qobject = self.__get(qname)
                qname.join()
                self.queues.pop(qname)

                self.log.debug("__del: Leaving")
        
        def __refresh(self):
                '''
                Calls method refresh() for all WMSQueue objects
                '''

                self.log.debug("__refresh: Starting")

                count = 0
                for q in self.queues.values():
                        q.refresh()
                        count += 1
                self.log.info('__refresh: %d queues refreshed' %count)

                self.log.debug("__refresh: Leaving")

        # ----------------------------------------------------------------------
        #  ancillas 
        # ----------------------------------------------------------------------

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

                # recording moment the object was created
                self.inittime = datetime.datetime.now()

                threading.Thread.__init__(self) # init the thread
                self.log = logging.getLogger('main.wmsqueue[%s]' %siteid)
                self.log.info('WMSQueue: Initializing object...')

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

                # Monitor
                if self.fcl.has_option('Factory', 'monitorURL'):
                        from autopyfactory.monitor import Monitor
                        args = dict(self.fcl.items('Factory'))
                        args.update(dict(self.fcl.items('Pilots')))
                        self.monitor = Monitor(**args)

                # Handle status and submit batch plugins. 
                self.batchstatus = self.__getplugin('batchstatus', self)
                self.batchstatus.start()                # starts the thread
                self.wmsstatus = self.__getplugin('wmsstatus', self)
                self.wmsstatus.start()                  # starts the thread
                self.batchsubmit = self.__getplugin('batchsubmit')
                self.log.info('WMSQueue: Object initialized.', self)

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

                self.log.debug("__getplugin: Starting with inputs %s and %s" %( k, kw))

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
                
                self.log.info("Attempting to import derived classname: autopyfactory.plugins.%s"
                                % plugin_module_name)

                plugin_module = __import__("autopyfactory.plugins.%s" % plugin_module_name, 
                                fromlist=["%s" % plugin_module_name])
                plugin_class = '%sPlugin' %plugin_prefix

                self.log.info("__getplugin: Attempting to return plugin with classname %s" %plugin_class)

                self.log.debug("__getplugin: Leaving with plugin named %s" %plugin_class)
                return getattr(plugin_module, plugin_class)(*k, **kw)

        # ----------------------------------------------
        #       run methods start here
        # ----------------------------------------------

        def run(self):
                '''
                Method called by thread.start()
                Main functional loop of this WMSQueue. 
                '''        

                self.log.debug("run: Starting" )

                while not self.stopevent.isSet():
                        self.__updatestatus()
                        nsub = self.__calculatenumberofpilots()
                        self.__submitpilots(nsub)
                        self.__monitor_shout()
                        self.__exitloop()
                        self.__sleep()
                        self.__reporttime()

                self.log.debug("run: Leaving")

        def __updatestatus(self):
                '''
                update batch info and panda info
                '''

                self.log.debug("__updatestatus: Starting")

                ## ? ## self.log.debug("Would be grabbing Batch info relevant to this queue.")
                self.status.batch = self.batchstatus.getInfo(self.siteid)  # FIXME : is siteid the correct input ??
                ## ? ## self.log.debug("Would be getting WMS info relevant to this queue.")
                self.status.cloud = self.wmsstatus.getCloudInfo(self.cloud)
                self.status.site = self.wmsstatus.getSiteInfo(self.siteid)
                self.status.jobs = self.wmsstatus.getJobsInfo(self.siteid)

                self.log.debug("__updatestatus: Leaving")

        def __calculatenumberofpilots(self):
                '''
                calculate number to submit
                '''

                self.log.debug("__calculatenumberofpilots: Starting")

                self.log.debug("Would be calculating number to submit.")
                nsub = self.scheduler.calcSubmitNum(self.status)

                self.log.debug("__calculatenumberofpilots: Leaving with output %s" %nsub)
                return nsub

        def __submitpilots(self, nsub):
                '''
                submit using this number
                '''

                self.log.debug("__submitpilots: Starting")

                self.log.debug("Would be submitting jobs for this queue.")
                # message for the monitor
                msg = 'Attempt to submit %d pilots for queue %s' %(nsub, self.siteid)
                self.__monitor_note(msg)

                (status, output) = self.batchsubmit.submitPilots(self.siteid, nsub, self.fcl, self.qcl)
                if output:
                        if status == 0:
                                self.__monitor_notify(output)

                self.cyclesrun += 1

                self.log.debug("__submitpilots: Leaving")

        # ------------------------------------------------------------ 
        #       Monitor ancillas 
        # ------------------------------------------------------------ 

        def __monitor_shout(self):
                '''
                call monitor.shout() method
                '''

                self.log.debug("__monitor_shout: Starting.")
                if hasattr(self, 'monitor'):
                        self.monitor.shout(self.siteid, self.cyclesrun)
                else:
                        self.log.info('__monitor_shout: no monitor instantiated')
                self.log.debug("__monitor_shout: Leaving.")

        def __monitor_note(self, msg):
                '''
                collects messages for the Monitor
                '''

                self.log.debug('__monitor_note: Starting.')

                if hasattr(self, 'monitor'):
                        nick = self.qcl.get(self.siteid, 'nickname')
                        self.monitor.msg(nick, self.siteid, msg)
                else:
                        self.log.info('__monitor_note: no monitor instantiated')
                        
                self.log.debug('__monitor__note: Leaving.')

        def __monitor_notify(self, output):
                '''
                sends all collected messages to the Monitor server
                '''

                self.log.debug('__monitor_notify: Starting.')

                if hasattr(self, 'monitor'):
                        nick = self.qcl.get(self.siteid, 'nickname')
                        label = self.siteid
                        self.mon.notify(nick, label, output)
                else:
                        self.log.info('__monitor_notify: no monitor instantiated')

                self.log.debug('__monitor_notify: Leaving.')


        def __exitloop(self):
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

        def __sleep(self):
                '''
                sleep interval
                '''

                self.log.debug("__sleep: Starting")

                self.log.debug("__sleep. Sleeping for %d seconds..." %self.sleep)
                time.sleep(self.sleep)

                self.log.debug("__sleep: Leaving")

        def __reporttime(self):
                '''
                report the time passed since the object was created
                '''

                self.log.debug("__reporttime: Starting")
        
                now = datetime.datetime.now()
                delta = now - self.inittime
                self.log.info('__reportime: %d days and %d seconds since this queue started running.' %(delta.days, delta.seconds))

                self.log.debug("__reportime: Leaving")

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

                self.log.debug("join: Starting")

                self.stopevent.set()
                self.log.info('Stopping thread...')
                threading.Thread.join(self, timeout)

                self.log.debug("join: Leaving")
                 

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

                self.log = logging.getLogger('main.status')
                self.log.info('Status: Initializing object...')

                self.cloud = {}
                self.site = {}
                self.jobs = {}
                self.batch = {}

                # example:
                self.jobs['activated'] = None
                self.jobs['failed'] = None
                self.jobs['running'] = None
                self.jobs['transferring'] = None

                self.log.info('Status: Object Initialized')


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

