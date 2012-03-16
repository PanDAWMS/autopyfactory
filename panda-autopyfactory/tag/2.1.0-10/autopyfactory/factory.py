#! /usr/bin/env python
#
# Simple(ish) python condor_g factory for panda pilots
#
# $Id: factory.py 7680 2011-04-07 23:58:06Z jhover $
#
#
#  Copyright (C) 2007,2008,2009 Graeme Andrew Stewart
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
'''
    Main module for autopyfactory, a pilot factory for PanDA
'''

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
from optparse import OptionParser
from ConfigParser import ConfigParser

from autopyfactory.apfexceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
#from autopyfactory.configloader import FactoryConfigLoader, QueueConfigLoader
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.cleanLogs import CleanCondorLogs
from autopyfactory.logserver import LogServer
from autopyfactory.proxymanager import ProxyManager

import userinterface.Client as Client

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010,2011 John Hover; 2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

major, minor, release, st, num = sys.version_info


class FactoryCLI(object):
    """class to handle the command line invocation of APF. 
       parse the input options,
       setup everything, and run Factory class
    """
    def __init__(self):
        self.options = None 
        self.args = None
        self.log = None
        self.fcl = None
    
    def parseopts(self):
        parser = OptionParser(usage='''%prog [OPTIONS]
autopyfactory is an ATLAS pilot factory.

This program is licenced under the GPL, as set out in LICENSE file.

Author(s):
Graeme A Stewart <g.stewart@physics.gla.ac.uk>
Peter Love <p.love@lancaster.ac.uk>
John Hover <jhover@bnl.gov>
Jose Caballero <jcaballero@bnl.gov>
''', version="%prog $Id: factory.py 7680 2011-04-07 23:58:06Z jhover $")


        parser.add_option("-d", "--debug", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.DEBUG, 
                          help="Set logging level to DEBUG [default WARNING]")
        parser.add_option("-v", "--info", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.INFO, 
                          help="Set logging level to INFO [default WARNING]")
        parser.add_option("--console", 
                          dest="console", 
                          default=False,
                          action="store_true", 
                          help="Forces debug and info messages to be sent to the console")
        parser.add_option("--quiet", dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.WARNING, 
                          help="Set logging level to WARNING [default]")
        parser.add_option("--oneshot", "--one-shot", 
                          dest="cyclesToDo", 
                          default=0,
                          action="store_const", 
                          const=1, 
                          help="Run one cycle only")
        parser.add_option("--cycles", 
                          dest="cyclesToDo",
                          action="store", 
                          type="int", 
                          metavar="CYCLES", 
                          help="Run CYCLES times, then exit [default infinite]")
        parser.add_option("--sleep", dest="sleepTime", 
                          default=120,
                          action="store", 
                          type="int", 
                          metavar="TIME", 
                          help="Sleep TIME seconds between cycles [default %default]")
        parser.add_option("--conf", dest="confFiles", 
                          default="/etc/apf/factory.conf",
                          action="store", 
                          metavar="FILE1[,FILE2,FILE3]", 
                          help="Load configuration from FILEs (comma separated list)")
        parser.add_option("--log", dest="logfile", 
                          default="syslog", 
                          metavar="LOGFILE", 
                          action="store", 
                          help="Send logging output to LOGFILE or SYSLOG or stdout [default <syslog>]")
        parser.add_option("--runas", dest="runAs", 
                          #
                          # By default
                          #
                          default=pwd.getpwuid(os.getuid())[0],
                          action="store", 
                          metavar="USERNAME", 
                          help="If run as root, drop privileges to USER")
        (self.options, self.args) = parser.parse_args()

        #self.options.confFiles = self.options.confFiles.split(',')

    def setuplogging(self):
        """ 
        Setup logging 
        
        General principles we have tried to used for logging: 
        
        -- Logging syntax and semantics should be uniform throughout the program,  
           based on whatever organization scheme is appropriate.  
        
        -- Have at least a single log message at DEBUG at beginning and end of each function call.  
           The entry message should mention input parameters,  
           and the exit message should not any important result.  
           DEBUG output should be detailed enough that almost any logic error should become apparent.  
           It is OK if DEBUG messages are produced too fast to read interactively. 
        
        -- A moderate number of INFO messages should be logged to mark major  
           functional steps in the operation of the program,  
           e.g. when a persistent object is instantiated and initialized,  
           when a functional cycle/loop is complete.  
           It would be good if these messages note summary statistics,  
           e.g. "the last submit cycle submitted 90 jobs and 10 jobs finished".  
           A program being run with INFO log level should provide enough output  
           that the user can watch the program function and quickly observe interesting events. 
        
        -- Initially, all logging should be directed to a single file.  
           But provision should be made for eventually directing logging output from different subsystems  
           (submit, info, proxy management) to different files,  
           and at different levels of verbosity (DEBUG, INFO, WARN), and with different formatters.  
           Control of this distribution should use the standard Python "logging.conf" format file: 
        
        -- All messages are always printed out in the logs files, 
           but also to the stderr when DEBUG or INFO levels are selected. 
        
        -- We keep the original python levels meaning,  
           including WARNING as being the default level.  
        
                DEBUG      Detailed information, typically of interest only when diagnosing problems. 
                INFO       Confirmation that things are working as expected. 
                WARNING    An indication that something unexpected happened,  
                           or indicative of some problem in the near future (e.g. 'disk space low').  
                           The software is still working as expected. 
                ERROR      Due to a more serious problem, the software has not been able to perform some function. 
                CRITICAL   A serious error, indicating that the program itself may be unable to continue running. 
        
        Info: 
        
          http://docs.python.org/howto/logging.html#logging-advanced-tutorial  

        """

        self.log = logging.getLogger('main')
        if self.options.logfile == "stdout":
            logStream = logging.StreamHandler()
        elif self.options.logfile == 'syslog':
            logStream = logging.handlers.SysLogHandler('/dev/log')
        else:
            lf = self.options.logfile
            logdir = os.path.dirname(lf)
            if not os.path.exists(logdir):
                os.makedirs(logdir)
            runuid = pwd.getpwnam(self.options.runAs).pw_uid
            rungid = pwd.getpwnam(self.options.runAs).pw_gid                  
            os.chown(logdir, runuid, rungid)
            logStream = logging.FileHandler(filename=lf)    

        formatter = logging.Formatter('%(asctime)s (UTC) - %(name)s: %(levelname)s: %(module)s: %(message)s')
        formatter.converter = time.gmtime  # to convert timestamps to UTC
        logStream.setFormatter(formatter)
        self.log.addHandler(logStream)

        # adding a new Handler for the console, 
        # to be used only for DEBUG and INFO modes. 
        if self.options.logLevel in [logging.DEBUG, logging.INFO]:
            if self.options.console:
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(formatter)
                console.setLevel(self.options.logLevel)
                self.log.addHandler(console)
        self.log.setLevel(self.options.logLevel)
        self.log.debug('logging initialised')


    def setuppandaenv(self):
        '''
        setting up some panda variables.
        '''

        if not 'APF_NOSQUID' in os.environ:
            if not 'PANDA_URL_MAP' in os.environ:
                os.environ['PANDA_URL_MAP'] = 'CERN,http://pandaserver.cern.ch:25085/server/panda,https://pandaserver.cern.ch:25443/server/panda'
                self.log.debug('Set PANDA_URL_MAP to %s' % os.environ['PANDA_URL_MAP'])
            else:
                self.log.debug('Found PANDA_URL_MAP set to %s. Not changed.' % os.environ['PANDA_URL_MAP'])
            if not 'PANDA_URL' in os.environ:
                os.environ['PANDA_URL'] = 'http://pandaserver.cern.ch:25085/server/panda'
                self.log.debug('Set PANDA_URL to %s' % os.environ['PANDA_URL'])
            else:
                self.log.debug('Found PANDA_URL set to %s. Not changed.' % os.environ['PANDA_URL'])
        else:
            self.log.debug('Found APF_NOSQUID set. Not changing/setting panda client environment.')


    def checkroot(self): 
        """
        If running as root, drop privileges to --runas' account.
        """
        starting_uid = os.getuid()
        starting_gid = os.getgid()
        starting_uid_name = pwd.getpwuid(starting_uid)[0]
        
        if os.getuid() != 0:
            self.log.info("Already running as unprivileged user %s" % starting_uid_name)
            
        if os.getuid() == 0:
            try:
                runuid = pwd.getpwnam(self.options.runAs).pw_uid
                rungid = pwd.getpwnam(self.options.runAs).pw_gid
                os.chown(self.options.logfile, runuid, rungid)
                
                os.setgid(rungid)
                os.setuid(runuid)
                os.seteuid(runuid)
                os.setegid(rungid)

                self._changehome()

                self.log.info("Now running as user %d:%d ..." % (runuid, rungid))
            
            except KeyError, e:
                self.log.error('No such user %s, unable run properly. Error: %s' % (self.options.runAs, e))
                sys.exit(1)
                
            except OSError, e:
                self.log.error('Could not set user or group id to %s:%s. Error: %s' % (runuid, rungid, e))
                sys.exit(1)

    def _changehome(self):
        '''
        at some point, proxyManager will make use of method
              os.path.expanduser()
        to find out the absolute path of the usercert and userkey files
        in order to renew proxy.   
        The thing is that expanduser() uses the value of $HOME
        as it is stored in os.environ, and that value still is /root/
        Ergo, if we want the path to be expanded to a different user, i.e. apf,
        we need to change by hand the value of $HOME in the environment
        '''
        os.environ['HOME'] = pwd.getpwnam(self.options.runAs).pw_dir 

    def createconfig(self):
        """Create config, add in options...
        """
        if self.options.confFiles != None:
            self.fcl = ConfigManager().getConfig(self.options.confFiles)
        
        self.fcl.set("Factory","cyclesToDo", str(self.options.cyclesToDo))
        self.fcl.set("Factory", "sleepTime", str(self.options.sleepTime))
        self.fcl.set("Factory", "confFiles", self.options.confFiles)
           
    def mainloop(self):
        """Create Factory and enter main loop
        """

        from autopyfactory.factory import Factory

        try:
            self.log.info('Creating Factory and entering main loop...')

            f = Factory(self.fcl)
            f.mainLoop()
        except KeyboardInterrupt:
            self.log.info('Caught keyboard interrupt - exitting')
            sys.exit(0)
        except FactoryConfigurationFailure, errMsg:
            self.log.error('Factory configuration failure: %s', errMsg)
            sys.exit(0)
        except ImportError, errorMsg:
            self.log.error('Failed to import necessary python module: %s' % errorMsg)
            sys.exit(0)
        except:
            # TODO - make this a logger.exception() call
            self.log.error('''Unexpected exception! \
There was an exception raised which the factory was not expecting \
and did not know how to handle. You may have discovered a new bug \
or an unforseen error condition. \
Please report this exception to Jose <jcaballero@bnl.gov>. \
The factory will now re-raise this exception so that the python stack trace is printed, \
which will allow it to be debugged - \
please send output from this message onwards. \
Exploding in 5...4...3...2...1... Have a nice day!''')
            # The following line prints the exception to the logging module
            self.log.error(traceback.format_exc(None))
            raise


          
class Factory(object):
    '''
    -----------------------------------------------------------------------
    Class implementing the main loop. 
    The class has two main goals:
            1. load the config files
            2. launch a new thread per queue 

    Information about queues created and running is stored in a 
    APFQueuesManager object.

    Actions are triggered by method update() 
    update() can be invoked at the beginning, from __init__,
    or when needed. For example, is an external SIGNAL is received.
    When it happens, update() does:
            1. calculates the new list of queues from the config file
            2. updates the APFQueuesManager object 
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
        self.log.debug('Factory: Initializing object...')

        self.fcl = fcl
        
        self.log.info("queueConf file(s) = %s" % fcl.get('Factory', 'queueConf'))
        self.qcl = ConfigManager().getConfig(fcl.get('Factory', 'queueConf'))
      
        # Handle ProxyManager
        usepman = fcl.get('Factory', 'proxymanager.enabled')
        if usepman:
                            
            pconfig = ConfigParser()
            pconfig_file = fcl.get('Factory','proxyConf')
            got_config = pconfig.read(pconfig_file)
            self.log.debug("Read config file %s, return value: %s" % (pconfig_file, got_config)) 
            self.proxymanager = ProxyManager(pconfig)
            self.log.info('ProxyManager initialized. Starting...')
            self.proxymanager.start()
            self.log.debug('ProxyManager thread started.')
        else:
            self.log.info("ProxyManager disabled.")
       
        # APF Queues Manager 
        self.wmsmanager = APFQueuesManager(self)
        
        # Set up LogServer
        ls = self.fcl.generic_get('Factory', 'logserver.enabled', 'getboolean', logger=self.log)
        lsidx = self.fcl.generic_get('Factory','logserver.index', 'getboolean', logger=self.log)
        if ls:
            logpath = self.fcl.get('Factory', 'baseLogDir')
            if not os.path.exists(logpath):
                os.makedirs(logpath)
        
            self.logserver = LogServer(port=self.fcl.get('Factory', 'baseLogHttpPort'),
                           docroot=logpath, index=lsidx
                           )

            self.log.info('LogServer initialized. Starting...')
            self.logserver.start()
            self.log.debug('LogServer thread started.')
        else:
            self.log.info('LogServer disabled. Not running.')
        
        self.log.debug('Factory shell PATH: %s' % os.getenv('PATH') )
             
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
            self.shutdown()
            raise
            
        self.log.debug("mainLoop: Leaving.")

    def update(self):
        '''
        Method to update the status of the APFQueuesManager object.
        This method will be used every time the 
        status of the queues changes: 
                - at the very beginning
                - when the config files change
        That means this method will be invoked by the regular factory
        main loop code or from any method capturing specific signals.
        '''

        self.log.debug("update: Starting")

        newqueues = self.qcl.sections()
        self.wmsmanager.update(newqueues) 

        self.log.debug("update: Leaving")

    def shutdown(self):
        '''
        Method to cleanly shut down all factory activity, joining threads, etc. 
                
        '''
        logging.debug(" Shutting down all Queue threads...")
        self.log.info("Joining all Queue threads...")
        self.wmsmanager.join()
        self.log.info("All Queue threads joined.")
        if self.fcl.get('Factory', 'proxymanager.enabled'):
            self.log.info("Shutting down Proxymanager...")
            self.proxymanager.join()
            self.log.info("Proxymanager stopped.")
        if self.fcl.get('Factory', 'logserver.enabled'):
            self.log.info("Shutting down Logserver...")
            self.logserver.join()
            self.log.info("Logserver stopped.")            
                            
            
            

            
            
            

# ==============================================================================                                
#                       QUEUES MANAGEMENT
# ==============================================================================                                

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

        self.log = logging.getLogger('main.apfquuesmanager')
        self.log.debug('APFQueuesManager: Initializing object...')

        self.queues = {}
        self.factory = factory

        self.log.info('APFQueuesManager: Object initialized.')

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
        Joins all APFQueue objects
        QUESTION: should the queues also be removed from self.queues ?
        '''

        self.log.debug("join: Starting")

        count = 0
        for q in self.queues.values():
            q.join()
            count += 1
        self.log.debug('join: %d queues joined' %count)

        self.log.debug("join: Leaving")
    
    # ----------------------------------------------------------------------
    #  private methods
    # ----------------------------------------------------------------------

    def _addqueues(self, apfqnames):
        '''
        Creates new APFQueue objects
        '''

        self.log.debug("_addqueues: Starting with input %s" %apfqnames)

        count = 0
        for apfqname in apfqnames:
            self._add(apfqname)
            count += 1
        self.log.debug('_addqueues: %d queues in the config file' %count)
        self.log.info('%d queues in the configuration.' %count)
        self.log.debug("_addqueues: Leaving")

    def _add(self, apfqname):
        '''
        Creates a single new APFQueue object and starts it
        '''

        self.log.debug("_add: Starting with input %s" %apfqname)

        enabled = self.factory.qcl.getboolean(apfqname, 'enabled') 
        if enabled:
            qobject = APFQueue(apfqname, self.factory)
            self.queues[apfqname] = qobject
            qobject.start()
            self.log.debug('_add: %s enabled.' %apfqname)
            self.log.info('Queue %s enabled.' %apfqname)
        else:
            self.log.debug('_add: %s not enabled.' %apfqname)
            self.log.info('Queue %s not enabled.' %apfqname)
        self.log.debug("_add: Leaving")
            
    def _delqueues(self, apfqnames):
        '''
        Deletes APFQueue objects
        '''

        self.log.debug("_delqueues: Starting with input %s" %apfqnames)

        count = 0
        for apfqname in apfqnames:
            q = self.queues[apfqname]
            q.join()
            self.queues.pop(apfqname)
            count += 1
        self.log.debug('_delqueues: %d queues joined and removed' %count)

        self.log.debug("_delqueues: Leaving")

    def _del(self, apfqname):
        '''
        Deletes a single queue object from the list and stops it.
        '''

        self.log.debug("_del: Starting with input %s" %apfqname)

        qobject = self._get(apfqname)
        qname.join()
        self.queues.pop(apfqname)

        self.log.debug("_del: Leaving")
    
    def _refresh(self):
        '''
        Calls method refresh() for all APFQueue objects
        '''

        self.log.debug("_refresh: Starting")

        count = 0
        for q in self.queues.values():
            q.refresh()
            count += 1
        self.log.debug('_refresh: %d queues refreshed' %count)

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
        siteid is the name of the section in the queueconfig, 
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

        self.log.debug('APFQueue init: initial configuration:\n%s' %self.qcl.getSection(apfqname).getContent())
    
        self.siteid = self.qcl.generic_get(apfqname, 'wmsqueue', default_value=apfqname, logger=self.log)
        self.batchqueue = self.qcl.generic_get(apfqname, 'batchqueue', logger=self.log)
        self.cloud = self.qcl.generic_get(apfqname, 'cloud', logger=self.log)
        self.cycles = self.fcl.generic_get("Factory", 'cycles', logger=self.log )
        self.sleep = self.qcl.generic_get(apfqname, 'apfqueue.sleep', 'getint', logger=self.log)
        self.cyclesrun = 0
        
        self.batchstatusmaxtime = self.fcl.generic_get('Factory', 'batchstatus.maxtime', default_value=0, logger=self.log)
        self.wmsstatusmaxtime = self.fcl.generic_get('Factory', 'wmsstatus.maxtime', default_value=0, logger=self.log)

        self._startmonitor()
        self._condorlogclean()
        self._plugins()

        self.log.info('APFQueue: Object initialized.')

    def _startmonitor(self):

        self.log.debug('_startmonitor: Starting')

        if self.fcl.has_option('Factory', 'monitorURL'):
            self.log.info('Instantiating a monitor...')
            from autopyfactory.monitor import Monitor
            args = dict(self.fcl.items('Factory'))
            self.monitor = Monitor(self.fcl)

        self.log.debug('_startmonitor: Leaving')

    def _condorlogclean(self):

        self.log.debug('_condorlogclean: Starting')
        self.clean = CleanCondorLogs(self)
        self.clean.start()
        self.log.debug('_condorlogclean: Leaving')

    def _plugins(self):
        '''
        auxiliar method just to instantiate the plugin objects
        '''
       
        self.log.debug('_plugins: Starting')
 
        # Handle sched plugin
        self.scheduler_cls = self._getplugin('sched')
        self.scheduler_plugin = self.scheduler_cls(self)

        # Handle status and submit batch plugins. 
        self.batchstatus_cls = self._getplugin('batchstatus')
        self.batchstatus_plugin = self.batchstatus_cls(self)
        self.batchstatus_plugin.start()                # starts the thread
        self.wmsstatus_cls = self._getplugin('wmsstatus')
        self.wmsstatus_plugin = self.wmsstatus_cls(self)
        self.wmsstatus_plugin.start()                  # starts the thread
        self.batchsubmit_cls = self._getplugin('batchsubmit')
        self.batchsubmit_plugin = self.batchsubmit_cls(self)

        # Handle config plugin, if needed
        self.config_cls = self._getplugin('config')
        if self.config_cls:
                # Note it could be None
                self.config_plugin = self.config_cls(self)

        self.log.debug('_plugins: Leaving')

    def _getplugin(self, action):
        '''
        Generic private method to find out the specific plugin
        to be used for this queue, depending on the action.
        Action can be:
                - sched
                - batchstatus
                - wmsstatus
                - batchsubmit
                - config
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
                   The name of the class is the same as the name of the module
        '''

        self.log.debug("_getplugin: Starting for action %s" %action)

        plugin_prefixes = {
                'sched' : 'Sched',
                'wmsstatus': 'WMSStatus',
                'batchstatus': 'BatchStatus',
                'batchsubmit': 'BatchSubmit',
                'config': 'Config'
        }

        plugin_config_item = '%splugin' %action
        plugin_prefix = plugin_prefixes[action] 

        if self.qcl.has_option(self.apfqname, plugin_config_item):
                schedclass = self.qcl.get(self.apfqname, plugin_config_item)
        else:
                return None

        plugin_module_name = '%s%sPlugin' %(schedclass, plugin_prefix)
        
        self.log.debug("_getplugin: Attempting to import derived classname: autopyfactory.plugins.%s"
                        % plugin_module_name)

        plugin_module = __import__("autopyfactory.plugins.%s" % plugin_module_name, 
                                   globals(), 
                                   locals(),
                                   ["%s" % plugin_module_name])

        plugin_class = plugin_module_name  #  the name of the class is the name of the module

        self.log.debug("_getplugin: Attempting to return plugin with classname %s" %plugin_class)
        self.log.debug("_getplugin: Leaving with plugin named %s" %plugin_class)
        return getattr(plugin_module, plugin_class)

# Run methods

    def run(self):
        '''
        Method called by thread.start()
        Main functional loop of this APFQueue. 
        '''        

        self.log.debug("run: Starting" )
        # give information gathering, and proxy generation enough time to perhaps have info
        time.sleep(15)
        while not self.stopevent.isSet():
            try:
                self._autofill()
                nsub = self.scheduler_plugin.calcSubmitNum()
                self._submitpilots(nsub)
                self._monitor_shout()
                self._exitloop()
                self._reporttime()           
            except Exception, e:
                self.log.error("Caught exception: %s " % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc())
            time.sleep(self.sleep)

        self.log.debug("run: Leaving")

    def _autofill(self):
        '''
        checks if the config loader needs to be autofilled
        with info coming from a Config Plugin.
        '''
        self.log.debug('_autofill: Starting')
        if self.qcl.getboolean(self.apfqname, 'autofill'):
                self.log.info('_autofill: is True, proceeding to query config plugin and merge')
                id = self.batchsubmit_cls.id
                newqcl = self.config_plugin.getConfig()
                newqcl.filterkeys('batchsubmit', 'batchsubmit.%s' %id)
                self.qcl.merge(newqcl) 
                self.log.debug('_autofill: new configuration:\n%s' %self.qcl.getSection(self.apfqname).getContent())

        self.log.debug('_autofill: Leaving')

    def _submitpilots(self, nsub):
        '''
        submit using this number
        '''

        self.log.debug("_submitpilots: Starting")
        # message for the monitor
        msg = 'Attempt to submit %d pilots for queue %s' %(nsub, self.apfqname)
        self._monitor_note(msg)

        (status, output) = self.batchsubmit_plugin.submit(nsub)
        if output:
            if status == 0:
                self._monitor_notify(output)

        self.cyclesrun += 1

        self.log.debug("_submitpilots: Leaving")

    # Monitor-releated methods

    def _monitor_shout(self):
        '''
        call monitor.shout() method
        '''

        self.log.debug("__monitor_shout: Starting.")
        if hasattr(self, 'monitor'):
            self.monitor.shout(self.apfqname, self.cyclesrun)
        else:
            self.log.debug('__monitor_shout: no monitor instantiated')
        self.log.debug("__monitor_shout: Leaving.")

    def _monitor_note(self, msg):
        '''
        collects messages for the Monitor
        '''

        self.log.debug('__monitor_note: Starting.')

        if hasattr(self, 'monitor'):
            nick = self.qcl.get(self.apfqname, 'batchqueue')
            self.monitor.msg(nick, self.apfqname, msg)
        else:
            self.log.debug('__monitor_note: no monitor instantiated')
                
        self.log.debug('__monitor__note: Leaving.')

    def _monitor_notify(self, output):
        '''
        sends all collected messages to the Monitor server
        '''

        self.log.debug('__monitor_notify: Starting.')

        if hasattr(self, 'monitor'):
            nick = self.qcl.get(self.apfqname, 'batchqueue')
            label = self.apfqname
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
                self.log.debug('__exitloop: stopping the thread because high cyclesrun')
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

        self.log.debug("join: Starting")

        self.stopevent.set()
        self.log.debug('Stopping thread...')
        threading.Thread.join(self, timeout)

        self.log.debug("join: Leaving")
                 

# ==============================================================================                                
#                     INFO CLASSES 
# ==============================================================================  
                            

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
    
    In a nutshell, the class is a dictionary of QueueInfo objects
    stored in self.queues
    -----------------------------------------------------------------------
    Public Interface:
            valid()
    -----------------------------------------------------------------------
    '''
    def __init__(self):
        '''
        Info for each queue is retrieved, set, and adjusted via APF queuename, e.g.
            numrunning = info.queues['BNL_ATLAS_1'].running
            info.queues['BNL_ITB_1'].pending = 17
            info.queues['BNL_ITB_1'].finished += 1
            
        Any alteration access updates the info.mtime attribute. 
        '''
        
        self.log = logging.getLogger('main.batchstatus')
        self.log.debug('Status: Initializing object...')
        
        # queues is a dictionary of QueueInfo objects
        self.queues = {}
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
        s = "BatchstatusInfo containing %d queues" % len(self.queues)
        return s


    def __len__(self):
        '''
        Implement len() so debug can confirm number of queueInfo objects in this BatchStatusInfo. 
        '''
        return len(self.queues)


class QueueInfo(object):
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
    def __init__(self):
        self.pending = 0
        self.running = 0
        self.suspended = 0
        self.done = 0
        self.unknown = 0
        self.error = 0

    def __str__(self):
        s = "QueueInfo: pending=%d, running=%d, suspended=%d" % (self.pending, 
                                                                 self.running, 
                                                                 self.suspended)
        return s


# ==============================================================================                                
#                      INTERFACES & TEMPLATES
# ==============================================================================  

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


class SchedInterface(object):
    '''
    -----------------------------------------------------------------------
    Calculates the number of jobs to be submitted for a given queue. 
    -----------------------------------------------------------------------
    Public Interface:
            calcSubmitNum()
            valid()
    -----------------------------------------------------------------------
    '''
    def calcSubmitNum(self):
        '''
        Calculates number of jobs to submit for the associated APF queue. 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
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
            valid()
    
    Returns BatchStatusInfo object
     
    -----------------------------------------------------------------------
    '''
    def getInfo(self, maxtime=0):
        '''
        Returns aggregate info about jobs in batch system. 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
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
            valid()
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

    def valid(self):
        '''
        Says if the object has been initialized properly
        '''
        raise NotImplementedError


class ConfigInterface(object):
    '''
    -----------------------------------------------------------------------
    Returns info to complete the queues config objects
    -----------------------------------------------------------------------
    Public Interface:
            getInfo()
            valid()
    -----------------------------------------------------------------------
    '''
    def getConfig(self):
        '''
        returns info 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
        '''
        raise NotImplementedError


class BatchSubmitInterface(object):
    '''
    -----------------------------------------------------------------------
    Interacts with underlying batch system to submit jobs. 
    It should be instantiated one per queue. 
    -----------------------------------------------------------------------
    Public Interface:
            submit(number)
            valid()
            addJSD()
            writeJSD()
    -----------------------------------------------------------------------
    '''
    def submit(self, n):
        '''
        Method to submit pilots 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
        '''
        raise NotImplementedError

    def addJSD(self):
        '''
        Adds content to the JSD file
        '''
        raise NotImplementedError
        
    def writeJSD(self):
        '''
        Writes on file the content of the JSD file
        '''
        raise NotImplementedError

