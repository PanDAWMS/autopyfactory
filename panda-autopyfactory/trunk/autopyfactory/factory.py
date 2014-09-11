#! /usr/bin/env python

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010-2014 John Hover; 2010-2014 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.4.0"
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


from autopyfactory.apfexceptions import FactoryConfigurationFailure, PandaStatusFailure, ConfigFailure
from autopyfactory.apfexceptions import CondorVersionFailure, CondorStatusFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.cleanlogs import CleanLogs
from autopyfactory.logserver import LogServer
from autopyfactory.proxymanager import ProxyManager

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

        self.__presetups()
        self.__parseopts()
        self.__setuplogging()
        self.__platforminfo()
        self.__checkroot()
        self.__createconfig()


    def __presetups(self):
        '''
        we put here some preliminary steps that 
        for one reason or another 
        must be done before anything else
        '''

        self.__addloggingtrace()


    def __addloggingtrace(self):
        """
        Adding custom TRACE level
        This must be done here, because parseopts()
        uses logging.TRACE, so it must be defined
        by that time
        """

        logging.TRACE = 5
        logging.addLevelName(logging.TRACE, 'TRACE')
        def trace(self, msg, *args, **kwargs):
            self.log(logging.TRACE, msg, *args, **kwargs)
        logging.Logger.trace = trace

        #
        #   NOTE:
        #
        #   I have been told that this way, messing with the root logging,
        #   can have problems with multi-threaded applications...
        #   Apparently, the best way to do it is
        #   with a dedicated Logger class:
        #   
        #           class MyLogger(logging.getLoggerClass()):
        #           
        #               TRACE = 5
        #               logging.addLevelName(TRACE, "TRACE")
        #           
        #               def trace(self, msg, *args, **kwargs):
        #                   self.log(self.TRACE, msg, *args, **kwargs)
        #           
        #           logging.setLoggerClass(MyLogger)
        #
        #   but that only works fine is we never 
        #   call the logger root, as we are doing
        #   Also, it has the problem that logging.TRACE would not be defined.
        #
        #
        #   Another option is
        #       
        #           logging.trace = functools.partial(logging.log, logging.TRACE)
        #
        #   Related documentation on partial() can be found here
        #
        #           http://docs.python.org/2/library/functools.html
        #


    
    def __parseopts(self):
        parser = OptionParser(usage='''%prog [OPTIONS]
autopyfactory is an ATLAS pilot factory.

This program is licenced under the GPL, as set out in LICENSE file.

Author(s):
Graeme A Stewart <g.stewart@physics.gla.ac.uk>
Peter Love <p.love@lancaster.ac.uk>
John Hover <jhover@bnl.gov>
Jose Caballero <jcaballero@bnl.gov>
''', version="%prog $Id: factory.py 7680 2011-04-07 23:58:06Z jhover $")


        parser.add_option("--trace", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.TRACE, 
                          help="Set logging level to TRACE [default WARNING], super verbose")
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
                          default="/etc/autopyfactory/autofactory.conf",
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

    def __setuplogging(self):
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
        
        -- We add a new custom level -TRACE- to be more verbose than DEBUG.

        Info: 
        
          http://docs.python.org/howto/logging.html#logging-advanced-tutorial  

        """
        self.log = logging.getLogger()
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

        if major == 2 and minor == 4:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d : %(message)s'
        else:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        formatter = logging.Formatter(FORMAT)
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
        self.log.info('Logging initialized.')


    def _printenv(self):

        envmsg = ''        
        for k in sorted(os.environ.keys()):
            envmsg += '\n%s=%s' %(k, os.environ[k])
        self.log.debug('Environment : %s' %envmsg)


    def __platforminfo(self):
        '''
        display basic info about the platform, for debugging purposes 
        '''
        self.log.info('platform: uname = %s %s %s %s %s %s' %platform.uname())
        self.log.info('platform: platform = %s' %platform.platform())
        self.log.info('platform: python version = %s' %platform.python_version())
        self._printenv()

    def __checkroot(self): 
        """
        If running as root, drop privileges to --runas' account.
        """
        starting_uid = os.getuid()
        starting_gid = os.getgid()
        starting_uid_name = pwd.getpwuid(starting_uid)[0]

        hostname = socket.gethostname()
        
        if os.getuid() != 0:
            self.log.info("Already running as unprivileged user %s at %s" % (starting_uid_name, hostname))
            
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
                self._changewd()

                self.log.info("Now running as user %d:%d at %s..." % (runuid, rungid, hostname))
                self._printenv()

            
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
        Ergo, if we want the path to be expanded to a different user, i.e. autopyfactory,
        we need to change by hand the value of $HOME in the environment
        '''
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir 
        os.environ['HOME'] = runAs_home
        self.log.debug('Setting up environment variable HOME to %s' %runAs_home)


    def _changewd(self):
        '''
        changing working directory to the HOME directory of the new user,
        typically "autopyfactory". 
        When APF starts as a daemon, working directory may be "/".
        If APF was called from the command line as root, working directory is "/root".
        It is better is current working directory is just the HOME of the running user,
        so it is easier to debug in case of failures.
        '''
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir
        os.chdir(runAs_home)
        self.log.debug('Switching working directory to %s' %runAs_home)


    def __createconfig(self):
        """Create config, add in options...
        """
        if self.options.confFiles != None:
            try:
                self.fcl = ConfigManager().getConfig(self.options.confFiles)
            except ConfigFailure, errMsg:
                self.log.error('Failed to create FactoryConfigLoader')
                sys.exit(1)
        
        self.fcl.set("Factory","cyclesToDo", str(self.options.cyclesToDo))
        self.fcl.set("Factory", "sleepTime", str(self.options.sleepTime))
        self.fcl.set("Factory", "confFiles", self.options.confFiles)
           
    def run(self):
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
            sys.exit(1)
        except ImportError, errorMsg:
            self.log.error('Failed to import necessary python module: %s' % errorMsg)
            sys.exit(1)
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
            print(traceback.format_exc(None))
            sys.exit(1)          
          
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
        self.version = __version__
        self.log = logging.getLogger('main.factory')
        self.log.info('AutoPyFactory version %s' %self.version)
        self.fcl = fcl
        # Create config loader object for queues 
        qcf = None
        qcd = None
        try: 
            qcf = fcl.get('Factory', 'queueConf')    # the configuration files for queues are a list of URIs
            self.log.debug("queues.conf file(s) = %s" % qcf)
            
        except:
            pass

        if not qcf:
            try:
                qcd = fcl.get('Factory', 'queueDirConf') # the configuration files for queues are in a directory
                self.log.debug("queues.conf directory = %s" % qcd)
            except:
                pass

        try:
            if qcf:
                self.qcl = ConfigManager().getConfig(sources=qcf)
            elif qcd:
                self.qcl = ConfigManager().getConfig(configdir=qcd)
            else:
                self.log.error('no files or directory with queues configuration specified')
                sys.exit(0)
                
        except ConfigFailure, errMsg:
            self.log.error('Failed to create QueuesConfigLoader, err: %s' % str(errMsg))
            sys.exit(0)
        
        # Handle ProxyManager configuration
        usepman = fcl.getboolean('Factory', 'proxymanager.enabled')
        if usepman:      
            pcf = fcl.get('Factory','proxyConf')
            self.log.debug("proxy.conf file(s) = %s" % pcf)
            pcl = ConfigParser()

            try:
                got_config = pcl.read(pcf)
            except Exception, e:
                self.log.error('Failed to create ProxyConfigLoader')
                self.log.debug("Exception: %s" % traceback.format_exc())
                sys.exit(0)

            self.log.debug("Read config file %s, return value: %s" % (pcf, got_config)) 
            self.proxymanager = ProxyManager(pcl, self)
            self.log.info('ProxyManager initialized. Starting...')
            self.proxymanager.start()
            self.log.debug('ProxyManager thread started.')
        else:
            self.log.info("ProxyManager disabled.")
       
        # Handle monitor configuration
        self.mcl = None
        self.mcf = self.fcl.generic_get('Factory', 'monitorConf')
        self.log.debug("monitor.conf file(s) = %s" % self.mcf)
        
        try:
            self.mcl = ConfigManager().getConfig(self.mcf)
        except ConfigFailure, e:
            self.log.error('Failed to create MonitorConfigLoader')
            sys.exit(0)

        self.log.debug("mcl is %s" % self.mcl)

        # Handle Log Serving
        self._initLogserver()

        # dump the content of queues.conf 
        qclstr = self.qcl.getContent(raw=False)
        logpath = self.fcl.get('Factory', 'baseLogDir')
        qclfile = open('%s/queues.conf' %logpath, 'w')
        print >> qclfile, qclstr
        qclfile.close()

        # APF Queues Manager 
        self.apfqueuesmanager = APFQueuesManager(self)
        
        # Collect other factory attibutes
        self.adminemail = self.fcl.get('Factory','factoryAdminEmail')
        self.factoryid = self.fcl.get('Factory','factoryId')
        self.smtpserver = self.fcl.get('Factory','factorySMTPServer')
        self.hostname = socket.gethostname()
        #self.username = os.getlogin()
        self.username = pwd.getpwuid(os.getuid()).pw_name   
        
        # Log some info...
        self.log.debug('Factory shell PATH: %s' % os.getenv('PATH') )     
        self.log.info("Factory: Object initialized.")

    def _initLogserver(self):
        # Set up LogServer
        self.log.debug("Handling LogServer...")
        ls = self.fcl.generic_get('Factory', 'logserver.enabled', 'getboolean')
        if ls:
            self.log.info("LogServer enabled. Initializing...")
            lsidx = self.fcl.generic_get('Factory','logserver.index', 'getboolean')
            lsrobots = self.fcl.generic_get('Factory','logserver.allowrobots', 'getboolean')
            logpath = self.fcl.get('Factory', 'baseLogDir')
            logurl = self.fcl.get('Factory','baseLogDirUrl')            
            logport = self._parseLogPort(logurl)
            if not os.path.exists(logpath):
                self.log.debug("Creating log path: %s" % logpath)
                os.makedirs(logpath)
            if not lsrobots:
                rf = "%s/robots.txt" % logpath
                self.log.debug("logserver.allowrobots is False, creating file: %s" % rf)
                try:
                    f = open(rf , 'w' )
                    f.write("User-agent: * \nDisallow: /")
                    f.close()
                except IOError:
                    self.log.warn("Unable to create robots.txt file...")
            self.log.debug("Creating LogServer object...")
            self.logserver = LogServer(port=logport, docroot=logpath, index=lsidx)
            self.log.info('LogServer initialized. Starting...')
            self.logserver.start()
            self.log.debug('LogServer thread started.')
        else:
            self.log.info('LogServer disabled. Not running.')

    def _parseLogPort(self, logurl):
        '''
        logUrl is like:  http[s]://hostname[:port]
        if port exists, return port
        if port is omitted, 
           if http, return 80
           if https, return 443
           
        Return value must be an int. 
        '''
        urlparts = logurl.split(':')
        urltype = urlparts[0]
        port = 80
        if len(urlparts) == 3:
            port = int(urlparts[2])
        elif len(urlparts) == 2:
            if urltype == "http":
                port = 80
            elif urltype == "https":
                port = 443
        return int(port)
        
        
    def mainLoop(self):
        '''
        Main functional loop of overall Factory. 
        Actions:
                1. Creates all queues and starts them.
                2. Wait for a termination signal, and
                   stops all queues when that happens.
        '''

        self.log.debug("Starting.")
        self.log.info("Starting all Queue threads...")

        self.update()
        self.__cleanlogs()
        
        try:
            while True:
                mainsleep = int(self.fcl.get('Factory', 'factory.sleep'))
                time.sleep(mainsleep)
                self.log.debug('Checking for interrupt.')
                        
        except (KeyboardInterrupt): 
            logging.info("Shutdown via Ctrl-C or -INT signal.")
            self.shutdown()
            raise
            
        self.log.debug("Leaving.")

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

        self.log.debug("Starting")

        newqueues = self.qcl.sections()
        self.apfqueuesmanager.update(newqueues) 

        self.log.debug("Leaving")

    def __cleanlogs(self):
        '''
        starts the thread that will clean the condor logs files
        '''

        self.log.debug('Starting')
        self.clean = CleanLogs(self)
        self.clean.start()
        self.log.debug('Leaving')

    def shutdown(self):
        '''
        Method to cleanly shut down all factory activity, joining threads, etc. 
        '''

        logging.debug(" Shutting down all Queue threads...")
        self.log.info("Joining all Queue threads...")
        self.apfqueuesmanager.join()
        self.log.info("All Queue threads joined.")
        if self.fcl.getboolean('Factory', 'proxymanager.enabled'):
            self.log.info("Shutting down Proxymanager...")
            self.proxymanager.join()
            self.log.info("Proxymanager stopped.")
        if self.fcl.getboolean('Factory', 'logserver.enabled'):
            self.log.info("Shutting down Logserver...")
            self.logserver.join()
            self.log.info("Logserver stopped.")            
            
                            
    def sendAdminEmail(self, subject, messagestring):
        msg = MIMEText(messagestring)
        msg['Subject'] = subject
        email_from = "%s@%s" % ( self.username, self.hostname)
        msg['From'] = email_from
        msg['To'] = self.adminemail
        tolist = self.adminemail.split(",")
        
        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        s = smtplib.SMTP(self.smtpserver)
        self.log.info("Sending email: %s" % msg.as_string())
        s.sendmail(email_from , tolist , msg.as_string())
        s.quit()
            
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

        self.log = logging.getLogger('main.apfqueuesmanager')
        self.queues = {}
        self.factory = factory
        self.log.debug('APFQueuesManager: Object initialized.')

# ----------------------------------------------------------------------
#            Public Interface
# ---------------------------------------------------------------------- 
    def update(self, newqueues):
        '''
        Compares the new list of queues with the current one
                1. creates and starts new queues if needed
                2. stops and deletes old queues if needed
        '''
        currentqueues = self.queues.keys()
        queues_to_remove, queues_to_add = \
                self._diff_lists(currentqueues, newqueues)
        self._addqueues(queues_to_add) 
        self._delqueues(queues_to_remove)
        self._refresh()


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
                qobject.start()
                self.log.info('Queue %s enabled.' %apfqname)
            except Exception, ex:
                self.log.error('Exception captured when initializing [%s]. Queue omitted. ' %apfqname)
                self.log.debug("Exception: %s" % traceback.format_exc())
        else:
            self.log.debug('Queue %s not enabled.' %apfqname)
            
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


    def _del(self, apfqname):
        '''
        Deletes a single queue object from the list and stops it.
        '''
        qobject = self._get(apfqname)
        qname.join()
        self.queues.pop(apfqname)

    
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
        self.mcl = self.factory.mcl

        self.log.debug('APFQueue init: initial configuration:\n%s' %self.qcl.getSection(apfqname).getContent())
   
        try: 
            self.wmsqueue = self.qcl.generic_get(apfqname, 'wmsqueue')
            self.batchqueue = self.qcl.generic_get(apfqname, 'batchqueue')
            #self.cloud = self.qcl.generic_get(apfqname, 'cloud')
            self.cycles = self.fcl.generic_get("Factory", 'cycles' )
            self.sleep = self.qcl.generic_get(apfqname, 'apfqueue.sleep', 'getint')
            self.cyclesrun = 0
            
            self.batchstatusmaxtime = self.fcl.generic_get('Factory', 'batchstatus.maxtime', default_value=0)
            self.wmsstatusmaxtime = self.fcl.generic_get('Factory', 'wmsstatus.maxtime', default_value=0)
        except Exception, ex:
            self.log.error('APFQueue: exception captured while reading configuration variables to create the object.')
            self.log.debug("Exception: %s" % traceback.format_exc())
            raise ex

        try:
            self._plugins()
        
        except CondorVersionFailure, cvf:
            self.log.error('APFQueue: No condor or bad version: %s' % str(cvf))
            raise cvf
        
        except Exception, ex:
            self.log.error('APFQueue: Exception getting plugins: %s' % str(ex))
            self.log.debug("Exception: %s" % traceback.format_exc())
            raise ex
        
        self.log.debug('APFQueue: Object initialized.')

    def _plugins(self):
        '''
         method just to instantiate the plugin objects
        '''

        pd = PluginDispatcher(self)
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
                jobinfolist = self._submitpilots(nsub)
                self.log.debug("APFQueue[%s]: Submitted jobs. Joblist is %s" % (self.apfqname, jobinfolist))
                for m in self.monitor_plugins:
                    self.log.debug('APFQueue[%s] run(): calling registerJobs for monitor plugin %s' % (self.apfqname, m))
                    m.registerJobs(self, jobinfolist)
                    if fullmsg:
                        self.log.debug('APFQueue[%s] run(): calling updateLabel for monitor plugin %s' % (self.apfqname, m))
                        m.updateLabel(self.apfqname, fullmsg)
                self._exitloop()
                self._logtime() 
                          
            except Exception, e:
                ms = str(e)
                self.log.error("APFQueue[%s] run(): Caught exception: %s " % (self.apfqname, ms))
                self.log.debug("APFQueue[%s] run(): Exception: %s" % (self.apfqname, traceback.format_exc()))
            time.sleep(self.sleep)

    def _submitpilots(self, nsub):
        '''
        submit using this number
        call for cleanup
        '''
        self.log.debug("Starting")
        msg = 'Attempt to submit %s pilots for queue %s' %(nsub, self.apfqname)
        jobinfolist = self.batchsubmit_plugin.submit(nsub)
        self.log.debug("Attempted submission of %d pilots and got jobinfolist %s" % (nsub,        
                                                                                                    jobinfolist))
        self.batchsubmit_plugin.cleanup()
        self.cyclesrun += 1
        return jobinfolist


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

                 

# ==============================================================================                                
#                     PLUGIN CLASSES
# ==============================================================================  

class PluginHandler(object):

    def __init__(self):

        self.plugin_name = None
        self.plugin_class_name = None
        self.plugin_module_name = None
        self.config_section = []
        self.plugin_class = None

    def __repr__(self):
        s = ""
        s += "plugin_name = %s " % self.plugin_name
        s += "plugin_class_name = %s " % self.plugin_class_name
        s += "plugin_module_name = %s " % self.plugin_module_name
        s += "plugin_section = %s " % self.config_section
        s += "plugin_class = %s " % self.plugin_class
        return s

class PluginDispatcher(object):
    '''
    class to create a deliver, on request, the different plug-ins.
    Does not really implement any generic API, each plugin has different characteristics.
    It is just to take all the code out of the APFQueue class. 
    '''

    def __init__(self, apfqueue):

        self.log = logging.getLogger('main.plugindispatcher')

        self.apfqueue = apfqueue
        self.qcl = apfqueue.qcl
        self.fcl = apfqueue.fcl
        self.mcl = apfqueue.mcl
        
        self.apfqname = apfqueue.apfqname

        self.log.debug("Getting sched plugins")
        self.schedplugins = self.getschedplugins()
        self.log.debug("Got %d sched plugins" % len(self.schedplugins))
        self.log.debug("Getting batchstatus plugin")
        self.batchstatusplugin =  self.getbatchstatusplugin()
        self.log.debug("Getting batchstatus plugin")        
        self.wmsstatusplugin =  self.getwmsstatusplugin()
        self.log.debug("Getting submit plugin")
        self.submitplugin =  self.getsubmitplugin()
        self.log.debug("Getting monitor plugins")
        self.monitorplugins = self.getmonitorplugins()
        self.log.debug("Got %d monitor plugins" % len(self.monitorplugins))

        self.log.info('PluginDispatcher: Object initialized.')

    def getschedplugins(self):

        scheduler_plugin_handlers = self._getplugin('sched')  # list of PluginHandler objects
                                                      # Note that for the Sched category,
                                                      # we allow more than one plugin 
                                                      # (split by comma in the config file)
        scheduler_plugins = []
        for scheduler_ph in scheduler_plugin_handlers:
            scheduler_cls = scheduler_ph.plugin_class
            scheduler_plugin = scheduler_cls(self.apfqueue)  # calls __init__() to instantiate the class
            scheduler_plugins.append(scheduler_plugin)
        return scheduler_plugins

    def getbatchstatusplugin(self):

        condor_q_id = 'local'
        if self.qcl.generic_get(self.apfqname, 'batchstatusplugin') == 'Condor': 
            queryargs = self.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs')
            if queryargs:
                    condor_q_id = self.__queryargs2condorqid(queryargs)    
        batchstatus_plugin_handler = self._getplugin('batchstatus')[0]
        batchstatus_cls = batchstatus_plugin_handler.plugin_class

        # calls __init__() to instantiate the class
        # In this case the call accepts a second arguments:
        #    an ID used to allow the creation of more than one Singleton
        #    of this category. Remember the BatchStatusPlugin class is a Singleton. 
        #    Therefore, we can have more than one
        #    Batch Status Plugin objects, each one shared by a different
        #    bunch of APF Queues.
        batchstatus_plugin = batchstatus_cls(self.apfqueue, condor_q_id=condor_q_id)  

        # starts the thread
        batchstatus_plugin.start() 
        
        return batchstatus_plugin



    ###def getwmsstatusplugin(self):
    ###
    ###    wmsstatus_plugin_handler = self._getplugin('wmsstatus')[0]
    ###    wmsstatus_cls = wmsstatus_plugin_handler.plugin_class
    ###
    ###    # calls __init__() to instantiate the class
    ###    wmsstatus_plugin = wmsstatus_cls(self.apfqueue)  
    ###
    ###    # starts the thread
    ###    wmsstatus_plugin.start()   
    ###
    ###    return wmsstatus_plugin

    def getwmsstatusplugin(self):

        condor_q_id = 'local'
        if self.qcl.generic_get(self.apfqname, 'wmsstatusplugin') == 'Condor':
            queryargs = self.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queryargs')
            if queryargs:
                    condor_q_id = self.__queryargs2condorqid(queryargs)

        wmsstatus_plugin_handler = self._getplugin('wmsstatus')[0]
        wmsstatus_cls = wmsstatus_plugin_handler.plugin_class

        # calls __init__() to instantiate the class
        if self.qcl.generic_get(self.apfqname, 'wmsstatusplugin') == 'Condor':
            # In this case the call accepts a second arguments:
            # an ID used to allow the creation of more than one Singleton
            # of this category. Remember the WMSStatusPlugin class is a Singleton. 
            # Therefore, we can have more than one
            # WMS Status Plugin objects, each one shared by a different
            # bunch of APF Queues.
            wmsstatus_plugin = wmsstatus_cls(self.apfqueue, condor_q_id=condor_q_id)
        else:
            wmsstatus_plugin = wmsstatus_cls(self.apfqueue)


        # starts the thread
        wmsstatus_plugin.start()

        return wmsstatus_plugin



    def getsubmitplugin(self):
    
        batchsubmit_plugin_handler = self._getplugin('batchsubmit')[0]
        batchsubmit_cls = batchsubmit_plugin_handler.plugin_class
    
        # calls __init__() to instantiate the class
        batchsubmit_plugin = batchsubmit_cls(self.apfqueue)  
    
        return batchsubmit_plugin

    def getmonitorplugins(self):
        monitor_plugin_handlers = self._getplugin('monitor', self.apfqueue.mcl)  # list of classes 
        self.log.debug("monitor_plugin_handlers =   %s" % monitor_plugin_handlers)
        monitor_plugins = []
        for monitor_ph in monitor_plugin_handlers:
            try:
                monitor_cls = monitor_ph.plugin_class
                monitor_id = monitor_ph.config_section[1] # the name of the section in the monitor.conf
                monitor_plugin = monitor_cls(self.apfqueue, monitor_id=monitor_id)
                monitor_plugins.append(monitor_plugin)
            except Exception, e:
                self.log.error("Problem getting monitor plugin %s" % monitor_ph.plugin_name)
                self.log.debug("Exception: %s" % traceback.format_exc())
        return monitor_plugins


    def __queryargs2condorqid(self, queryargs):
        """
        method to get the name for the condor_q singleton,
        based on the combination of the values from 
        -name and -pool input options.
        The entire list of input options come from the queues conf file,
        and it is recorded in queryargs. 
        """
        l = queryargs.split()  # convert the string into a list
                               # e.g.  ['-name', 'foo', '-pool', 'bar'....]

        name = ''
        pool = ''
        
        if '-name' in l:
            name = l[l.index('-name') + 1]
        if '-pool' in l:
            pool = l[l.index('-pool') + 1]

        if name == '' and pool == '':
            return 'local'
        else:
            return '%s:%s' %(name, pool)


    def _getplugin(self, action, config=None):
        '''
        Generic private method to find out the specific plugin
        to be used for this queue, depending on the action.
        Action can be:
                - sched
                - batchstatus
                - wmsstatus
                - batchsubmit
                - monitor

        If passed, config is an Config object, as defined in autopyfactory.configloader

        Steps taken are:
           [a] config is None:
                This means the content of the variable <action>plugin 
                in self.qcl is directly the actual plugin
 
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

            [b] config is not None. 
                This means the content of variable <action>section 
                points to a section in config where the actual plugin can be found.
                Therefore, there is an extra step to read the value of <action>plugin
                from the config object.

        It has been added the option of getting more than one plugins 
        of the same category. 
        The value is comma-split, and one class is retrieve for each field. 
        Then, it will be up to the invoking entity to determine if only one item
        is expected, and therefore a [0] is needed, or a list of item is possible.

        Output is a list of 2-items tuples.
        First item is the name of the plugin.
        '''

        self.log.debug("Starting for action %s" %action)

        plugin_prefixes = {
                'sched' : 'Sched',
                'wmsstatus': 'WMSStatus',
                'batchstatus': 'BatchStatus',
                'batchsubmit': 'BatchSubmit',
                'monitor' : 'Monitor',
        }

        plugin_config_item = '%splugin' %action # i.e. schedplugin
        plugin_prefix = plugin_prefixes[action] 
        plugin_action = action
        
        # list of objects PluginHandler
        plugin_handlers = [] 

        # Get the list of plugin names
        if config:
            config_section_item = '%ssection' % action  # i.e. monitorsection
            if self.qcl.has_option(self.apfqname, config_section_item):
                plugin_names = []
                sections = self.qcl.get(self.apfqname, config_section_item)
                for section in sections.split(','):
                    section = section.strip()
                    plugin_name = config.get(section, plugin_config_item)  # i.e. APF (from monitor.conf)
                    plugin_names.append(plugin_name)

                    ph = PluginHandler()
                    ph.plugin_name = plugin_name 
                    ph.config_section = [self.apfqname, section]
                    plugin_handlers.append(ph)
            #else:
            #    return [PluginHandler()] # temporary solution  
        
        else:
            if self.qcl.has_option(self.apfqname, plugin_config_item):
                plugin_names = self.qcl.get(self.apfqname, plugin_config_item)  # i.e. Activated
                plugin_names = plugin_names.split(',') # we convert a string split by comma into a list
               
                for plugin_name in plugin_names: 
                    if plugin_name != "None":
                        plugin_name = plugin_name.strip()
                        ph = PluginHandler()
                        ph.plugin_name = plugin_name 
                        ph.config_section = [self.apfqname]
                        plugin_handlers.append(ph)
            
            #else:
            #    return [PluginHandler()] # temporary solution  


        for ph in plugin_handlers:

            name = ph.plugin_name 

            plugin_module_name = '%s%sPlugin' %(name, plugin_prefix)
            # Example of plugin_module_name is CondorGT2 + BatchSubmit + Plugin => CondorGT2BatchSubmitPlugin

            plugin_path = "autopyfactory.plugins.%s.%s" % ( plugin_action, plugin_module_name)
            self.log.debug("Attempting to import derived classnames: %s"
                % plugin_path)

            plugin_module = __import__(plugin_path,
                                       globals(),
                                       locals(),
                                       ["%s" % plugin_module_name])

            plugin_class_name = plugin_module_name  #  the name of the class is always the name of the module
            
            self.log.debug("Attempting to return plugin with classname %s" %plugin_class_name)

            plugin_class = getattr(plugin_module, plugin_class_name)  # with getattr() we extract the actual class from the module object

            ph.plugin_class_name = plugin_class_name 
            ph.plugin_module_name = plugin_module_name 
            ph.plugin_class = plugin_class

        return plugin_handlers


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


class CondorSingleton(type):
    '''
    -----------------------------------------------------------------------
    Ancillary class to be used as metaclass to make other classes Singleton.
    This particular implementation is for CondorBatchStatusPlugin.
    It allow to create different instances, one per schedd.
    Each instance is a singleton. 
    -----------------------------------------------------------------------
    '''
    def __init__(cls, name, bases, dct):
        cls.__instance = {} 
        type.__init__(cls, name, bases, dct)

    def __call__(cls, *args, **kw): 
        condor_q_id = kw.get('condor_q_id', 'local')
        if condor_q_id not in cls.__instance.keys():
            cls.__instance[condor_q_id] = type.__call__(cls, *args,**kw)
        return cls.__instance[condor_q_id]


def singletonfactory(id_var=None, id_default=None):
    '''
    This is an abstraction of the two previous classes. 
    We have here a metaclass factory, which will decide 
    which type of Singleton metaclass returns based on the inputs

    If id_var is not passed, then we asume a regular singleton __metaclass__ is expected.
    If id_var has a value, then it is a multi-singleton.
    We understand by multi-singleton a class that can instantiate the same object or not,
    depending on the value of id_var. Same value of id_var will generate the same object.
  
    id_var is the name of a key variable to be passed via __init__() when asking for a new object.
    The value of that variable will be the ID to determine if a real new object is needed or not.

    Note: when calling __init__(), the id_var has to be passed as a key=value variable,
    not just as a positional variable. 

    Examples:

        class A(object):
            __metaclass__ = singletonfactory()

        ---------------------------------------------------------------------

        class B(object):
            __metaclass__ = singletonfactory(id_var='condorpool', id_default='local')
        
        obj1 = B(..., condorpool='pool1', ...)
        obj2 = B(..., condorpool='pool1', ...)
        obj3 = B(..., condorpool='pool2', ...)

        obj1 and obj2 will be the same. obj3 will not. 
    '''

    class Singleton(type):

        # regular singleton __metaclass__
        if not id_var:

            def __init__(cls, name, bases, dct):
                cls.__instance = None 
                type.__init__(cls, name, bases, dct)
            def __call__(cls, *args, **kw):
                if cls.__instance is None:
                    cls.__instance = type.__call__(cls, *args,**kw)
                return cls.__instance

        # multi-singleton __metaclass__
        else:

            def __init__(cls, name, bases, dct):
                cls.__instance = {}
                type.__init__(cls, name, bases, dct)
            def __call__(cls, *args, **kw):
                id = kw.get(id_var, id_default)
                # note: we read the value of id_var from **kw
                #       so it has to be passed as a key=value variable,
                #       not as a positional variable. 
                if id not in cls.__instance.keys():
                    cls.__instance[id] = type.__call__(cls, *args,**kw)
                return cls.__instance[id]

    return Singleton

