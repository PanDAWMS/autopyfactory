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


import grp
import logging
import logging.handlers
import os
import pwd
import sys
import time
import traceback

from optparse import OptionParser

from autopyfactory.configloader import FactoryConfigLoader
from autopyfactory.apfexceptions import FactoryConfigurationFailure


class APF(object):
        """class to parse the input options,
        setup everything, and run Factory
        """

        def __init__(self):
                self.options = None 
                self.args = None
                self.log = None
                self.fc = None
        
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
                                  default="apf",
                                  action="store", 
                                  metavar="USERNAME", 
                                  help="If run as root, drop privileges to USER")
                (self.options, self.args) = parser.parse_args()

                self.options.confFiles = self.options.confFiles.split(',')
   
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
                    logStream = logging.SysLogHandler('/dev/log')
                else:
                    lf = self.options.logfile
                    logdir = os.path.dirname(lf)
                    if not os.path.exists(logdir):
                        os.makedirs(logdir)
                    runuid = pwd.getpwnam(self.options.runAs).pw_uid
                    rungid = pwd.getpwnam(self.options.runAs).pw_gid                  
                    os.chown(logdir, runuid, rungid)
                    logStream = logging.FileHandler(filename=lf)    

                formatter = logging.Formatter('%(asctime)s - %(name)s: %(levelname)s: %(module)s: %(message)s')
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
                seting up some panda variables.
                '''

                if not 'APF_NOSQUID' in os.environ:
                    if not 'PANDA_URL_MAP' in os.environ:
                        os.environ['PANDA_URL_MAP'] = 'CERN,http://pandaserver.cern.ch:25085/server/panda,https://pandaserver.cern.ch:25443/server/panda'
                        self.log.warning('FACTORY DEBUG: Set PANDA_URL_MAP to %s' % os.environ['PANDA_URL_MAP'])
                    else:
                        self.log.warning('FACTORY DEBUG: Found PANDA_URL_MAP set to %s. Not changed.' % os.environ['PANDA_URL_MAP'])
                    if not 'PANDA_URL' in os.environ:
                        os.environ['PANDA_URL'] = 'http://pandaserver.cern.ch:25085/server/panda'
                        self.log.warning('FACTORY DEBUG: Set PANDA_URL to %s' % os.environ['PANDA_URL'])
                    else:
                        self.log.warning('FACTORY DEBUG: Found PANDA_URL set to %s. Not changed.' % os.environ['PANDA_URL'])
                else:
                    self.log.warning('FACTORY DEBUG: Found APF_NOSQUID set. Not changing/setting panda client environment.')


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
                    self.fc = FactoryConfigLoader(self.options.confFiles)
                
                self.fc.config.set("Factory","cyclesToDo", str(self.options.cyclesToDo))
                self.fc.config.set("Factory", "sleepTime", str(self.options.sleepTime))
                self.fc.config.set("Factory", "confFiles", ','.join(self.options.confFiles))
               
        def mainloop(self):
                """Create Factory and enter main loop
                """

                from autopyfactory.factory import Factory

                try:
                    self.log.info('Creating Factory and entering main loop...')

                    f = Factory(self.fc)
                    f.mainLoop()
                except KeyboardInterrupt:
                    self.log.info('Caught keyboard interrupt - exiting')
                except FactoryConfigurationFailure, errMsg:
                    self.log.error('Factory configuration failure: %s', errMsg)
                except ImportError, errorMsg:
                    self.log.error('Failed to import necessary python module: %s' % errorMsg)
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

 

# =============================================================================
#                       M A I N
# =============================================================================


def main():


        apf = APF()
        apf.parseopts()
        apf.setuplogging()
        apf.setuppandaenv()
        apf.checkroot()
        apf.createconfig()
        apf.mainloop()
        

if __name__ == "__main__":
    main()
