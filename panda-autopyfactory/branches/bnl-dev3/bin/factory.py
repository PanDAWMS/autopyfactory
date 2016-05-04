#! /usr/bin/env python
#
# Simple(ish) python condor_g factory for panda pilots
#
# $Id$
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


from optparse import OptionParser
import logging
import logging.handlers
import time
import os
import sys
import traceback
import pwd
import grp


# Need to set PANDA_URL_MAP before the Client module is loaded (which happens
# when the Factory module is loaded). Unfortunately this means that logging
# is not yet available.
if not 'APF_NOSQUID' in os.environ:
    if not 'PANDA_URL_MAP' in os.environ:
        os.environ['PANDA_URL_MAP'] = 'CERN,http://pandaserver.cern.ch:25085/server/panda,https://pandaserver.cern.ch:25443/server/panda'
        print >>sys.stderr,  'FACTORY DEBUG: Set PANDA_URL_MAP to %s' % os.environ['PANDA_URL_MAP']  
    else:
        print >>sys.stderr, 'FACTORY DEBUG: Found PANDA_URL_MAP set to %s. Not changed.' % os.environ['PANDA_URL_MAP']
    if not 'PANDA_URL' in os.environ:
        os.environ['PANDA_URL'] = 'http://pandaserver.cern.ch:25085/server/panda'
        print >>sys.stderr, 'FACTORY DEBUG: Set PANDA_URL to %s' % os.environ['PANDA_URL']
    else:
        print >>sys.stderr, 'FACTORY DEBUG: Found PANDA_URL set to %s. Not changed.' % os.environ['PANDA_URL']
else:
    print >>sys.stderr, 'FACTORY DEBUG: Found APF_NOSQUID set. Not changing/setting panda client environment.'


from autopyfactory.factory import Factory
from autopyfactory.configloader import FactoryConfigLoader
from autopyfactory.exceptions import FactoryConfigurationFailure

def main():
    parser = OptionParser(usage='''%prog [OPTIONS]

  autopyfactory is an ATLAS pilot factory.

  This program is licenced under the GPL, as set out in LICENSE file.

  Author(s):
    Graeme A Stewart <g.stewart@physics.gla.ac.uk>
    Peter Love <p.love@lancaster.ac.uk>
    John Hover <jhover@bnl.gov>
 ''', version="%prog $Id$")

    parser.add_option("--verbose", "--debug", dest="logLevel", default=logging.INFO,
                      action="store_const", const=logging.DEBUG, help="Set logging level to DEBUG [default INFO]")
    parser.add_option("--quiet", dest="logLevel",
                      action="store_const", const=logging.WARNING, help="Set logging level to WARNING [default INFO]")
    parser.add_option("--test", "--dry-run", dest="dryRun", default=False,
                      action="store_true", help="Dry run - supress job submission")
    parser.add_option("--oneshot", "--one-shot", dest="cyclesToDo", default=0,
                      action="store_const", const=1, help="Run one cycle only")
    parser.add_option("--cycles", dest="cyclesToDo",
                      action="store", type="int", metavar="CYCLES", help="Run CYCLES times, then exit [default infinite]")
    parser.add_option("--sleep", dest="sleepTime", default=120,
                      action="store", type="int", metavar="TIME", help="Sleep TIME seconds between cycles [default %default]")
    parser.add_option("--conf", dest="confFiles", default="/etc/apf/factory.conf",
                      action="store", metavar="FILE1[,FILE2,FILE3]", help="Load configuration from FILEs (comma separated list)")
    parser.add_option("--log", dest="logfile", default="syslog", metavar="LOGFILE", action="store", 
                      help="Send logging output to LOGFILE or SYSLOG or stdout [default <syslog>]")
    parser.add_option("--runas", dest="runAs", default="apf",
                      action="store", metavar="USERNAME", help="If run as root, drop privileges to USER")
    (options, args) = parser.parse_args()

    options.confFiles = options.confFiles.split(',')
    
    # Setup logging
    log = logging.getLogger('main')
    if options.logfile == "stdout":
        logStream = logging.StreamHandler()
    elif options.logfile == 'syslog':
        logStream = logging.handlers.SysLogHandler('/dev/log')
    else:
        logStream = logging.handlers.RotatingFileHandler(filename=options.logfile, maxBytes=10000000, backupCount=5)    

    formatter = logging.Formatter('%(asctime)s - %(name)s: %(levelname)s %(message)s')
    logStream.setFormatter(formatter)
    log.addHandler(logStream)
    log.setLevel(options.logLevel)
    log.debug('logging initialised')
    
    # If running as root, drop privileges to --runas' account.
    starting_uid = os.getuid()
    starting_gid = os.getgid()
    starting_uid_name = pwd.getpwuid(starting_uid)[0]
    
    if os.getuid() !=0:
        log.info("Already running as unprivileged user %s" % starting_uid_name)
        
    if os.getuid() == 0:
        try:
            runuid = pwd.getpwnam(options.runAs).pw_uid
            rungid = pwd.getpwnam(options.runAs).pw_gid
            os.chown(options.logfile, runuid, rungid)
            
            os.setgid(rungid)
            os.setuid(runuid)
            log.info("Now running as user %d:%d ..." % (runuid, rungid))
        
        except KeyError, e:
            log.error('No such user %s, unable run properly. Error: %s' % (options.runAs, e))
            sys.exit(1)
            
        except OSError, e:
            log.error('Could not set user or group id to %s:%s. Error: %s' % (runuid, rungid, e))
            sys.exit(1)
    
    # Create config, add in options...
    if options.confFiles != None:
        fc = FactoryConfigLoader(options.confFiles)
    
    fc.config.set("Factory","dryRun", str(options.dryRun))
    fc.config.set("Factory","cyclesToDo", str(options.cyclesToDo))
    fc.config.set("Factory", "sleepTime", str(options.sleepTime))
    fc.config.set("Factory", "confFiles", ','.join(options.confFiles))
    
        
    # Create Factory and enter main loop
    try:
        log.info('Creating Factory and entering main loop...')
        f = Factory(fc)
        f.mainLoop()
    except KeyboardInterrupt:
        log.info('Caught keyboard interrupt - exiting')
    except FactoryConfigurationFailure, errMsg:
        log.error('Factory configuration failure: %s', errMsg)
    except ImportError, errorMsg:
        log.error('Failed to import necessary python module: %s' % errorMsg)
    except:
        # TODO - make this a logger.exception() call
        log.error('''Unexpected exception! There was an exception
  raised which the factory was not expecting and did not know how to
  handle. You may have discovered a new bug or an unforseen error
  condition. Please report this exception to Graeme
  <g.stewart@physics.gla.ac.uk>. The factory will now re-raise this
  exception so that the python stack trace is printed, which will allow
  it to be debugged - please send output from this message
  onwards. Exploding in 5...4...3...2...1... Have a nice day!''')
        # The following line prints the exception to the logging module
        log.error(traceback.format_exc(None))
        raise


if __name__ == "__main__":
    main()
