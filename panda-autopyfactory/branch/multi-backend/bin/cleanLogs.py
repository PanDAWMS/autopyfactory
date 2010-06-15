#!  /usr/bin/env python
#
# Clean python factory logs
#
# $Id: cleanLogs.py 154 2010-03-19 13:02:16Z graemes $
#

import os, os.path, sys, getopt, logging, commands, datetime, re, ConfigParser

# Global logging to console
global console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)

mainMessages = logging.getLogger('cleanLogs.py')
mainMessages.addHandler(console)
mainMessages.setLevel(logging.INFO)

# You'll never see this message, ha ha
mainMessages.debug('Logger initialised')

# Defaults
conf = 'factory.conf'
compress = 7
delete = 21
dryRun = False


try:
    opts, args = getopt.getopt(sys.argv[1:], "h", \
                                   ["help", "quiet", "verbose", "conf=",
                                    "compress=", "delete=", "test", "dryrun"])
except getopt.GetoptError, errMsg:
    mainMessages.error("Option parsing error (%s). Try '%s -h' for usage.", errMsg, sys.argv[0])
    sys.exit(2)

try:
    for opt, val in opts:
        if opt in ("-h", "--help"):
            print '''cleanLogs.py OPTIONS

  Basic clean-up script for factory log files.

  Simple pilot factory for atlas production pilots.
  Defaut configuration file is factory.conf.

  Options:
    --help: Print this message.
    --quiet : Silent running.
    --verbose: Tell me all about it.
    --test, --dryrun: Don't do anything, just say what you would do.
    --compress=N: Number of days after which logfile directories get
                  compressed (default 7).
    --delete=N: Number of days after which logfile directories get
                deleted (default 21).
    --conf=FILE: Read configuration file FILE (default 'factory.conf')

  Author:
    Graeme A Stewart <g.stewart@physics.gla.ac.uk>
'''
            sys.exit(0)
        if opt in ("--quiet",):
            debugLevel = logging.WARNING
            mainMessages.setLevel(debugLevel)
        if opt in ("--verbose", "--debug"):
            debugLevel = logging.DEBUG
            mainMessages.setLevel(debugLevel)
        if opt in ("--test", "--dryrun",):
            dryRun = True
            cyclesToDo = 1
        if opt in ("--compress",):
            compress = int(val)
        if opt in ("--delete",):
            delete = int(val)
        if opt in ("--conf",):
            conf = val
except ValueError, errMsg:
    print >>sys.stderr, "Error converting '%s' to int: %s" % (val, errMsg)
    sys.exit(1)


config = ConfigParser.SafeConfigParser()
config.optionxform = str
config.read(conf)

logDir = config.get('Pilots', 'baseLogDir')
if not os.access(logDir, os.F_OK):
    mainMessages.error('Base log directory %s does not exist - nothing to do',
                       logDir)
    sys.exit(1)
 

logDirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})(\.tgz)?$")
now = datetime.date.today()

entries = os.listdir(logDir)
entries.sort()
for entry in entries:
    mainMessages.debug('Looking at %s' % entry)
    logDirMatch = logDirRe.match(entry)
    if logDirMatch:
        mainMessages.debug('This is a factory log')
        then = datetime.date(int(logDirMatch.group(1)), int(logDirMatch.group(2)), int(logDirMatch.group(3)))
        deltaT = now - then
        mainMessages.info('Entry %s is %d days old' % (entry, deltaT.days))
        if logDirMatch.group(4) != None and deltaT.days > delete:
            mainMessages.info("Deleting compressed %s..." % entry)
            if dryRun:
                mainMessages.info("Dry run - deletion supressed")
            else:
                commands.getstatusoutput('rm -fr %s' % (logDir + "/" + entry))
        elif logDirMatch.group(4) == None and deltaT.days > compress:
            mainMessages.info("Compressing %s..." % entry)
            if dryRun:
                mainMessages.info("Dry run - compression supressed")
            else:
                commands.getstatusoutput('tar -czf %s %s' % (logDir + "/" + entry + ".tgz", logDir + "/" + entry))
                commands.getstatusoutput('rm -fr %s' % (logDir + "/" + entry))
        else:
            if logDirMatch.group(4) == None:
                mainMessages.info("%s is fresh - doing nothing" % entry)
            else:
                mainMessages.info("%s is an archived log - doing nothing" % entry)
    else:
        mainMessages.debug('This does not look like a factory log')

