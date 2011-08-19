#!  /usr/bin/env python
#
# Clean python factory logs
#
# $Id: cleanLogs.py 154 2010-03-19 13:02:16Z graemes $
#

import os, sys, getopt, logging, datetime, re, ConfigParser
import shutil

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
delete = 21
dryRun = False


#### try:
####     opts, args = getopt.getopt(sys.argv[1:], "", ["conf=", "delete=", "test", "dryrun"])
#### except getopt.GetoptError, errMsg:
####         mainMessages.error("Option parsing error (%s). Try '%s -h' for usage.", errMsg, sys.argv[0])
####         sys.exit(2)
#### try:
####         for opt, val in opts:
####                 if opt in ("--test", "--dryrun",):
####                         dryRun = True
####                         cyclesToDo = 1
####                 if opt in ("--delete",):
####                         delete = int(val)
####                 if opt in ("--conf",):
####                         conf = val
#### except ValueError, errMsg:
####             print >>sys.stderr, "Error converting '%s' to int: %s" % (val, errMsg)
####             sys.exit(1)
#### 

config = ConfigParser.SafeConfigParser()
config.optionxform = str
config.read(conf)

def getentries():
        '''
        get the list of subdirectories underneath 'baseLogDir'
        '''

        logDir = config.get('Pilots', 'baseLogDir')
        if not os.access(logDir, os.F_OK):
                    mainMessages.error('Base log directory %s does not exist - nothing to do',
                                       logDir)
                    sys.exit(1)
        entries = os.listdir(logDir)
        entries.sort()
        return entries

def process(delete):
        '''
        loops over all directories to perform cleaning actions
        '''
        entries = getentries()
        for entry in entries:
                process_entry(entry, delete)

def process_entry(entry, delete):
        ''' 
        processes each directory
        ''' 

        mainMessages.debug('Looking at %s' % entry)

        logDirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")  # i.e. 2011-08-12
        logDirMatch = logDirRe.match(entry)

        then = datetime.date(int(logDirMatch.group(1)), int(logDirMatch.group(2)), int(logDirMatch.group(3)))
        # then is the time of the directory, recreated from its name
        now = datetime.date.today()
        deltaT = now - then

        mainMessages.info('Entry %s is %d days old' % (entry, deltaT.days))

        if deltaT.days > delete:
                mainMessages.info("Deleting %s..." % entry)
                if dryRun:
                        mainMessages.info("Dry run - deletion supressed")
                else:
                        shutil.rmtree(logDir + '/' + entry)

# --------------------------------------------------------------------------------------------
process(delete)
