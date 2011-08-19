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
dryRun = False

config = ConfigParser.SafeConfigParser()
config.optionxform = str
config.read(conf)

class CleanCondorLogs(object):

        def __init__(self, fcl):
        
                self.fcl = fcl
                self.logDir = self.fcl.get('Pilots', 'baseLogDir')
                self.delete = self.__getdelete()

        def __getdelete(self):
                '''
                determines how old can logs be w/o being removed
                '''

                # default
                delete = 14

                if self.fcl.has_option('Pilots', 'delete'):  # FIXME: pick up a better name
                        delete = self.fcl.getint('Pilots','delete')

                return delete
                
        def process(self):
                '''
                loops over all directories to perform cleaning actions
                '''
                entries = self.__getentries()
                for entry in entries:
                        self.__process_entry(entry)

        def __getentries(self):
                '''
                get the list of subdirectories underneath 'baseLogDir'
                '''
                if not os.access(self.logDir, os.F_OK):
                            mainMessages.error('Base log directory %s does not exist - nothing to do',
                                               self.ogDir)
                            sys.exit(1)
                entries = os.listdir(self.logDir)
                entries.sort()
                return entries

        def __process_entry(self, entry):
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

                if deltaT.days > self.delete:
                        mainMessages.info("Deleting %s..." % entry)
                        if dryRun:
                                mainMessages.info("Dry run - deletion supressed")
                        else:
                                entrypath = os.path.join(self.logDir, entry)
                                shutil.rmtree(entrypath)

# --------------------------------------------------------------------------------------------

d = CleanCondorLogs()
d.process()
