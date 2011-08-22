#!  /usr/bin/env python
#
# Clean python factory logs
#
# $Id: cleanLogs.py 154 2010-03-19 13:02:16Z graemes $
#

import datetime
import logging
import os
import re
import shutil
import sys

import ConfigParser

###### # Global logging to console
###### global console
###### console = logging.StreamHandler()
###### console.setLevel(logging.DEBUG)
###### formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
###### console.setFormatter(formatter)
###### 
###### mainMessages = logging.getLogger('cleanLogs.py')
###### mainMessages.addHandler(console)
###### mainMessages.setLevel(logging.INFO)
###### 
###### # You'll never see this message, ha ha
###### mainMessages.debug('Logger initialised')
###### 
###### config = ConfigParser.SafeConfigParser()
###### config.optionxform = str
###### config.read(conf)

class CleanCondorLogs(object):
        '''
        -----------------------------------------------------------------------
        Class to handle the condor log files removal.
        There are several possibilities to decide which files 
        have to be deleted is:
                - basic algorithm is just to remove files older than some 
                  number of days.
                - based on disk space usage. 
                  We can keep files as long as possible, until some percentage
                  of the disk is used. 
                - Both algorithm can be combined. 
        -----------------------------------------------------------------------
        Public Interface:
                __init__(fcl)
                process()
        -----------------------------------------------------------------------
        '''

        ##def __init__(self, fcl):
        ##
        ##        self.fcl = fcl
        ##        self.logDir = self.fcl.get('Pilots', 'baseLogDir')
        ##        self.delete = self.__getdelete()

        def __init__(self, factory):
                '''
                factory is a reference to the Factory object that created
                the CleanCondorLogs instance
                '''
        
                self.fcl = factory.fcl
                self.logDir = self.fcl.get('Pilots', 'baseLogDir')

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
                #### if not os.access(self.logDir, os.F_OK):
                ####             mainMessages.error('Base log directory %s does not exist - nothing to do',
                ####                                self.ogDir)
                ####             sys.exit(1)
                entries = os.listdir(self.logDir)
                entries.sort()
                return entries

        def __process_entry(self, entry):
                ''' 
                processes each directory
                ''' 

                #### mainMessages.debug('Looking at %s' % entry)

                logDirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")  # i.e. 2011-08-12
                logDirMatch = logDirRe.match(entry)

                then = datetime.date(int(logDirMatch.group(1)), 
                                     int(logDirMatch.group(2)), 
                                     int(logDirMatch.group(3)))
                # then is the time of the directory, recreated from its name
                now = datetime.date.today()
                deltaT = now - then

                #### mainMessages.info('Entry %s is %d days old' % (entry, deltaT.days))

                # how many days before we delete?
                maxdays = self.__getmaxdays() 

                if deltaT.days > maxdays:
                        ##### mainMessages.info("Deleting %s..." % entry)
                        entrypath = os.path.join(self.logDir, entry)
                        shutil.rmtree(entrypath)

        def __getmaxdays(self):
                '''
                determines how old (in term of nb of days) 
                can logs be w/o being removed
                '''

                # default
                maxdays = 14

                if self.fcl.has_option('Pilots', 'maxdays'):  # FIXME: pick up a better name
                        maxdays = self.fcl.getint('Pilots', 'maxdays')

                return maxdays
