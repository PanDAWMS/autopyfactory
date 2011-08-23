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
import threading

class CleanCondorLogs(threading.Thread):
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
                the interface inherited from Thread `
        -----------------------------------------------------------------------
        '''
        def __init__(self, wmsqueue):
                '''
                factory is a reference to the Factory object that created
                the CleanCondorLogs instance
                '''

                self.siteid = wmsqueue.siteid
                self.log = logging.getLogger('main.cleancondorlogs[%s]' %self.siteid)
                self.log.info('CleanCondorLogs: Initializing object...')
        
                self.fcl = wmsqueue.fcl
                self.logDir = self.fcl.get('Pilots', 'baseLogDir')

                threading.Thread.__init__(self) # init the thread
                self.stopevent = threading.Event()

                self.log.info('CleanCondorLogs: Object initialized.')

        def run(self):
                '''
                Main loop
                '''

                self.log.debug('run: Starting.')

                while not self.stopevent.isSet():
                        self.__process()
                        self.__sleep()

                self.log.debug('run: Leaving.')

        def ___process(self):
                '''
                loops over all directories to perform cleaning actions
                '''

                self.log.debug("process: Starting.")
                
                entries = self.__getentries()
                for entry in entries:
                        self.__process_entry(entry)

                self.log.debug("process: Leaving.")

        def __getentries(self):
                '''
                get the list of subdirectories underneath 'baseLogDir'
                '''

                self.log.debug("__getentries: Starting.")

                if not os.access(self.logDir, os.F_OK):
                        self.log.warning('__getentries: Base log directory %s does not exist - nothing to do',
                                          self.logDir)
                        self.log.warning("__getentries: Leaving with no output.") 
                        return []
       
                # if the base directory exists...  

                entries = os.listdir(self.logDir)
                # sort directories by name (== by creation date)
                entries.sort()

                self.log.debug("__getentries: Leaving with output %s." %entries) 
                return entries

        def __process_entry(self, entry):
                ''' 
                processes each directory
                ''' 

                self.log.debug("__process_entry: Starting with input %s." %entry)

                logDirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")  # i.e. 2011-08-12
                logDirMatch = logDirRe.match(entry)

                then = datetime.date(int(logDirMatch.group(1)), 
                                     int(logDirMatch.group(2)), 
                                     int(logDirMatch.group(3)))
                # then is the time of the directory, recreated from its name
                now = datetime.date.today()
                deltaT = now - then

                # how many days before we delete?
                maxdays = self.__getmaxdays() 

                if deltaT.days > maxdays:
                        self.log.info("__process_entry: Entry %s is %d days old" % (entry, deltaT.days))
                        entrypath = os.path.join(self.logDir, entry, self.siteid)
                        # entrypath should look like  <logDir>/2011-08-12/BNL_ITB/
                        if os.path.exists(entrypath):
                                self.log.info("__process_entry: Deleting %s..." % entrypath)
                                shutil.rmtree(entrypath)

                self.log.debug("__process_entry: Leaving.")

        def __getmaxdays(self):
                '''
                determines how old (in term of nb of days) 
                can logs be w/o being removed
                '''

                self.log.debug("__getmaxdays: Starting.")

                # default
                maxdays = 14

                if self.fcl.has_option('Pilots', 'maxdays'):
                        maxdays = self.fcl.getint('Pilots', 'maxdays')

                self.log.debug("__getmaxdays: Leaving with output %s." %maxdays)
                return maxdays
