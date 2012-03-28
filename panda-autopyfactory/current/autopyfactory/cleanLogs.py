#!  /usr/bin/env python
#
# Clean python factory logs
#
# $Id: cleanLogs.py 154 2010-03-19 13:02:16Z graemes $
#

import datetime
import logging
import os
import random
import re
import shutil
import threading
import time

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
    def __init__(self, apfqueue):
        '''
        factory is a reference to the Factory object that created
        the CleanCondorLogs instance
        '''

        self.apfqname = apfqueue.apfqname
        self.log = logging.getLogger('main.cleancondorlogs[%s]' %self.apfqname)
        self.log.debug('CleanCondorLogs: Initializing object...')
    
        self.fcl = apfqueue.fcl
        self.qcl = apfqueue.qcl
        self.logDir = self.fcl.get('Factory', 'baseLogDir')

        threading.Thread.__init__(self) # init the thread
        self.stopevent = threading.Event()

        self.log.info('CleanCondorLogs: Object initialized.')

    def run(self):
        '''
        Main loop
        '''

        self.log.debug('run: Starting.')

        while True:
            try:
                while not self.stopevent.isSet():
                    self.__wait_random()
                    self.__process()
                    self.__sleep()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
        
        self.log.debug('run: Leaving.')

    def __wait_random(self): 
        '''
        wait a random time to prevent all queues to start
        deleting at the same time. In particular, just after
        APF is turned on.
        '''
        # wait some random time
        randomsleep = int(random.uniform(0,30) * 60)         
        time.sleep(randomsleep)

    def __process(self):
        '''
        loops over all directories to perform cleaning actions
        '''

        self.log.debug("process: Starting.")
        
        entries = self.__getentries()
        for entry in entries:
            self.__process_entry(entry)
            
        self.log.info("cleanLogs: Processed %d directories." % len(entries))
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
        if not logDirMatch:
            # there is an entry robot.txt, which does not match the date format
            self.log.debug('__process_entry: ignoring entry %s' %entry)
            return 

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
            entrypath = os.path.join(self.logDir, entry, self.apfqname)
            # entrypath should look like  <logDir>/2011-08-12/BNL_ITB/
            if os.path.exists(entrypath):
                self.log.info("__process_entry: Deleting %s ..." % entrypath)
                shutil.rmtree(entrypath)

        # now, try to remove the parent directory        
        try:
            entrypath = os.path.join(self.logDir, entry)
            # entrypath should look like  <logDir>/2011-08-12/
            self.log.info("__process_entry: Trying to delete %s ..." % entrypath)
            os.rmdir(entrypath)     
        except:
            # it only works if the directoy is empty. 
            pass

        self.log.debug("__process_entry: Leaving.")

    def __getmaxdays(self):
        '''
        determines how old (in term of nb of days) 
        can logs be w/o being removed
        '''

        self.log.debug("__getmaxdays: Starting.")

        #maxdays = self.qcl.generic_get(self.apfqname, 'cleanlogs.maxdays', 'getint', default_value=7)
        maxdays = self.qcl.generic_get(self.apfqname, 'cleanlogs.keepdays', 'getint')

        self.log.debug("__getmaxdays: Leaving with output %s." %maxdays)
        return maxdays

    def __sleep(self):
        '''
        sleep for one day
        At some point, this should be read from config file
        '''
        # sleep 24 hours
        sleeptime = 24 * 60 * 60 
        time.sleep(sleeptime) 

           

