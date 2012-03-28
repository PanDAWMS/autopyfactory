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

class CleanLogs(threading.Thread):
    '''
    -----------------------------------------------------------------------
    Class to handle the log files removal.
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
    def __init__(self, factory):
        '''
        factory is a reference to the Factory object that created
        the CleanLogs instance
        '''

        self.log = logging.getLogger('main.cleanlogs')
        self.log.debug('CleanLogs: Initializing object...')
    
        self.factory = factory
        self.fcl = factory.fcl
        self.qcl = factory.qcl
        self.logDir = self.fcl.get('Factory', 'baseLogDir')

        threading.Thread.__init__(self) # init the thread
        self.stopevent = threading.Event()

        self.log.info('CleanLogs: Object initialized.')

    def run(self):
        '''
        Main loop
        '''
        self.log.debug('run: Starting.')

        while True:
            try:
                while not self.stopevent.isSet():
                    self.__getkeepdays()
                    self.__process()
                    self.__sleep()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
        
        self.log.debug('run: Leaving.')

    def __process(self):
        '''
        loops over all directories to perform cleaning actions
        '''

        self.log.debug("__process: Starting.")
        
        dirs = self.__getdirs()
        for dir in dirs:
            self.__processdir(dir)
        self.log.info("__process: Processed %d directories." % len(dirs))
            
        self.log.debug("__process: Leaving.")

    def __getdirs(self):
        '''
        get the list of subdirectories underneath 'baseLogDir'
        Each dir looks like <logDir>/2011-08-12/
        '''

        self.log.debug("__getdirs: Starting.")

        if not os.access(self.logDir, os.F_OK):
            self.log.warning('__getdirs: Base log directory %s does not exist - nothing to do',
                      self.logDir)
            self.log.warning("__getdirs: Leaving with no output.") 
            return []
        # else (==the base directory exists)
        dirs = os.listdir(self.logDir)
        # we only consider dirs looking like <logDir>/2011-08-12/
        #   -- we need to insert <logDir> at the beginning
        #   -- there is a file called robot.txt which does not match
        logDirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")
        dirs = [os.path.join(self.logDir, d) for d in dirs if logDirRe.match(d)]

        self.log.debug("__getdirs: Leaving with output %s." %entries) 
        return entries

    def __processdir(self, dir):
        ''' 
        processes each directory.
        Directories look like <logDir>/2011-08-12/ 
        ''' 

        self.log.debug("__processdir: Starting with input %s." %dir)
        self.__delsubdirs(dir)
        self.__deldir(dir)
        self.log.debug("__processdir: Leaving.")

    def __delsubdirs(self, dir):
        '''
        tries to delete all subdirectories
        dir looks like <logDir>/2011-08-12
        '''

        self.log.debug('__delsubdirs: Starting for dir=%s.' %dir)

        delta_t = self.__delta_t(dir)
        subdirs = os.listdir(dir)
        for subdir in subdirs:
            self.__delsubdir(dir, subdir, delta_t)

        self.log.debug('__delsubdir: Leaving.')

    def __delsubdir(self, dir, subdir, delta_t):
        '''
        tries to delete each subdirectory
        dir looks like <logDir>/2011-08-12
        '''

        self.log.debug('__delsubdir: Starting with inputs dir=%s, subdir=%s, delta_t=%s' %(dir, subdir, delta_t))

        keep_days = self.queues_keepdays.get(subdir, self.factory_keepdays) 
        if delta_t.days > keep_days:
            path = os.join(dir, subdir)
            self.log.info("__delsubdir: Entry %s is %d days old" % (path, delta_t.days))
            if os.path.exists(path):
                self.log.info("__delsubdir: Deleting %s ..." % path)
                shutil.rmtree(path)

        self.log.debug('__delsubdir: Leaving.')

    def __delta_t(self, dir):
        '''
        returns how long since the directory was created.
        It gets the creation time from the path itself.
        Each directory looks like <logDir>/2011-08-12/
        logDirMatch search for patterns like 2011-08-12
        '''

        self.log.debug("__delta_t: Starting for dir %s" %dir)

        logDirRe = re.compile(r".*/(\d{4})-(\d{2})-(\d{2})?$")
        logDirMatch = logDirRe.match(dir)
        creation_t = datetime.date(int(logDirMatch.group(1)), 
                                   int(logDirMatch.group(2)), 
                                   int(logDirMatch.group(3)))
        current_t = datetime.date.today()
        delta_t = current_t - creation_t 
        self.log.debug("__delta_t: Leaving with delta_t = %s" %delta_t)
        return delta_t

    def __deldir(self, dir):
        '''
        try to remove the directory dir
        dir should look like  <logDir>/2011-08-12/
        '''

        self.log.debug('__deldir: Starting with dir=%s.' %dir)
        if os.listdir(dir) == []:
            # the dir is empty 
            self.log.info("__deldir: dir %s is empty. We can delete it" % dir)
            os.rmdir(dir)     
        self.log.debug('__deldir: Leaving.')

    def __getkeepdays(self):
        '''
        determines how old (in term of nb of days) 
        can logs be w/o being removed
        '''

        self.log.debug("__getkeepdays: Starting.")

        self.factory_keepdays = self.fcl.generic_get('factory', 'cleanlogs.keepdays', 'getint')
        self.queues_keepdays = {}
        for apfqname in self.qcl.sections():
            keepdays = self.qcl.generic_get(apfqname, 'cleanlogs.keepdays', 'getint')
            self.queues_keepdays[apfqname] = keepdays

        self.log.debug("__getkeepdays: Leaving ")

    def __sleep(self):
        '''
        sleep for one day
        At some point, this should be read from config file
        '''
        # sleep 24 hours
        sleeptime = 24 * 60 * 60 
        time.sleep(sleeptime) 


# =============================================================


class DirMgr(object):
    '''
    class to create a list of Dir objects
    '''
    def __init__(self, basedir):
        self.basedir = basedir
        self.dirs = self.getdirs() 

    def getdirs(self):
 
        if not os.access(self.basedir, os.F_OK):
            #self.log.warning('__getdirs: Base log directory %s does not exist - nothing to do',
            #          self.basedir)
            #self.log.warning("__getdirs: Leaving with no output.") 
            return []
        # else (==the base directory exists)
        dirs = []
        for d in os.listdir(self.basedir)
            dir_obj = Dir(self.basedir, d)
            if dir_obj:
                dirs.append(dir_obj)

        return dirs
                
           
class Dir(object):
    '''
    class to manage each parent directory.
    The parent directory looks like <logDir>/2011-08-12/ 
    '''
    dirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")

    def __new__(self, basedir, dir):
        if Dir.dirRe.match(dir):
            return super(Dir, cls).__new__(cls) 

    def __init__(self, basedir, dir):
        '''
        basedir is <logDir>
        dir is like 2011-08-12
        '''

        self.basedir = basedir
        self.dir = dir
        self.path = os.path.join(basedir, dir)
        self.creation_t = self.creation_t() 

    def empty(self):
        '''
        checks if the parent directory is empty
        '''
        return os.listdir(self.path) == []

    def creation_t(self):
        '''
        returns a datetime object with the creation time.
        Creation time is calculated from the self.dir itself.
        '''
        match = Dir.dirRe.match(self.dir)
        creation_t = datetime.date(int(match.group(1)), 
                                   int(match.group(2)), 
                                   int(match.group(3)))
        return creation_t

    def subdirs(self):
        ''' 
        returns the list of subdirs 
        ''' 
        return os.listdir(path)

    def del(self):
        '''
        '''
        if self.empty(): 
            os.rmdir(self.path)     


