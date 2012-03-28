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
        

        for dir in DirMgr(self.logDir):
            self.__processdir(dir)
        self.log.info("__process: Processed %d directories." % len(dirs))
            
        self.log.debug("__process: Leaving.")


    def __processdir(self, dir):
        ''' 
        processes each directory.
        dir is a Dir object
        ''' 

        self.log.debug("__processdir: Starting with input %s." %dir)

        #dir.rm_subdirs(self.keepdays)

        self.keepdays = KeepDays(self.fcl, self.qcl)
        dir.rm(self.keepdays)

        self.log.debug("__processdir: Leaving.")


    def __sleep(self):
        '''
        sleep for one day
        At some point, this should be read from config file
        '''
        # sleep 24 hours
        sleeptime = 24 * 60 * 60 
        time.sleep(sleeptime) 


# =============================================================

class KeepDays(object):

    def __init__(self, fcl, qcl):
        self.fcl = fcl
        self.qcl = qcl
        self.__inspect()

    def __inspect(self):
        self.factory_keepdays = self.fcl.generic_get('factory', 'cleanlogs.keepdays', 'getint')
        self.queues_keepdays = {}
        for apfqname in self.qcl.sections():
            keepdays = self.qcl.generic_get(apfqname, 'cleanlogs.keepdays', 'getint')
            self.queues_keepdays[apfqname] = keepdays

    def get(self, apfqname):
        return self.queues_keepdays.get(apfqname, self.factory_keepdays) 
  

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
        for d in os.listdir(self.basedir):
            dir_obj = Dir(self.basedir, d)
            if dir_obj:
                dirs.append(dir_obj)

        return dirs
                
           
class Dir(object):
    '''
    class to manage each parent directory.
    The parent directory looks like <logDir>/2011-08-12/ 
    '''

    def __new__(cls, basedir, dir):
        dirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")
        if dirRe.match(dir):
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
        self.delta_t = self.delta_t() 

    def empty(self):
        return os.listdir(self.path) == []

    def creation_t(self):
        '''
        returns a datetime object with the creation time.
        Creation time is calculated from the self.dir itself.
        '''
        fields = self.dir.split('-')
        creation_t = datetime.date(int(fields[0]),
                                   int(fields[1]),
                                   int(fields[2]))
        return creation_t


    def delta_t(self):
        current_t = datetime.date.today()
        return current_t - self.creation_t 

    def subdirs(self):
        ''' 
        returns the list of subdirs 
        ''' 
        subdirs = []
        for subdir in os.listdir(self.path):
            subdirs.append(SubDir(self, subdir))
        return subdirs 

    def rm(self, keepdays):

        self.rm_subdirs(keepdays)

        if self.empty(): 
            os.rmdir(self.path)     

    def rm_subdirs(self, keepdays):
        '''
        tries to remove as many subdirs as possible
        keepdays is a KeepDays object
        '''
        
        for subdir in self.subdirs():
            subdir.rm(keepdays)


class SubDir(object):
    '''
    class to handle each subdirectory.
    Subdirs look like <logDir>/2011-08-11/ANALY_BNL/
    '''
    def __init__(self, parent, subdir):
        '''
        parent is a Dir object
        subdir is the APFQname
        '''
        self.parent = parent
        self.subdir = subdir
        self.path = os.path.join(parent.path, subdir)

    def rm(self, keepdays):

        delta_t = self.parent.delta_t.days
        if delta_t > keepdays.get(self.subdir):
            if os.path.exists(self.path):
                #self.log.info("__delsubdir: Deleting %s ..." % path)
                shutil.rmtree(self.path)
