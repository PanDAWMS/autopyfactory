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

from autopyfactory.interfaces import _thread

class CleanLogs(_thread):
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

        _thread.__init__(self)

        self.log = logging.getLogger('main.cleanlogs')
        self.log.trace('CleanLogs: Initializing object...')
    
        self.factory = factory
        self.fcl = factory.fcl
        self.qcl = factory.qcl
        self.logDir = self.fcl.get('Factory', 'baseLogDir')

        self.log.trace('CleanLogs: Object initialized.')


    def _time_between_loops(self):
        '''
        sleep for one day
        At some point, this should be read from config file
        '''
        # sleep 24 hours
        sleeptime = 24 * 60 * 60 
        return sleeptime


    def _run(self):
        '''
        Main loop
        '''
        self.log.trace('Starting.')
        self.__process()
        self.log.trace('Leaving.')


    def __process(self):
        '''
        loops over all directories to perform cleaning actions
        '''

        self.log.trace("Starting.")

        dirmgr = DirMgr(self.logDir)
        dirs = dirmgr.dirs
        for dir in dirs:
            self.__processdir(dir)
        self.log.debug("Processed %d directories." % len(dirs))
            
        self.log.trace("Leaving.")


    def __processdir(self, dir):
        ''' 
        processes each directory.
        dir is a Dir object
        ''' 

        self.log.trace("Starting with input %s." %dir.dir)

        self.keepdays = KeepDays(self.fcl, self.factory.qcl)
        dir.rm(self.keepdays)

        self.log.trace("Leaving.")




# =============================================================

class KeepDays(object):

    def __init__(self, fcl, qcl):

        self.log = logging.getLogger('main.keepdays')

        self.fcl = fcl
        self.qcl = qcl
        self.__inspect()
        
        self.log.trace('KeepDays: Object initialized.')

    def __inspect(self):

        self.log.trace('Starting.')

        self.factory_keepdays = self.fcl.generic_get('Factory', 'cleanlogs.keepdays', 'getint')
        self.queues_keepdays = {}
        for apfqname in self.qcl.sections():
            keepdays = self.qcl.generic_get(apfqname, 'cleanlogs.keepdays', 'getint')
            self.queues_keepdays[apfqname] = keepdays

        self.log.trace('Leaving.')

    def get(self, apfqname):
        return self.queues_keepdays.get(apfqname, self.factory_keepdays) 
  

class DirMgr(object):
    '''
    class to create a list of Dir objects
    '''
    def __init__(self, basedir):

        self.log = logging.getLogger('main.DirMgr')

        self.basedir = basedir
        self.dirs = self.getdirs() 

        self.log.trace('DirMgr: Object initialized.')

    def getdirs(self):
 
        if not os.access(self.basedir, os.F_OK):
            self.log.warning('Base log directory %s does not exist - nothing to do',
                      self.basedir)
            self.log.warning("Leaving with no output.") 
            return []
        # else (==the base directory exists)
        dirs = []
        for d in os.listdir(self.basedir):
            dir_obj = Dir(self.basedir, d)
            if dir_obj:
                dirs.append(dir_obj)

        self.log.trace('Leaving return %s dirs' %len(dirs))
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

        self.log = logging.getLogger('main.dir')

        self.basedir = basedir
        self.dir = dir
        self.path = os.path.join(basedir, dir)
        self.creation_t = self.creation_t() 
        self.delta_t = self.delta_t() 

        self.log.trace('Dir: Object initialized for dir %s.' %self.dir)

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
        '''
        tries to delete the entire subtree.
        First orders each subdir to delete itself.
        After that, if Dir is empty, 
        it deletes itself.
        '''

        self.log.trace('rm for dir %s: Starting.' %self.dir)

        self.rm_subdirs(keepdays)
        if self.empty(): 
            self.log.info('Deleting directory: %s' %self.dir)
            os.rmdir(self.path)     

        self.log.trace('rm for dir %s: Leaving.' %self.dir)

    def rm_subdirs(self, keepdays):
        '''
        tries to remove as many subdirs as possible
        keepdays is a KeepDays object
        '''
        
        self.log.trace('Starting.')

        for subdir in self.subdirs():
            subdir.rm(keepdays)

        self.log.trace('Leaving.')


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

        self.log = logging.getLogger('main.subdir')

        self.parent = parent
        self.subdir = subdir
        self.path = os.path.join(parent.path, subdir)

        self.log.trace('SubDir: Object initialized for subdir %s.'%self.subdir)

    def rm(self, keepdays):
        ''' 
        tries to delete a subdirectory,
        but only if the timing of the parent is older than
        what keepdays object has to say about it
        ''' 
        self.log.trace('rm for subdir %s: Starting.' %self.subdir)

        delta_days = self.parent.delta_t.days
        days = keepdays.get(self.subdir)
        if not days:
            self.log.info("there is not keepdays defined for subdir: %s and no default value either. Doing nothing." %self.subdir)
        else:
            if delta_days > days:
                if os.path.exists(self.path):
                    self.log.info("Deleting subdirectory %s ..." % self.path)
                    shutil.rmtree(self.path)

        self.log.trace('rm for subdir %s: Leaving.' %self.subdir)
