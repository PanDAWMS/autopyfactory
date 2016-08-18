#!/bin/env python
#
# module to wrap bosco_cluster and related functionality
# .bosco/clusterlist
# .bosco/.pass
# entry=griddev03.racf.bnl.gov max_queued=-1 cluster_type=slurm
#

import logging
import os
import subprocess
import shutil
import threading
import time

# Added to support running module as script from arbitrary location. 
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-2])
import sys
sys.path.insert(0, prepath)

from autopyfactory.interfaces import Singleton

# module level threadlock
boscolock = threading.Lock()

class BoscoCluster(object):


    def __init__(self, entry, cluster_type='pbs', port=22, max_queued=-1,  ):
        self.log = logging.getLogger('boscocluster')
        self.entry = entry
        self.port = port
        self.max_queued = max_queued
        self.cluster_type = cluster_type
        self.log.debug("Init complete. ")

    def getentry(self):
        s += "entry=%s max_queueud=%d cluster_type=%s " % (self.entry, self.max_queued, self.cluster_type) 
        return s
    
    def __str__(self):
        return self.getentry()
    

class BoscoCLI(object):
    '''
    Encapsulates all bosco_cluster command functionality. 
    
    '''
    __metaclass__ = Singleton

    def __init__(self):
        self.log = logging.getLogger("bosco")
        self.log.debug("Initializing bosco module...")
        self.boscopubkeyfile = os.path.expanduser("~/.ssh/bosco_key.rsa.pub")
        self.boscoprivkeyfile = os.path.expanduser("~/.ssh/bosco_key.rsa")
        self.boscopassfile = os.path.expanduser("~/.bosco/.pass")
        self.boscokeydir = os.path.expanduser("~/.ssh")
        if os.path.exists(self.boscokeydir) and os.path.isdir(self.boscokeydir):
            self.log.debug("boscokeydir exists.")
        else:
            self.log.debug("Making boscokeydir.")
            os.mkdir(self.boscokeydir)

    def _checkbosco(self):
        '''
        Confirm BOSCO is installed. 
        '''
        self.log.debug("Checking to see if local bosco_cluster is on path...")
        isinstalled = False
        exetouse = None
        for path in os.environ['PATH'].split(os.pathsep):
            path = path.strip('"')
            exefile = os.path.join(path,'bosco_cluster')
            if os.path.isfile(exefile) and os.access(exefile, os.X_OK):
                isinstalled = True
                exetouse = exefile
        if not isinstalled:
            self.log.error('Missing dep. bosco_cluster not on PATH: %s' % os.environ['PATH'])
            #raise MissingDependencyFailure('Missing dep. bosco_cluster not on PATH: %s' % os.environ['PATH'] )
        else:
            self.log.debug("Using bosco_cluster: %s" % exetouse )
        return isinstalled
   
   
    def _getBoscoClusters(self):
        '''
        [jhover@grid05 ~]$ bosco_cluster -l
        griddev03.racf.bnl.gov/slurm
            
        '''
        cmd = 'bosco_cluster -l'
        self.log.trace("cmd is %s" % cmd) 
        before = time.time()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out = None
        (out, err) = p.communicate()
        delta = time.time() - before
        self.log.trace('It took %s seconds to issue the command' %delta)
        self.log.trace('%s seconds to issue command' %delta)
        if p.returncode == 0 or p.returncode == 2:
            self.log.trace('Leaving with OK return code.')
        else:
            self.log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err )) 
        self.clusters = []
        lines = out.split("\n")
        self.log.trace("got %d lines" % len(lines))
        for line in lines:
            self.log.trace("line is %s" % line)
            if line.strip() == 'No clusters configured':
                self.log.debug("No clusters configured.")
            elif len(line) < 2:
                self.log.trace('empty line discarded')
            else:
                host, batch = line.split('/')
                #  entry, cluster_type='pbs', port=22, max_queued=-1,
                self.log.trace('got entry from bosco: %s %s Making object... ' % (host, batch))
                bentry = BoscoCluster(entry=host, cluster_type=batch)
                self.log.trace('made bentry object, appending.')
                self.clusters.append(bentry)
                
        return self.clusters

    def _clusteradd(self, host, port, batch, pubkeyfile, privkeyfile, passfile):
        self.log.info("Setting up cluster %s/%s " % (host, batch))                 
        
        self.log.trace("ensuring pubkeyfile") 
        shutil.copy(pubkeyfile, self.boscopubkeyfile)
        self.log.trace("ensuring privkeyfile") 
        shutil.copy(privkeyfile, self.boscoprivkeyfile)        
        self.log.trace("ensuring passfile")        
        shutil.copy(passfile, self.boscopassfile )
             
        cmd = 'bosco_cluster -a %s %s ' % (host, setupbatch)
        self.log.trace("cmd is %s" % cmd) 
        before = time.time()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out = None
        (out, err) = p.communicate()        
        delta = time.time() - before
        
        self.log.trace('It took %s seconds to issue the command' %delta)
        self.log.trace('%s seconds to issue command' %delta)
        

    def _clusteraddnative(self,host,batch):
        self.log.info("Setting up cluster %s/%s " % (host,batch))

             
    
    def _checktarget(self, host, port, batch, pubkeyfile, privkeyfile, passfile ):
        '''
        Ensure bosco_cluster has been run.         
        '''
        self.log.debug("Checking to see if remote bosco is installed and up to date...")
        host = host.lower()
        batch = batch.lower()
        try:
            self.log.trace("getting lock")
            boscolock.acquire()
            clist = self._getBoscoClusters()
            self.log.trace("got list of %d clusters" % len(clist))
            found = False
            for c in clist:
                if c.entry == host and c.cluster_type == batch:
                    found = True
            if not found:
                self.log.info("Setting up cluster %s/%s " % (host,batch))
                self._clusteradd(host, port, batch, pubkeyfile, privkeyfile, passfile)
            else:
                self.log.trace("cluster %s/%s already set up." % (host,batch))    
            
        except Exception, e:
            self.log.exception("Exception during bosco remote installation. ")
    
        finally:
            self.log.trace("releasing lock")
            boscolock.release()
            
            
            
if __name__ == '__main__':
    # Set up logging. 
    debug = 0
    info = 0
    trace = 1
    
    # Add TRACE level
    logging.TRACE = 5
    logging.addLevelName(logging.TRACE, 'TRACE')
    
    def trace(self, msg, *args, **kwargs):
        self.log(logging.TRACE, msg, *args, **kwargs)
    
    logging.Logger.trace = trace
    # Check python version 
    major, minor, release, st, num = sys.version_info
    
    # Set up logging, handle differences between Python versions... 
    # In Python 2.3, logging.basicConfig takes no args
    #
    FORMAT23="[ %(levelname)s ] %(asctime)s %(filename)s (Line %(lineno)d): %(message)s"
    FORMAT24=FORMAT23
    FORMAT25="[%(levelname)s] %(asctime)s %(module)s.%(funcName)s(): %(message)s"
    FORMAT26=FORMAT25
    
    if major == 2:
        if minor ==3:
            formatstr = FORMAT23
        elif minor == 4:
            formatstr = FORMAT24
        elif minor == 5:
            formatstr = FORMAT25
        elif minor == 6:
            formatstr = FORMAT26
        elif minor == 7:
            formatstr = FORMAT26
    
    log = logging.getLogger()
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(FORMAT23)
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)
    
    if debug: 
        log.setLevel(logging.DEBUG) # Override with command line switches
    if info:
        log.setLevel(logging.INFO) # Override with command line switches
    if trace:
        log.setLevel(logging.TRACE) 
    log.debug("Logging initialized.")      
    
    bcli = BoscoCLI()
    bcli._checktarget('gridev03.racf.bnl.gov', 'slurm', '~/etc/apf/jhover/pass')
    
    