#!/bin/env python

import commands
import logging
import os
import string
import shutil
import time

from autopyfactory.factory import BatchSubmitInterface
import submit 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class BatchSubmitPlugin(BatchSubmitInterface):
    '''
    This Submit Plugin simply executes a provided local executable. 
    This class is expected to have separate instances for each APFQueue object. 
    '''
    
    def __init__(self, wmsqueue):
        self.log = logging.getLogger("main.batchsubmitplugin[%s]" %wmsqueue.apfqname)
        self.apfqname = wmsqueue.apfqname
        self.factory = wmsqueue.factory
        self.qcl = wmsqueue.factory.qcl
        self.fcl = wmsqueue.factory.fcl

        self.executable = self.qcl.get(self.apfqname, 'executable')
        self.factoryadminemail = self.fcl.get('Factory', 'factoryAdminEmail')
        self.x509userproxy = self.factory.proxymanager.getProxyPath(self.qcl.get(self.apfqname,'proxy'))
        self.factoryid = self.fcl.get('Factory', 'factoryId')

        self.monitorurl = None
        if self.fcl.has_option('Factory', 'monitorURL'):
            self.monitorurl = self.fcl.get('Factory', 'monitorURL')

        self.factoryuser = None
        if self.fcl.has_option('Factory', 'factoryUser'):
            self.factoryuser = self.fcl.get('Factory', 'factoryUser')

        self.environ = None
        if self.qcl.has_option(self.apfqname, 'batchsubmit.condorlocal.environ'):
            self.environ = self.qcl.get(self.apfqname, 'batchsubmit.condorlocal.environ')

        self.condor_attributes = None
        if self.qcl.has_option(self.apfqname, 'batchsubmit.condorlocal.condor_attributes'):
            self.condor_attributes = self.qcl.get(self.apfqname, 'batchsubmit.condorlocal.condor_attributes')

        self.nickname = self.qcl.get(self.apfqname, 'nickname')

        self.pandagrid = None
        if self.qcl.has_option(self.apfqname, 'executable.pandagrid'):
            self.pandagrid = self.qcl.get(self.apfqname, 'executable.pandagrid')
        
        self.pandaserverurl = self.qcl.get(self.apfqname, 'executable.pandaserverurl')
        self.pandawrappertarballurl = self.qcl.get(self.apfqname, 'executable.pandawrappertarballurl')
        
        self.pandaloglevel = None
        if self.qcl.has_option(self.apfqname, 'executable.pandaloglevel'):
            self.pandaloglevel = self.qcl.get(self.apfqname, 'executable.pandaloglevel')
        
        self.arguments = None
        if self.qcl.has_option(self.apfqname, 'executable.arguments'):
            self.arguments = self.qcl.get(self.apfqname, 'executable.arguments')

        self.log.info('BatchSubmitPlugin: Object initialized.')
    
    def submitPilots(self, siteid, nbpilots, fcl, qcl):
        '''
        queue is the queue
        nsub is the number of pilots to be submitted 
        fcl is the FactoryConfigLoader object
        qcl is the QueueConfigLoader object
        '''

        self.log.debug('submitPilots: Starting with inputs siteid=%s nbpilots=%s fcl=%s qcl=%s' %(siteid, nbpilots, fcl, qcl)) 

        self.siteid = siteid 
        self.nbpilots = nbpilots
        self.fcl = fcl
        self.qcl = qcl

        #now = time.localtime()
        now = time.gmtime() # gmtime() is like localtime() but in UTC
        self.logPath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2]) + self.apfqname.translate(string.maketrans('/:','__'))
        self.logDir = self.fcl.get('Factory', 'baseLogDir') + self.logPath
        self.logUrl = self.fcl.get('Factory', 'baseLogDirUrl') + self.logPath

        if self.nbpilots != 0:
            self.__prepareExecutable()
            st, output = self.__run() 
        else:
            st, output = (None, None)

        self.log.debug('submitPilots: Leaving with output (%s, %s).' %(st, output))
        return st, output
    
    
    def __prepareExecutable(self):
        '''
        tries to create destination directory and 
        copies the executable file inside
        '''

        self.log.debug('__prepareExecutable: Starting.')

        if not os.access(self.logDir, os.F_OK):
            try:
                os.makedirs(self.logDir)
                self.log.debug('__writeJSDFile: Created directory %s', self.logDir)
                shutil.copy(self.executable, self.logDir) 
            except OSError, (errno, errMsg):
                self.log.error('__writeJSDFile: Failed to create directory %s (error %d): %s', self.logDir, errno, errMsg)
                self.log.error('__writeJSDFile: Cannot submit pilots for %s', self.siteid)
                return

        self.log.debug('__prepareExecutable: Leaving.')
    
    
    def __run(self):
        '''
        run jobs locally  
        '''

        self.log.debug('__run: Starting.')

        executable = os.path.basename(self.executable)

        # --- argumetns ---
        arguments = 'arguments = '
        arguments += ' --pandasite=%s ' %self.siteid
        arguments += ' --pandaqueue=%s ' %self.nickname
        if self.pandagrid:
                arguments += ' --pandagrid=%s ' %self.pandagrid
        arguments += ' --pandaserverurl=%s ' %self.pandaserverurl
        arguments += ' --pandawrappertarballurl=%s ' %self.pandawrappertarballurl
        if self.pandaloglevel:
                arguments += ' --pandaloglevel=%s' %self.pandaloglevel
        if self.arguments:
                arguments += ' ' + self.arguments


        cmd = 'cd %s; ./%s %s; cd -' %(self.logDir, executable, arguments )
        self.log.info('Attempt to submit command %s' %cmd)

        (exitStatus, output) = commands.getstatusoutput(cmd)
        if exitStatus != 0:
            self.log.error('local execution for %s failed (status %d): %s', self.siteid, exitStatus, output)
        else:
            self.log.info('local execution for %s succeeded', self.siteid)
        st, out = exitStatus, output


        self.log.debug('__run: Leaving with output (%s, %s).' %(st, out))
        return st, out
