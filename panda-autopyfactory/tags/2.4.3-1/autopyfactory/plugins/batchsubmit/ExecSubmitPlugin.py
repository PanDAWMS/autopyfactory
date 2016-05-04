#!/bin/env python

import commands
import logging
import os
import string
import shutil
import time

from autopyfactory.interfaces import BatchSubmitInterface


class ExecSubmitPlugin(BatchSubmitInterface):
    '''
    This Submit Plugin simply executes a provided local executable. 
    This class is expected to have separate instances for each APFQueue object. 
    '''
    
    def __init__(self, apfqueue):
        try:
            self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)
            self.apfqname = apfqueue.apfqname
            self.factory = apfqueue.factory
            self.qcl = apfqueue.factory.qcl
            self.fcl = apfqueue.factory.fcl

            self.executable = self.qcl.generic_get(self.apfqname, 'executable')
            self.factoryadminemail = self.fcl.generic_get('Factory', 'factoryAdminEmail')
            self.x509userproxy = self.factory.proxymanager.getProxyPath(self.qcl.get(self.apfqname,'proxy'))
            self.factoryid = self.fcl.generic_get('Factory', 'factoryId', logger=self)
            self.monitorurl = self.fcl.generic_get('Factory', 'monitorURL')
            self.factoryuser = self.fcl.generic_get('Factory', 'factoryUser')
            self.environ = self.qcl.generic_get(self.apfqname, 'batchsubmit.condorlocal.environ')
            self.condor_attributes = self.qcl.generic_get(self.apfqname, 'batchsubmit.condorlocal.condor_attributes')
            self.batchqueue = self.qcl.generic_get(self.apfqname, 'batchqueue')
            self.arguments = self.qcl.generic_get(self.apfqname, 'executable.arguments')

            self.log.info('BatchSubmitPlugin: Object initialized.')
        except Exception, ex:
            self.log.error("BatchSubmitPlugin object initialization failed. Raising exception")
            raise ex
   
    def submitPilots(self, wmsqueue, nbpilots, fcl, qcl):
        '''
        queue is the queue
        nsub is the number of pilots to be submitted 
        fcl is the FactoryConfigLoader object
        qcl is the QueueConfigLoader object
        '''

        self.log.debug('Starting with inputs wmsqueue=%s nbpilots=%s fcl=%s qcl=%s' %(wmsqueue, nbpilots, fcl, qcl)) 

        self.wmsqueue = wmsqueue 
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

        self.log.debug('Leaving with output (%s, %s).' %(st, output))
        return st, output
    
    def cleanup(self):
        pass
    
    def __prepareExecutable(self):
        '''
        tries to create destination directory and 
        copies the executable file inside
        '''

        self.log.debug('Starting.')

        if not os.access(self.logDir, os.F_OK):
            try:
                os.makedirs(self.logDir)
                self.log.debug('__writeJSDFile: Created directory %s', self.logDir)
                shutil.copy(self.executable, self.logDir) 
            except OSError, (errno, errMsg):
                self.log.error('__writeJSDFile: Failed to create directory %s (error %d): %s', self.logDir, errno, errMsg)
                self.log.error('__writeJSDFile: Cannot submit pilots for %s', self.wmsqueue)
                return

        self.log.debug('Leaving.')
    
    
    def __run(self):
        '''
        run jobs locally  
        '''

        self.log.debug('Starting.')

        executable = os.path.basename(self.executable)

        # --- argumetns ---
        arguments = 'arguments = %s' %self.arguments
        cmd = 'cd %s; ./%s %s; cd -' %(self.logDir, executable, arguments )
        self.log.info('Attempt to submit command %s' %cmd)

        (exitStatus, output) = commands.getstatusoutput(cmd)
        if exitStatus != 0:
            self.log.error('local execution for %s failed (status %d): %s', self.wmsqueue, exitStatus, output)
        else:
            self.log.info('local execution for %s succeeded', self.wmsqueue)
        st, out = exitStatus, output


        self.log.debug('Leaving with output (%s, %s).' %(st, out))
        return st, out
