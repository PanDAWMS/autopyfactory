#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

import commands
import logging
import os
import string
import time

from autopyfactory.factory import BatchSubmitInterface
import autopyfactory.utils as utils
import jsd 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorBaseBatchSubmitPlugin(BatchSubmitInterface):
    
    def __init__(self, apfqueue):

        self._valid = True
        self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        self.factory = apfqueue.factory
        self.fcl = apfqueue.factory.fcl


        self.log.info('BatchSubmitPlugin: Object initialized.')

    def valid(self):
        return self._valid

    def _readconfig(self, qcl):
        '''
        read the config loader object
        '''

        try:
            self.siteid = qcl.generic_get(self.apfqname, 'wmsqueue', logger=self.log)

            self.executable = qcl.generic_get(self.apfqname, 'executable', logger=self.log)
            self.factoryadminemail = self.fcl.generic_get('Factory', 'factoryAdminEmail', logger=self.log)

            self.x509userproxy = None
            if qcl.has_option(self.apfqname,'batchsubmit.condorbase.proxy'):
                proxy = qcl.get(self.apfqname,'batchsubmit.condorbase.proxy')
                self.x509userproxy = self.factory.proxymanager.getProxyPath(proxy)
                self.log.debug('proxy is %s. Loaded path from proxymanager: %s' % (proxy, self.x509userproxy))
            else:
                self.log.debug('proxy is None. No proxy configured.')
            
            self.factoryid = self.fcl.generic_get('Factory', 'factoryId', logger=self.log)
            self.monitorurl = self.fcl.generic_get('Factory', 'monitorURL', logger=self.log)
            self.factoryuser = self.fcl.generic_get('Factory', 'factoryUser', logger=self.log)
            self.environ = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.environ', logger=self.log)
            self.condor_attributes = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.condor_attributes', logger=self.log)
            self.batchqueue = qcl.generic_get(self.apfqname, 'batchqueue', logger=self.log)
            self.wrappergrid = qcl.generic_get(self.apfqname, 'executable.wrappergrid', logger=self.log)
            self.wrappervo = qcl.generic_get(self.apfqname, 'executable.wrappervo', logger=self.log)
            self.wrapperserverurl = qcl.generic_get(self.apfqname, 'executable.wrapperserverurl', logger=self.log)
            self.wrappertarballurl = qcl.generic_get(self.apfqname, 'executable.wrappertarballurl', logger=self.log)
            self.wrapperloglevel = qcl.generic_get(self.apfqname, 'executable.wrapperloglevel', logger=self.log)
            self.wrappermode = qcl.generic_get(self.apfqname, 'executable.wrappermode', logger=self.log)
            self.arguments = qcl.generic_get(self.apfqname, 'executable.arguments', logger=self.log)

            return True
        except:
            return False


    def submit(self, n):
        '''
        n is the number of pilots to be submitted 
        '''

        self.log.debug('submit: Preparing to submit %s pilots' %n)

        if not utils.checkDaemon('condor'):
            self.log.info('submit: condor daemon is not running. Doing nothing')
            return None, None

        if n != 0:

            self._calculateDateDir()

            self.JSD = jsd.JSDFile()
            valid = self._readconfig()
            if not valid:
                self.log.error('submit: self._readconfig returned False, we cannot submit.')
                st, output = (None, None)
            else:
                self.log.debug('submit: self._readconfig returned True. Keep going...')
                self._addJSD()
                self._finishJSD(n)
                jsdfile = self._writeJSD()
                if jsdfile:
                    st, output = self.__submit(n, jsdfile) 
                else:
                    self.log.info('submit: jsdfile has no value. Doing nothing')
                    st, output = (None, None)
        else:
            st, output = (None, None)

        self.log.debug('submit: Leaving with output (%s, %s).' %(st, output))
        return st, output
   
    def _calculateDateDir(self):
        '''
        a new directory is created for each day. 
        Here we calculate it.
        '''

        now = time.gmtime() # gmtime() is like localtime() but in UTC
        timePath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2])
        logPath = timePath + self.apfqname.translate(string.maketrans('/:','__'))
        self.logDir = self.fcl.generic_get('Factory', 'baseLogDir', logger=self.log) + logPath
        self.logUrl = self.fcl.generic_get('Factory', 'baseLogDirUrl', logger=self.log) + logPath
 
    def _addJSD(self):

        self.log.debug('addJSD: Starting.')

        self.JSD.add("Dir=%s/" % self.logDir)
        self.JSD.add("notify_user=%s" % self.factoryadminemail)

        # -- MATCH_APF_QUEUE --
        # this token is very important, since it will be used by other plugins
        # to identify this pilot from others when running condor_q
        self.JSD.add('+MATCH_APF_QUEUE="%s"' % self.apfqname)

        # -- proxy path --
        if self.x509userproxy:
            self.JSD.add("x509userproxy=%s" % self.x509userproxy)

        ### Environment
        environment = 'environment = "PANDA_JSID=%s' % self.factoryid
        environment += ' GTAG=%s/$(Cluster).$(Process).out' % self.logUrl
        environment += ' APFCID=$(Cluster).$(Process)'
        environment += ' APFFID=%s' % self.factoryid
        if self.monitorurl:
            environment += ' APFMON=%s' % self.monitorurl
        environment += ' FACTORYQUEUE=%s' % self.apfqname
        if self.factoryuser:
            environment += ' FACTORYUSER=%s' % self.factoryuser
        if self.environ:
            if self.environ != 'None' and self.environ != '':
                    environment += " " + self.environ
        environment += '"'
        self.JSD.add(environment)

        # Adding condor attributes
        if self.condor_attributes:
            for attr in self.condor_attributes.split(','):
                self.JSD.add(attr)

        # -- Executable and Arguments to the wrapper -- 
        self.JSD.add("executable=%s" % self.executable)
        arguments = 'arguments = '
        if self.wrappervo:
            arguments += ' --wrappervo=%s ' %self.wrappervo
        arguments += ' --wrapperwmsqueue=%s ' %self.siteid
        arguments += ' --wrapperbatchqueue=%s ' %self.batchqueue
        if self.wrappergrid:
            arguments += ' --wrappergrid=%s ' %self.wrappergrid
        arguments += ' --wrapperserverurl=%s ' %self.wrapperserverurl
        arguments += ' --wrappertarballurl=%s ' %self.wrappertarballurl
        if self.wrapperloglevel:
            arguments += ' --wrapperloglevel=%s ' %self.wrapperloglevel
        if self.wrappermode:
            arguments += ' --wrappermode=%s ' %self.wrappermode
        if self.arguments:
            arguments += self.arguments
        self.JSD.add(arguments)


        # -- fixed stuffs -- 
        self.JSD.add("output=$(Dir)/$(Cluster).$(Process).out")
        self.JSD.add("error=$(Dir)/$(Cluster).$(Process).err")
        self.JSD.add("log=$(Dir)/$(Cluster).$(Process).log")
        self.JSD.add("stream_output=False")
        self.JSD.add("stream_error=False")
        self.JSD.add("notification=Error")
        self.JSD.add("transfer_executable = True")
        self.JSD.add("should_transfer_files = YES")
        self.JSD.add("when_to_transfer_output = ON_EXIT_OR_EVICT")
        
        self.log.debug('addJSD: Leaving.')
    
    def __submit(self, n, jsdfile):
        '''
        Submit pilots
        '''

        self.log.debug('__submit: Starting.')

        self.log.info('Attempt to submit %d pilots for queue %s' %(n, self.siteid))

        (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + jsdfile)
        if exitStatus != 0:
            self.log.error('condor_submit command for %s failed (status %d): %s', self.siteid, exitStatus, output)
        else:
            self.log.info('condor_submit command for %s succeeded', self.siteid)
        st, out = exitStatus, output


        self.log.debug('__submit: Leaving with output (%s, %s).' %(st, out))
        return st, out

    def _finishJSD(self, n):
        '''
        add the number of pilots (n)
        '''
        self.log.debug('finishJSD: Starting.')
        self.log.debug('finishJSD: adding queue line with %d jobs' %n)
        self.JSD.add("queue %d" %n)
        self.log.debug('finishJSD: Leaving.')

    def _writeJSD(self):
        '''
        Dumps the whole content of the JSDFile object into a disk file
        '''
    
        self.log.debug('writeJSD: Starting.')
        self.log.debug('writeJSD: the submit file content is\n %s ' %self.JSD)
        out = self.JSD.write(self.logDir, 'submit.jdl')
        self.log.debug('writeJSD: Leaving.')
        return out
