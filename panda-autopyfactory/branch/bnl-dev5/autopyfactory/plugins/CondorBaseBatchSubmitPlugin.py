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
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorBaseBatchSubmitPlugin(BatchSubmitInterface):
    '''
    This class is expected to have separate instances for each PandaQueue object. 
    '''
    
    def __init__(self, apfqueue, qcl):

        self._valid = True
        self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)

        # JSDFile object where we will write the content condor submission file
        self.JSD = jsd.JSDFile()

        self.apfqname = apfqueue.apfqname
        self.factory = apfqueue.factory
        self.fcl = apfqueue.factory.fcl

        # calculating the directory path from where to submit jobs
        now = time.gmtime() # gmtime() is like localtime() but in UTC
        timePath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2])
        logPath = timePath + self.apfqname.translate(string.maketrans('/:','__'))
        self.logDir = self.fcl.get('Factory', 'baseLogDir') + logPath
        self.logUrl = self.fcl.get('Factory', 'baseLogDirUrl') + logPath

        try:
            self.siteid = qcl.get(self.apfqname, 'siteid')

            self.executable = qcl.get(self.apfqname, 'executable')
            self.factoryadminemail = self.fcl.get('Factory', 'factoryAdminEmail')

            self.x509userproxy = None
            proxy = qcl.get(self.apfqname,'proxy')
            if proxy:
                self.x509userproxy = self.factory.proxymanager.getProxyPath(qcl.get(self.apfqname,'proxy'))
                self.log.debug('proxy is %s. Loaded path from proxymanager: %s' % (proxy, self.x509userproxy))
            else:
                self.log.debug('proxy is None. No proxy configured.')
            
            self.factoryid = self.fcl.get('Factory', 'factoryId')

            self.monitorurl = None
            if self.fcl.has_option('Factory', 'monitorURL'):
                self.monitorurl = self.fcl.get('Factory', 'monitorURL')

            self.factoryuser = None
            if self.fcl.has_option('Factory', 'factoryUser'):
                self.factoryuser = self.fcl.get('Factory', 'factoryUser')

            self.environ = None
            if qcl.has_option(self.apfqname, 'batchsubmit.condorbase.environ'):
                self.environ = qcl.get(self.apfqname, 'batchsubmit.condorbase.environ')

            self.condor_attributes = None
            if qcl.has_option(self.apfqname, 'batchsubmit.condorbase.condor_attributes'):
                self.condor_attributes = qcl.get(self.apfqname, 'batchsubmit.condorbase.condor_attributes')

            self.nickname = qcl.get(self.apfqname, 'nickname')

            self.wrappergrid = None
            if qcl.has_option(self.apfqname, 'executable.wrappergrid'):
                self.wrappergrid = qcl.get(self.apfqname, 'executable.wrappergrid')

            self.wrappervo = None
            if qcl.has_option(self.apfqname, 'executable.wrappervo'):
                self.wrappervo = qcl.get(self.apfqname, 'executable.wrappervo')
            
            self.wrapperserverurl = qcl.get(self.apfqname, 'executable.wrapperserverurl')
            self.wrappertarballurl = qcl.get(self.apfqname, 'executable.wrappertarballurl')
            
            self.wrapperloglevel = None
            if qcl.has_option(self.apfqname, 'executable.wrapperloglevel'):
                self.wrapperloglevel = qcl.get(self.apfqname, 'executable.wrapperloglevel')
            
            self.wrappermode = None
            if qcl.has_option(self.apfqname, 'executable.wrappermode'):
                self.wrappermode = qcl.get(self.apfqname, 'executable.wrappermode')
            
            self.arguments = None
            if qcl.has_option(self.apfqname, 'executable.arguments'):
                self.arguments = qcl.get(self.apfqname, 'executable.arguments')

            self.log.info('BatchSubmitPlugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):
        return self._valid
    
    def submit(self, n):
        '''
        n is the number of pilots to be submitted 
        '''

        self.log.debug('submit: Preparing to submit %s pilots' %n)

        if not utils.checkDaemon('condor'):
                self.log.info('submit: condor daemon is not running. Doing nothing')
                return None, None

        if n != 0:
            self._addJSD()
            self._finishJSD(n)
            jsdfile = self._writeJSD()
            if jdsfile:
                st, output = self.__submit(n, jsdfile) 
            else:
                self.log.info('submit: jdsfile has no value. Doing nothing')
                st, output = (None, None)
        else:
            st, output = (None, None)

        self.log.debug('submit: Leaving with output (%s, %s).' %(st, output))
        return st, output
    
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
        arguments += ' --wrapperbatchqueue=%s ' %self.nickname
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
        # In case of Local submission, the env must be passed 
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
