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
    This class is expected to have separate instances for each PandaQueue object. 
    '''
    
    def __init__(self, apfqueue):
        self._valid = True
        try:
            self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)
            self.apfqname = apfqueue.apfqname
            self.factory = apfqueue.factory
            self.qcl = apfqueue.factory.qcl
            self.fcl = apfqueue.factory.fcl

            self.executable = self.qcl.get(self.apfqname, 'executable')
            self.factoryadminemail = self.fcl.get('Factory', 'factoryAdminEmail')
            self.proxy = self.qcl.get(self.apfqname,'proxy')
            if self.proxy:
                self.x509userproxy = self.factory.proxymanager.getProxyPath(self.qcl.get(self.apfqname,'proxy'))
                self.log.debug('self.proxy is %s. Loaded path from proxymanager: %s' % (self.proxy, self.x509userproxy))
            else:
                self.x509userproxy = None
                self.log.debug('self.proxy is None. No proxy configured.')
            
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

            self.wrappergrid = None
            if self.qcl.has_option(self.apfqname, 'executable.wrappergrid'):
                self.wrappergrid = self.qcl.get(self.apfqname, 'executable.wrappergrid')

            self.wrappervo = None
            if self.qcl.has_option(self.apfqname, 'executable.wrappervo'):
                self.wrappervo = self.qcl.get(self.apfqname, 'executable.wrappervo')
            
            self.wrapperserverurl = self.qcl.get(self.apfqname, 'executable.wrapperserverurl')
            self.wrappertarballurl = self.qcl.get(self.apfqname, 'executable.wrappertarballurl')
            
            self.wrapperloglevel = None
            if self.qcl.has_option(self.apfqname, 'executable.wrapperloglevel'):
                self.wrapperloglevel = self.qcl.get(self.apfqname, 'executable.wrapperloglevel')
            
            self.wrappermode = None
            if self.qcl.has_option(self.apfqname, 'executable.wrappermode'):
                self.wrappermode = self.qcl.get(self.apfqname, 'executable.wrappermode')
            
            self.arguments = None
            if self.qcl.has_option(self.apfqname, 'executable.arguments'):
                self.arguments = self.qcl.get(self.apfqname, 'executable.arguments')

            self.log.info('BatchSubmitPlugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):
        return self._valid
    
    def submitPilots(self, siteid, nbpilots, fcl, qcl):
        '''
        queue is the queue
        nsub is the number of pilots to be submitted 
        fcl is the FactoryConfigLoader object
        qcl is the QueueConfigLoader object
        '''

        self.log.debug('submitPilots: Starting with inputs siteid=%s nbpilots=%s fcl=%s qcl=%s' %(siteid, nbpilots, fcl, qcl)) 

        if not utils.checkDaemon('condor'):
                self.log.info('submitPilots: condor daemon is not running. Doing nothing')
                return None, None

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
            self.__prepareJSDFile()
            st, output = self.__submit() 
        else:
            st, output = (None, None)

        self.log.debug('submitPilots: Leaving with output (%s, %s).' %(st, output))
        return st, output
    
    
    def __prepareJSDFile(self):
        self.log.debug('__prepareJSDFile: Starting.')

        self.JSD = submit.JSDFile()
        self.__createJSDFile()
        self.__writeJSDFile()

        self.log.debug('__prepareJSDFile: Leaving.')
    
    
    def __createJSDFile(self):
        ### def writeJDL(self, queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber=0):
        # Encoding the wrapper in the script is a bit inflexible, but saves
        # nasty search and replace on a template file, and means one less 
        # dependency for the factory.


        #try:
        #        JDL = open(self.jdlFile, "w")
        #except IOError, (errno, errMsg) :
        #        self.log.error('Failed to open file %s (error %d): %s', self.jdlFile, errno, errMsg)
        #        return 1

        self.log.debug('__createJSDFile: Starting.')

        self.JSD.add("# Condor-C glidein pilot for panda")

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
            if self.environ != 'None' and environ != '':
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
        self.JSD.add("universe=vanilla")
        self.JSD.add("output=$(Dir)/$(Cluster).$(Process).out")
        self.JSD.add("error=$(Dir)/$(Cluster).$(Process).err")
        self.JSD.add("log=$(Dir)/$(Cluster).$(Process).log")
        self.JSD.add("stream_output=False")
        self.JSD.add("stream_error=False")
        self.JSD.add("notification=Error")
        self.JSD.add("transfer_executable = True")
        self.JSD.add("should_transfer_files = NO")
        #self.JSD.add("transfer_output_files = pilotlog.out")
        self.JSD.add("when_to_transfer_output = ON_EXIT_OR_EVICT")
        #
        # getenv should be false. If portions of the submit environment need to be migrated, 
        # use the Environment = attributes. These will override WN-local env vars.         
        #self.JSD.add('GetEnv = True')
        self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')

        # -- Number of pilots --
        self.JSD.add("queue %d" % self.nbpilots)

        self.log.debug('__createJSDFile: Leaving.')
    
    def __writeJSDFile(self):
        '''
        Dumps the whole content of the JSDFile object into a disk file
        '''

        self.log.debug('__writeJSDFile: Starting.')

        if not os.access(self.logDir, os.F_OK):
            try:
                os.makedirs(self.logDir)
                self.log.debug('__writeJSDFile: Created directory %s', self.logDir)
            except OSError, (errno, errMsg):
                self.log.error('__writeJSDFile: Failed to create directory %s (error %d): %s', self.logDir, errno, errMsg)
                self.log.error('__writeJSDFile: Cannot submit pilots for %s', self.siteid)
                return
        self.jdlFile = self.logDir + '/submit.jdl'
        self.JSD.write(self.jdlFile)
        self.log.debug('__writeJSDFile: the submit file content is\n %s ' %self.JSD)

        self.log.debug('__writeJSDFile: Leaving.')
    
    def __submit(self):
        '''
        Submit pilots
        '''

        self.log.debug('__submit: Starting.')

        self.log.info('Attempt to submit %d pilots for queue %s' %(self.nbpilots, self.siteid))

        (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + self.jdlFile)
        if exitStatus != 0:
            self.log.error('condor_submit command for %s failed (status %d): %s', self.siteid, exitStatus, output)
        else:
            self.log.info('condor_submit command for %s succeeded', self.siteid)
        st, out = exitStatus, output


        self.log.debug('__submit: Leaving with output (%s, %s).' %(st, out))
        return st, out
