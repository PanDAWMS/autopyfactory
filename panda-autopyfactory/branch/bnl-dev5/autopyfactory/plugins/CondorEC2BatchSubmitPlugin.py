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
import submit 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CondorEC2BatchSubmitPlugin(BatchSubmitInterface):
        '''
        This class is expected to have separate instances for each PandaQueue object. 
        '''
        
        def __init__(self, apfqueue):
                self._valid = True
                try:
                        self.log = logging.getLogger("main.batchsubmitplugin[%s]" %apfqueue.apfqname)
                        self.apfqueue = apfqueue
                        self.apfqname = self.apfqueue.apfqname
                        self.factory = apfqueue.factory
                        self.qcl = self.apfqueue.factory.qcl
                        self.fcl = self.apfqueue.factory.fcl

                        self.executable = self.qcl.get(self.apfqname, 'executable')
                        self.factoryadminemail = self.fcl.get('Factory', 'factoryAdminEmail')
                        self.gridresource = self.qcl.get(self.apfqname, 'gridresource') 

                        self.ami_id = self.qcl.get(self.apfqname, 'batchsubmit.condorec2.ami_id')

                        self.instance_type  = self.qcl.get(self.apfqname, 'batchsubmit.condorec2.instance_type')

                        self.user_data = None        
                        if self.qcl.has_option(self.apfqname, 'batchsubmit.condorec2.user_data'):
                                self.user_data = self.fcl.get(self.apfqname, 'batchsubmit.condorec2.user_data')

                        self.queue = None
                        if self.qcl.has_option(self.apfqname,'batchsubmit.condorec2.queue'):
                                self.queue = self.qcl.get(self.apfqname, 'batchsubmit.condorec2.queue')

                        #self.x509userproxy = self.factory.proxymanager.getProxyPath(self.qcl.get(self.apfqname,'proxy'))
                        self.access_key_id = self.factory.proxymanager.getProxyPath(self.qcl.get(self.apfqname,'batchsubmit.condorec2.access_key_id'))
                        self.secret_access_key = self.factory.proxymanager.getProxyPath(self.qcl.get(self.apfqname,'batchsubmit.condorec2.secret_access_key'))

                        self.factoryid = self.fcl.get('Factory', 'factoryId')

                        self.monitorurl = None
                        if self.fcl.has_option('Factory', 'monitorURL'):
                                self.monitorurl = self.fcl.get('Factory', 'monitorURL')

                        self.factoryuser = None
                        if self.fcl.has_option('Factory', 'factoryUser'):
                                self.factoryuser = self.fcl.get('Factory', 'factoryUser')

                        self.environ = None
                        if self.qcl.has_option(self.apfqname, 'batchsubmit.condorec2.environ'):
                                self.environ = self.qcl.get(self.apfqname, 'batchsubmit.condorec2.environ')

                        self.condor_attributes = None
                        if self.qcl.has_option(self.apfqname, 'batchsubmit.condorec2.condor_attributes'):
                                self.condor_attributes = self.qcl.get(self.apfqname, 'batchsubmit.condorec2.condor_attributes')

###                        self.nickname = self.qcl.get(self.apfqname, 'nickname')
###
###                        self.pandagrid = None
###                        if self.qcl.has_option(self.apfqname, 'executable.pandagrid'):
###                                self.pandagrid = self.qcl.get(self.apfqname, 'executable.pandagrid')
###
###                        self.pandaserverurl = self.qcl.get(self.apfqname, 'executable.pandaserverurl') 
###                        self.pandawrappertarballurl = self.qcl.get(self.apfqname, 'executable.pandawrappertarballurl')
###
###                        self.pandaloglevel = None
###                        if self.qcl.has_option(self.apfqname, 'executable.pandaloglevel'):
###                                self.pandaloglevel = self.qcl.get(self.apfqname, 'executable.pandaloglevel')
###
###                        self.arguments = None
###                        if self.qcl.has_option(self.apfqname, 'executable.arguments'):
###                                self.arguments = self.qcl.get(self.apfqname, 'executable.arguments')
###
                        self.log.info('BatchSubmitPlugin: Object initialized.')
                except:
                        self._valid = False

        def valid(self):
                return self._valid

        def submitPilots(self, siteid, nbpilots, fcl, qcl):
                '''
                siteid is the panda queue
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

                self.log.debug('__createJSDFile: Starting.')

                self.JSD.add("# Condor-G glidein pilot for panda")

###                self.JSD.add("Dir=%s/" % self.logDir)
###                self.JSD.add("notify_user=%s" % self.factoryadminemail)
                self.JSD.add("universe=grid")
                self.JSD.add('grid_resource=ec2 %s' % self.gridresource) 
                self.JSD.add("executable=EC2_instance_%s" %self.ami_id)

                # -- MATCH_APF_QUEUE --
                # this token is very important, since it will be used by other plugins
                # to identify this pilot from others when running condor_q
                self.JSD.add('+MATCH_APF_QUEUE="%s"' % self.apfqname)

                # -- proxy path --
                #self.JSD.add("ec2_access_key_id=%s" % self.x509userproxy) 
                #self.JSD.add("ec2_secret_access_key=%s" % self.x509userproxy) 
                self.JSD.add("ec2_access_key_id=%s" % self.access_key_id) 
                self.JSD.add("ec2_secret_access_key=%s" % self.secret_access_key) 

                # -- EC2 specific parameters --
                self.JSD.add("ec2_ami_id=%s" % self.ami_id) 
                self.JSD.add("ec2_instance_type=%s" % self.instance_type) 
                if self.user_data:
                    self.JSD.add('ec2_user_data=%s' % self.user_data)              

                # -- Environment -- 
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

###                # -- Condor attributes -- 
###                if self.condor_attributes:
###                        for attr in self.condor_attributes.split(','):
###                                self.JSD.add(attr)
###
###                # -- Executable and Arguments to the wrapper -- 
###                self.JSD.add("executable=%s" % self.executable)
###                arguments = 'arguments = '
###                arguments += ' --pandasite=%s ' %self.siteid
###                arguments += ' --pandaqueue=%s ' %self.nickname
###                if self.pandagrid:
###                        arguments += ' --pandagrid=%s ' %self.pandagrid
###                arguments += ' --pandaserverurl=%s ' %self.pandaserverurl 
###                arguments += ' --pandawrappertarballurl=%s ' %self.pandawrappertarballurl
###                if self.pandaloglevel:
###                        arguments += ' --pandaloglevel=%s' %self.pandaloglevel
###                if self.arguments:
###                        arguments += ' ' + self.arguments
###                self.JSD.add(arguments)
###
###                # -- globusrsl -- 
###                globusrsl = "globusrsl=(jobtype=single)"
###                if self.queue:
###                        globusrsl += "(queue=%s)" % self.queue
###                self.JSD.add(globusrsl)
###
###                # -- fixed stuffs -- 
###                self.JSD.add("universe=grid")
###                self.JSD.add("output=$(Dir)/$(Cluster).$(Process).out")
###                self.JSD.add("error=$(Dir)/$(Cluster).$(Process).err")
###                self.JSD.add("log=$(Dir)/$(Cluster).$(Process).log")
###                self.JSD.add("stream_output=False")
###                self.JSD.add("stream_error=False")
###                self.JSD.add("notification=Error")
###                self.JSD.add("transfer_executable = True")
###                self.JSD.add("should_transfer_files = YES")
###                self.JSD.add("when_to_transfer_output = ON_EXIT_OR_EVICT")
###                self.JSD.add('periodic_hold=GlobusResourceUnavailableTime =!= UNDEFINED &&(CurrentTime-GlobusResourceUnavailableTime>30)')
###                self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')
###                self.JSD.add('+Nonessential = True')
###                self.JSD.add('copy_to_spool = false')

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

