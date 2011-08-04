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

class BatchSubmitPlugin(BatchSubmitInterface):
        '''
        This class is expected to have separate instances for each PandaQueue object. 
        '''
        
        def __init__(self, wmsqueue):
                self.log = logging.getLogger("main.batchsubmitplugin[%s]" %wmsqueue.siteid)
                self.log.info('BatchSubmitPlugin: Object initialized.')
 
        def submitPilots(self, queue, nbpilots, fcl, qcl):
                '''
                queue is the queue
                nsub is the number of pilots to be submitted 
                fcl is the FactoryConfigLoader object
                qcl is the QueueConfigLoader object
                '''

                self.log.debug('submitPilots: Starting with inputs queue=%s nbpilots=%s fcl=%s qcl=%s' %(queue, nbpilots, fcl, qcl)) 

                self.queue = queue
                self.nbpilots = nbpilots
                self.fcl = fcl
                self.qcl = qcl

                now = time.localtime()
                self.logPath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2]) + self.queue.translate(string.maketrans('/:','__'))
                self.logDir = self.fcl.get('Pilots', 'baseLogDir') + self.logPath
                self.logUrl = self.fcl.get('Pilots', 'baseLogDirUrl') + self.logPath

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

                self.JSD.add("executable=%s" % self.fcl.get('Pilots', 'executable'))
                self.JSD.add("transfer_executable = True")
                self.JSD.add("should_transfer_files = YES")
                self.JSD.add("when_to_transfer_output = ON_EXIT_OR_EVICT")

                self.JSD.add("Dir=%s/" % self.logDir)
                self.JSD.add("output=$(Dir)/$(Cluster).$(Process).out")
                self.JSD.add("error=$(Dir)/$(Cluster).$(Process).err")
                self.JSD.add("log=$(Dir)/$(Cluster).$(Process).log")
                self.JSD.add("stream_output=False")
                self.JSD.add("stream_error=False")
                self.JSD.add("notification=Error")
                self.JSD.add("notify_user=%s" % self.fcl.get('Factory', 'factoryOwner'))
                self.JSD.add("universe=vanilla")
                ####  # Probably not so helpful to set these in the JDL
                #if self.qcl.has_option(self.queue, 'memory'):
                #        self.JSD.add("(maxMemory=%d)" % self.qcl.getint(self.queue, 'memory'))
                ####  #if self.config.queues[self.queue]['wallClock'] != None:
                ####  #        self.JSD.add("(maxWallTime=%d)" % self.config.queues[self.queue]['wallClock'],)
                ####  ##print >>JDL
                ####  #self.JSD.add('+MATCH_gatekeeper_url="%s"' % self.config.queues[self.queue]['queue'])
                ####  #self.JSD.add('+MATCH_queue="%s"' % self.config.queues[self.queue]['localqueue'])

                self.JSD.add('+MATCH_APF_QUEUE="%s"' % self.queue)

                self.JSD.add("x509userproxy=%s" % self.qcl.get(self.queue, 'gridProxy'))
                self.JSD.add('periodic_remove = (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > 3600) || (JobStatus == 1 && globusstatus =!= 1 && (CurrentTime - EnteredCurrentStatus) > 86400)')
                ####  # In job environment correct GTAG to URL for logs, JSID should be factoryId

                ### Environment
                environment = 'environment = "PANDA_JSID=%s' % self.fcl.get('Factory', 'factoryId')
                environment += ' GTAG=%s/$(Cluster).$(Process).out' % self.logUrl
                environment += ' APFCID=$(Cluster).$(Process)'
                environment += ' APFFID=%s' % self.fcl.get('Factory', 'factoryId')
                if self.fcl.has_option('Factory', 'monitorURL'):
                        environment += ' APFMON=%s' % self.fcl.get('Factory', 'monitorURL')
                environment += ' FACTORYQUEUE=%s' % self.queue
                if self.fcl.has_option('Factory', 'factoryUser'):
                        environment += ' FACTORYUSER=%s' % self.fcl.get('Factory', 'factoryUser')
                if self.qcl.has_option(self.queue, 'environ'):
                        environ = self.qcl.get(self.queue, 'environ')
                        if environ != 'None' and environ != '':
                                environment += " " + environ
                environment += '"'
                self.JSD.add(environment)

                ### self.JSD.add('environment = "PANDA_JSID=%s"' % self.fcl.get('Factory', 'factoryId'))
                ### self.JSD.add('GTAG=%s/$(Cluster).$(Process).out' % self.logUrl)
                ### self.JSD.add('APFCID=$(Cluster).$(Process)')
                ### self.JSD.add('APFFID=%s' % self.fcl.get('Factory', 'factoryId'))
                ### ####  if isinstance(self.mon, Monitor):
                ### ####          self.JSD.add('APFMON=%s' % self.fcl.get('Factory', 'monitorURL'),)
                ### self.JSD.add('FACTORYQUEUE=%s' % self.queue)
                ### if self.qcl.has_option(self.queue, 'user'):
                ###         self.JSD.add('FACTORYUSER=%s' % self.qcl.get(self.queue, 'user'))
                ### if self.qcl.has_option(self.queue, 'environ'):
                ###         environ = self.qcl.get(self.queue, 'environ')
                ###         if environ != '':
                ###                 self.JSD.add(environ)

                # Adding condor attributes
                if self.qcl.has_option(self.queue, 'condor_attributes'):
                        condor_attributes = self.qcl.get(self.queue, 'condor_attributes')
                        for attr in condor_attributes.split(','):
                                self.JSD.add(attr)

                # In case of Local submission, the env must be passed 
                self.JSD.add('GetEnv = True')

                # adding the arguments to the wrapper
                arguments = 'arguments = '
                arguments += ' --pandasite=%s ' %self.queue
                arguments += ' --pandaqueue=%s ' %self.qcl.get(self.queue, 'nickname')
                arguments += ' --pandagrid=%s ' %self.qcl.get(self.queue, 'pandagrid')
                arguments += ' -j false'
                if self.qcl.has_option(self.queue, 'memory'):
                        arguments += ' -k %s' %self.qcl.get(self.queue, 'memory')
                if self.qcl.has_option(self.queue, 'user'):
                        arguments += ' -u %s' %self.qcl.get(self.queue, 'user')
                if self.qcl.has_option(self.queue, 'group'):
                        arguments += ' -v %s' %self.qcl.get(self.queue, 'group')
                if self.qcl.has_option(self.queue, 'country'):
                        arguments += ' -o %s' %self.qcl.get(self.queue, 'country')
                if self.qcl.has_option(self.queue, 'allowothercountry') and\
                   self.qcl.getboolean(self.queue, 'allowothercountry'):
                        arguments += ' -A True '

                self.JSD.add(arguments)

                #self.JSD.add('"')
                ####  self.JSD.add("arguments = -s %s -h %s" % (self.config.queues[self.queue]['siteid'], self.config.queues[self.queue]['nickname']),)
                ####  self.JSD.add("-p %d -w %s" % (self.config.queues[self.queue]['port'], self.config.queues[self.queue]['server']),)
                ####  if self.config.queues[self.queue]['jobRecovery'] == False:
                ####          self.JSD.add(" -j false",)
                ####  if self.config.queues[self.queue]['memory'] != None:
                ####          self.JSD.add(" -k %d" % self.config.queues[self.queue]['memory'],)
                ####  if self.config.queues[self.queue]['user'] != None:
                ####          self.JSD.add(" -u %s" % self.config.queues[self.queue]['user'],)
                ####  if self.config.queues[self.queue]['group'] != None:
                ####          self.JSD.add(" -v %s" % self.config.queues[self.queue]['group'],)
                ####  if self.config.queues[self.queue]['country'] != None:
                ####          self.JSD.add(" -o %s" % self.config.queues[self.queue]['country'],)
                ####  if self.config.queues[self.queue]['allowothercountry'] == True:
                ####          self.JSD.add(" -A True",)
                self.JSD.add("queue %d" % self.nbpilots)
                #return 0

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
                                self.log.error('__writeJSDFile: Cannot submit pilots for %s', self.queue)
                                return
                self.jdlFile = self.logDir + '/submitMe.jdl'
                ### error = self.writeJDL(queue, jdlFile, pilotNumber, logDir, logUrl, cycleNumber)
                self.JSD.write(self.jdlFile)

                self.log.debug('__writeJSDFile: Leaving.')

        def __submit(self):
                '''
                Submit pilots
                '''

                self.log.debug('__submit: Starting.')

                self.dryRun = self.fcl.getboolean('Factory', 'dryRun')
                if not self.dryRun:
                        self.log.info('Attempt to submit %d pilots for queue %s' %(self.nbpilots, self.queue))
                        (exitStatus, output) = commands.getstatusoutput('condor_submit -verbose ' + self.jdlFile)
                        if exitStatus != 0:
                                self.log.error('condor_submit command for %s failed (status %d): %s', self.queue, exitStatus, output)
                        else:
                                self.log.info('condor_submit command for %s succeeded', self.queue)
                        st, out = exitStatus, output

                else:
                        self.log.debug('Dry run mode - pilot submission supressed.')
                        st, out = None, None

                self.log.debug('__submit: Leaving with output (%s, %s).' %(st, out))
                return st, out


