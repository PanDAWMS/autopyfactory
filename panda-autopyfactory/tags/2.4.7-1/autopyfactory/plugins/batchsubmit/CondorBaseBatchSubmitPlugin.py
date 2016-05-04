#!/bin/env python
#
# AutoPyfactory batch plugin for Condor
#

import commands
import datetime
import logging
import os
import re
import string
import time
import traceback


from autopyfactory import condor 
from autopyfactory import jsd
from autopyfactory.condor import CondorRequest
from autopyfactory.interfaces import BatchSubmitInterface
from autopyfactory.info import JobInfo
import autopyfactory.utils as utils



class CondorBaseBatchSubmitPlugin(BatchSubmitInterface):
    
    def __init__(self, apfqueue, config=None):
        if not config:
            qcl = apfqueue.qcl            
        else:
            qcl = config
        
        self.log = logging.getLogger("main.batchsubmitplugin[%s]" % apfqueue.apfqname)

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        self.factory = apfqueue.factory
        self.fcl = apfqueue.factory.fcl
        self.mcl = apfqueue.factory.mcl
        
        try:
            self.wmsqueue = qcl.generic_get(self.apfqname, 'wmsqueue')
            self.executable = qcl.generic_get(self.apfqname, 'executable')
            self.factoryadminemail = self.fcl.generic_get('Factory', 'factoryAdminEmail')

            self.factoryid = self.fcl.generic_get('Factory', 'factoryId')
            self.monitorsection = qcl.generic_get(self.apfqname, 'monitorsection')
            self.log.debug("monitorsection is %s" % self.monitorsection)            
            self.monitorurl = self.mcl.generic_get(self.monitorsection, 'monitorURL')
            self.log.debug("monitorURL is %s" % self.monitorurl)
            
            self.factoryuser = self.fcl.generic_get('Factory', 'factoryUser')
            self.submitargs = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.submitargs')
            self.environ = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.environ')
            #self.batchqueue = qcl.generic_get(self.apfqname, 'batchqueue')
            self.arguments = qcl.generic_get(self.apfqname, 'executable.arguments')
            self.condor_attributes = qcl.generic_get(self.apfqname, 'batchsubmit.condorbase.condor_attributes')
            self.extra_condor_attributes = [(opt.replace('batchsubmit.condorbase.condor_attributes.',''),qcl.generic_get(self.apfqname, opt)) \
                                            for opt in qcl.options(self.apfqname) \
                                            if opt.startswith('batchsubmit.condorbase.condor_attributes.')]  # Note the . at the end of the pattern !!

            self.baselogdir = self.fcl.generic_get('Factory', 'baseLogDir') 
            self.baselogdirurl = self.fcl.generic_get('Factory', 'baseLogDirUrl') 
            
            condor.checkCondor()
            self.log.info('BatchSubmitPlugin: Object properly initialized.')
        except Exception, e:
            self.log.error("Caught exception: %s " % str(e))
            raise


    def submit(self, n):
        '''
        n is the number of pilots to be submitted 
        Returns processed list of JobInfo objects. 
        
        '''
        self.log.debug('Preparing to submit %s jobs' %n)
        joblist = None

        if not utils.checkDaemon('condor'):
            self.log.info('condor daemon is not running. Doing nothing')
            return joblist
        
        try:
            if n > 0:
                self._calculateDateDir()
                self.JSD = jsd.JSDFile()
                self._getX509Proxy()
                self._addJSD()
                self._custom_attrs()
                self._finishJSD(n)
                jsdfile = self._writeJSD()
                if jsdfile:
                    st, output = self.__submit(n, jsdfile)
                    self.log.debug('Got output (%s, %s).' %(st, output)) 
                    joblist = self._parseCondorSubmit(output)
                else:
                    self.log.info('jsdfile has no value. Doing nothing')
            elif n < 0:
                # For certain plugins, this means to retire or terminate nodes...
                self.log.debug('Preparing to retire %s jobs' % abs(n))
                self.retire(abs(n))
            else:
                self.log.debug("Asked to submit 0. Doing nothing...")
            
            self.log.debug('Done. Returning joblist %s.' %joblist)
                
        except Exception, e:
            self.log.error('Exception during submit processing. Exception: %s' % e)
            self.log.debug("Exception: %s" % traceback.format_exc())
        return joblist
        

    def retire(self, num):
        '''
         Do nothing by default. 
        '''
        self.log.debug('Default retire() do nothing.')


    def cleanup(self):
        '''
        
        '''
        self.log.info("Cleanup called. Noop.")
  
    def _parseCondorSubmit(self, output):
        '''
        Parses raw output from condor_submit -verbose and returns list of JobInfo objects. 
        
        condor_submit -verbose output:
                
** Proc 769012.0:
Args = "--wrappergrid=OSG --wrapperwmsqueue=BNL_CVMFS_1 --wrapperbatchqueue=BNL_CVMFS_1-condor --wrappervo=ATLAS --wrappertarballurl=http://dev.racf.bnl.gov/dist/wrapper/wrapper-0.9.7-0.9.3.tar.gz --wrapperserverurl=http://pandaserver.cern.ch:25080/cache/pilot --wrapperloglevel=debug --script=pilot.py --libcode=pilotcode.tar.gz,pilotcode-rc.tar.gz --pilotsrcurl=http://panda.cern.ch:25880/cache -f false -m false --user managed"
BufferBlockSize = 32768
BufferSize = 524288
Cmd = "/usr/libexec/wrapper.sh"
CommittedSlotTime = 0
CommittedSuspensionTime = 0
CommittedTime = 0
CompletionDate = 0
CondorPlatform = "$CondorPlatform: X86_64-CentOS_5.8 $"
CondorVersion = "$CondorVersion: 7.9.0 Jun 19 2012 PRE-RELEASE-UWCS $"
CoreSize = 0
CumulativeSlotTime = 0
CumulativeSuspensionTime = 0
CurrentHosts = 0
CurrentTime = time()
DiskUsage = 22
EC2TagNames = "(null)"
EnteredCurrentStatus = 1345558923
Environment = "FACTORYUSER=apf APFFID=BNL-gridui08-jhover APFMON=http://apfmon.lancs.ac.uk/mon/ APFCID=769012.0 PANDA_JSID=BNL-gridui08-jhover FACTORYQUEUE=BNL_CVMFS_1-gridgk07 GTAG=http://gridui08.usatlas.bnl.gov:25880/2012-08-21/BNL_CVMFS_1-gridgk07/769012.0.out"
Err = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07//769012.0.err"
ExecutableSize = 22
ExitBySignal = false
ExitStatus = 0
GlobusResubmit = false
GlobusRSL = "(jobtype=single)(queue=cvmfs)"
GlobusStatus = 32
GridResource = "gt5 gridgk07.racf.bnl.gov/jobmanager-condor"
ImageSize = 22
In = "/dev/null"
Iwd = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07"
JobNotification = 3
JobPrio = 0
JobStatus = 1
JobUniverse = 9
KillSig = "SIGTERM"
LastSuspensionTime = 0
LeaveJobInQueue = false
LocalSysCpu = 0.0
LocalUserCpu = 0.0
MATCH_APF_QUEUE = "BNL_CVMFS_1-gridgk07"
MaxHosts = 1
MinHosts = 1
MyType = "Job"
NiceUser = false
Nonessential = true
NotifyUser = "jhover@bnl.gov"
NumCkpts = 0
NumGlobusSubmits = 0
NumJobStarts = 0
NumRestarts = 0
NumSystemHolds = 0
OnExitHold = false
OnExitRemove = true
Out = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07//769012.0.out"
Owner = "apf"
PeriodicHold = false
PeriodicRelease = false
PeriodicRemove = false
QDate = 1345558923
Rank = 0.0
RemoteSysCpu = 0.0
RemoteUserCpu = 0.0
RemoteWallClockTime = 0.0
RequestCpus = 1
RequestDisk = DiskUsage
RequestMemory = ifthenelse(MemoryUsage =!= undefined,MemoryUsage,( ImageSize + 1023 ) / 1024)
Requirements = true
RootDir = "/"
ShouldTransferFiles = "YES"
StreamErr = false
StreamOut = false
TargetType = "Machine"
TotalSuspensions = 0
TransferIn = false
UserLog = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07/769012.0.log"
WantCheckpoint = false
WantClaiming = false
WantRemoteIO = true
WantRemoteSyscalls = false
WhenToTransferOutput = "ON_EXIT_OR_EVICT"
x509UserProxyEmail = "jhover@bnl.gov"
x509UserProxyExpiration = 1346126473
x509UserProxyFirstFQAN = "/atlas/usatlas/Role=production/Capability=NULL"
x509UserProxyFQAN = "/DC=org/DC=doegrids/OU=People/CN=John R. Hover 47116,/atlas/usatlas/Role=production/Capability=NULL,/atlas/lcg1/Role=NULL/Capability=NULL,/atlas/usatlas/Role=NULL/Capability=NULL,/atlas/Role=NULL/Capability=NULL"
x509userproxysubject = "/DC=org/DC=doegrids/OU=People/CN=John R. Hover 47116"
x509userproxy = "/tmp/prodProxy"
x509UserProxyVOName = "atlas"

        '''
        
        self.log.debug('Starting')

        now = datetime.datetime.utcnow()
        joblist = []
        lines = output.split('\n')
        for line in lines:
            jobidline = None
            if line.strip().startswith('**'):
                jobidline = line.split()
                procid = jobidline[2]
                procid = procid.replace(':','') # remove trailing colon
                ji = JobInfo(procid, 'submitted', now)
                joblist.append(ji)
        if not len(joblist) > 0:
            self.log.debug('joblist has length 0, returning None')
            joblist = None

        self.log.debug('Leaving with joblist = %s' %joblist )
        return joblist
        
    
    def _calculateDateDir(self):
        '''
        a new directory is created for each day. 
        Sets logDir and logUrl
        Here we calculate it.
        '''
        now = time.gmtime() # gmtime() is like localtime() but in UTC
        timePath = "/%04d-%02d-%02d/" % (now[0], now[1], now[2])
        logPath = timePath + self.apfqname.translate(string.maketrans('/:','__'))
        self.logDir = self.baselogdir + logPath
        self.logUrl = self.baselogdirurl + logPath
        
 
    def _addJSD(self):

        self.log.debug('addJSD: Starting.')

        self.JSD.add("Dir", "%s/" % self.logDir)
        self.JSD.add("notify_user", "%s" % self.factoryadminemail)

        # -- MATCH_APF_QUEUE --
        # this token is very important, since it will be used by other plugins
        # to identify this pilot from others when running condor_q
        self.JSD.add('+MATCH_APF_QUEUE', '"%s"' % self.apfqname)

        ### Environment
        environment = '"PANDA_JSID=%s' % self.factoryid
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
        self.JSD.add('environment', environment)


        self.JSD.add("executable", "%s" % self.executable)
        self.JSD.add('arguments', '%s' % self.arguments)

        # -- fixed stuffs -- 
        self.JSD.add("output", "$(Dir)/$(Cluster).$(Process).out")
        self.JSD.add("error", "$(Dir)/$(Cluster).$(Process).err")
        self.JSD.add("log", "$(Dir)/$(Cluster).$(Process).log")
        self.JSD.add("stream_output", "False")
        self.JSD.add("stream_error", "False")
        self.JSD.add("notification", "Error")
        self.JSD.add("transfer_executable", "True")
        
        self.log.debug('addJSD: Leaving.')
   
    def __parse_condor_attribute(self, s):
        '''
        auxiliar method to help splitting the string
        using the comma as splitting character.
        The trick here is what to do when the comma is preceded 
        by one or more \
        Sometimes the user wants the comma to be taken literally 
        instead of as an splitting char. In that case, the comma
        can be escaped with a \.
        And the \ can be escaped with another \ in case the user
        wants the \ to be literal. 
        '''
        p = re.compile(r"(\\)+,")  # regex matching for 1 or more \ followed by a ,
                                   # the backslash appears twice, but it means a single \
        m = re.finditer(p, s)      # searching for all ocurrencies 

        # now we create a list of pairs (x,y) where 
        #   x is the index of the first char matching the regexp: the first \ in our case.
        #   y is the index of the last char matching the regexp: the , in our case
        l = [(i.start(), i.end()) for i in m] 
       
        # we reverse the list, to start processing it from the end to the beginning.
        # In this way, each manipulation will not change the rest of indexes. 
        l.reverse()
        for i in l:
            nb_slashes = i[1] - i[0] - 1
            nb_real_slashes = nb_slashes / 2
            # each pair \\ actually has to be translated as \
        
            if nb_slashes % 2 == 0:
                # even nb of slashes
                # => nb/2 real slashes, and comma is splitting char
                s = s[:i[0]] + '\\'* nb_real_slashes + "," + s[i[1]:]
            else:
                # odd nb of slashes
                # => (nb-1)/2 real slashes, and comma is literal 
                s = s[:i[0]] + '\\'* nb_real_slashes + "APF_LITERAL_COMMA" + s[i[1]:]
        
        fields = []
        for field in s.split(','):
                # we change back the fake string APF_LITERAL_COMMA by an actual ,
                field = field.replace('APF_LITERAL_COMMA', ',')
                fields.append(field)
        
        return fields

 
    def __submit(self, n, jsdfile):
        '''
        Submit pilots
        '''

        self.log.debug('Starting.')

        self.log.info('Attempt to submit %d pilots for queue %s' %(n, self.wmsqueue))

        ###     cmd = 'condor_submit -verbose '
        ###     self.log.debug('submitting using executable condor_submit from PATH=%s' %utils.which('condor_submit'))
        ###     # NOTE: -verbose is needed. 
        ###     # The output generated with -verbose is parsed by the monitor code to determine the number of jobs submitted
        ###     if self.submitargs:
        ###         cmd += self.submitargs
        ###     cmd += ' ' + jsdfile
        ###     self.log.info('command = %s' %cmd)
        ###
        ###     (exitStatus, output) = commands.getstatusoutput(cmd)
        ###     if exitStatus != 0:
        ###         self.log.error('condor_submit command for %s failed (status %d): %s', self.wmsqueue, exitStatus, output)
        ###     else:
        ###         self.log.info('condor_submit command for %s succeeded', self.wmsqueue)
        ###     st, out = exitStatus, output

        req = CondorRequest()
        req.cmd = 'condor_submit'
        args = ''
        if self.submitargs:
            args += self.submitargs
        args += ' ' + jsdfile
        req.args = args
        self.factory.condorrequestsqueue.put(req)
        
        while not req.out:
            time.sleep(1)
        out = req.out
        st = req.rc
        if st != 0:
            self.log.error('condor_submit command for %s failed (status %d): %s', self.wmsqueue, st, out)
        else:
            self.log.info('condor_submit command for %s succeeded', self.wmsqueue)

        self.log.debug('Leaving with output (%s, %s).' %(st, out))
        return st, out


    def _custom_attrs(self):
        ''' 
        adding custom attributes from the queues.conf file
        ''' 
        self.log.debug('Starting.')

        if self.condor_attributes:
            for attr in self.__parse_condor_attribute(self.condor_attributes):
                if '=' in attr:
                    #key = attr.split('=')[0]
                    #value = '='.join( attr.split('=')[1:] )
                    key, value = attr.split('=', 1)
                    self.JSD.add(key, value)
                else:
                    # I think this will never happens
                    self.JSD.add(attr)

        for item in self.extra_condor_attributes:
            self.JSD.add(item[0], item[1])

        self.log.debug('Leaving.')


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
