#!/usr/bin/env python
'''
   Condor-related common utilities and library for AutoPyFactory.
   Focussed on properly processing output of condor_q -xml and condor_status -xml and converting
   to native Python data structures. 

'''
import commands
import datetime
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import xml.dom.minidom

import autopyfactory.utils as utils
from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure

from pprint import pprint
from Queue import Queue




# FIXME !!!
# this should not be here !!!
condorrequestsqueue = Queue()


# FIXME
# factory, submitargs and wmsqueue should not be needed
def mynewsubmit(n, jsdfile, factory, wmsqueue, submitargs=None):
    '''
    Submit pilots
    '''
    
    log = logging.getLogger() # FIXME !!
    log.trace('Starting.')

    log.info('Attempt to submit %d pilots for queue %s' %(n, wmsqueue))

    ###     cmd = 'condor_submit -verbose '
    ###     self.log.trace('submitting using executable condor_submit from PATH=%s' %utils.which('condor_submit'))
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

    # FIXME:
    # maybe this should not be here???
    processcondorrequests = ProcessCondorRequests()
    processcondorrequests.start()


    req = CondorRequest()
    req.cmd = 'condor_submit'
    args = ' -verbose '
    if submitargs:
        args += submitargs
        args += ' '
    args += ' ' + jsdfile
    req.args = args
    condorrequestsqueue.put(req)

    while not req.out:
        time.sleep(1)
    out = req.out
    st = req.rc
    if st != 0:
        log.error('condor_submit command for %s failed (status %d): %s', wmsqueue, st, out)
    else:
        log.info('condor_submit command for %s succeeded', wmsqueue)

    log.trace('Leaving with output (%s, %s).' %(st, out))
    return st, out


def parsecondorsubmit(output):
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

    log = logging.getLogger() # FIXME !! 
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
        log.trace('joblist has length 0, returning None')
        joblist = None

    log.trace('Leaving with joblist = %s' %joblist )
    return joblist


def classad2dict(outlist):
    '''
    convert each ClassAd object into a python dictionary.
    In the process, every value is converted into an string
    (needed because some numbers are retrieved originally as integers
    but we want to compare them with strings)
    '''
    out = []

    for job in outlist:
        job_dict = {}
        for k in job:
            job_dict[k] = str( job[k] )
        out.append( job_dict )
    return out 


def mincondorversion(major, minor, release):
    '''
    Call which sets a minimum HTCondor version. If the existing version is too low, it throws an exception.
    
    '''

    log = logging.getLogger() # FIXME !!
    s,o = commands.getstatusoutput('condor_version')
    if s == 0:
        cvstr = o.split()[1]
        log.debug('Condor version is: %s' % cvstr)
        maj, min, rel = cvstr.split('.')
        maj = int(maj)
        min = int(min)
        rel = int(rel)
        
        if maj < major:
            raise CondorVersionFailure("HTCondor version %s too low for the CondorEC2BatchSubmitPlugin. Requires 8.1.2 or above." % cvstr)
        if maj == major and min < minor:
            raise CondorVersionFailure("HTCondor version %s too low for the CondorEC2BatchSubmitPlugin. Requires 8.1.2 or above." % cvstr)
        if maj == major and min == minor and rel < release:
            raise CondorVersionFailure("HTCondor version %s too low for the CondorEC2BatchSubmitPlugin. Requires 8.1.2 or above." % cvstr)
    else:
        ec2log.error('condor_version program not available!')
        raise CondorVersionFailure("HTCondor required but not present!")


def checkCondor():
    '''
    Perform sanity check on condor environment.
    Does condor_q exist?
    Is Condor running?
    '''
    
    # print condor version
    log = logging.getLogger() # FIXME !!
    (s,o) = commands.getstatusoutput('condor_version')
    if s == 0:
        log.trace('Condor version is: \n%s' % o )       
        CONDOR_CONFIG = os.environ.get('CONDOR_CONFIG', None)
        if CONDOR_CONFIG:
            log.trace('Environment variable CONDOR_CONFIG set to %s' %CONDOR_CONFIG)
        else:
            log.trace("Condor config is: \n%s" % commands.getoutput('condor_config_val -config'))
    else:
        log.error('checkCondor() has been called, but not Condor is available on system.')
        raise ConfigFailure("No Condor available on system.")


def statuscondor(queryargs = None):
    '''
    Return info about job startd slots. 
    '''
    log = logging.getLogger() # FIXME !!
    cmd = 'condor_status -xml '
    if queryargs:
        cmd += queryargs
    log.trace('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.trace('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.trace('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s out=%s' %(p.returncode, err, out ))
        out = None
    return out

def statuscondormaster(queryargs = None):
    '''
    Return info about masters. 
    '''
    log = logging.getLogger() # FIXME !!
    cmd = 'condor_status -master -xml '
    if queryargs:
        cmd += queryargs
    
    log.trace('Querying cmd = %s' % cmd.replace('\n','\\n'))
    #log.trace('Querying cmd = %s' % cmd)
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.trace('It took %s seconds to perform the query' %delta)

    if p.returncode == 0:
        log.trace('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s out=%s' %(p.returncode, err, out ))
        out = None
    return out

def querycondor(queryargs=None):
    '''
    Query condor for specific job info and return xml representation string
    for further processing.

    queryargs are potential extra query arguments from queues.conf    
    queryargs are possible extra query arguments from queues.conf 
    '''

    log = logging.getLogger() # FIXME !!
    log.trace('Starting.')
    querycmd = "condor_q "
    log.trace('_querycondor: using executable condor_q in PATH=%s' %utils.which('condor_q'))


    # adding extra query args from queues.conf
    if queryargs:
        querycmd += queryargs 

    querycmd += " -format ' MATCH_APF_QUEUE=%s' match_apf_queue"
    querycmd += " -format ' JobStatus=%d\n' jobstatus"
    querycmd += " -format ' GlobusStatus=%d\n' globusstatus"
    querycmd += " -xml"

    log.trace('Querying cmd = %s' %querycmd.replace('\n','\\n'))

    before = time.time()          
    p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('condor_q: %s seconds to perform the query' %delta)

    if p.returncode == 0:
        log.trace('Leaving with OK return code.')
    else:
        # lets try again. Sometimes RC!=0 does not mean the output was bad
        if out.startswith('<?xml version="1.0"?>'):
            log.warning('RC was %s but output is still valid' %p.returncode)
        else:
            log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
    log.trace('_querycondor: Out is %s' % out)
    log.trace('_querycondor: Leaving.')
    return out
    


def querycondorxml(queryargs=None):
    '''
    Return human readable info about startds. 
    '''
    log = logging.getLogger() # FIXME !!
    cmd = 'condor_q -xml '

    # adding extra query args from queues.conf
    if queryargs:
        querycmd += queryargs 
       
    log.trace('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.trace('It took %s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.trace('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    log.trace('Out is %s' % out)
    log.trace('Leaving.')
    return out


def xml2nodelist(input):
    log = logging.getLogger() # FIXME !!
    xmldoc = xml.dom.minidom.parseString(input).documentElement
    nodelist = []
    for c in listnodesfromxml(xmldoc, 'c') :
        node_dict = node2dict(c)
        nodelist.append(node_dict)
    log.trace('_parseoutput: Leaving and returning list of %d entries.' %len(nodelist))
    log.debug('Got list of %d entries.' %len(nodelist))
    return nodelist


def parseoutput(output):
    '''
    parses XML output of condor_q command with an arbitrary number of attribute -format arguments,
    and creates a Python List of Dictionaries of them. 
    
    Input:
    <!DOCTYPE classads SYSTEM "classads.dtd">
    <classads>
        <c>
            <a n="match_apf_queue"><s>BNL_ATLAS_1</s></a>
            <a n="jobstatus"><i>2</i></a>
        </c>
        <c>
            <a n="match_apf_queue"><s>BNL_ATLAS_1</s></a>
            <a n="jobstatus"><i>1</i></a>
        </c>
    </classads>                       
    
    Output:
    [ { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '2' },
      { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '1' }
    ]
    
    If the query has no 'c' elements, returns empty list
    
    '''

    log=logging.getLogger() # FIXME !!
    log.trace('Starting.')                

    # first convert the XML output into a list of XML docs
    outputs = _out2list(output)

    nodelist = []
    for output in outputs:
        xmldoc = xml.dom.minidom.parseString(output).documentElement
        for c in listnodesfromxml(xmldoc, 'c') :
            node_dict = node2dict(c)
            nodelist.append(node_dict)            
    log.debug('Got list of %d entries.' %len(nodelist))       
    return nodelist


def _out2list(xmldoc):
    '''
    converts the xml output of condor_q into a list.
    This is in case the output is a multiple XML doc, 
    as it happens when condor_q -g 
    So each part of the output is one element of the list
    '''

    # we assume the header of each part of the output starts
    # with string '<?xml version="1.0"?>'
    #indexes = [m.start() for m in re.finditer('<\?xml version="1.0"\?>',  xmldoc )]
    indexes = [m.start() for m in re.finditer('<\?xml',  xmldoc )]
    if len(indexes)==1:
        outs = [xmldoc]
    else:
        outs = []
        for i in range(len(indexes)):
            if i == len(indexes)-1:
                tmp = xmldoc[indexes[i]:]
            else:
                tmp = xmldoc[indexes[i]:indexes[i+1]]
            outs.append(tmp)
    return outs



def listnodesfromxml( xmldoc, tag):
    return xmldoc.getElementsByTagName(tag)


def node2dict(node):
    '''
    parses a node in an xml doc, as it is generated by 
    xml.dom.minidom.parseString(xml).documentElement
    and returns a dictionary with the relevant info. 
    An example of output looks like
           {'globusstatus':'32', 
             'match_apf_queue':'UC_ITB', 
             'jobstatus':'1'
           }        
    
    
    '''
    log = logging.getLogger() # FIXME !!
    dic = {}
    for child in node.childNodes:
        if child.nodeType == child.ELEMENT_NODE:
            key = child.attributes['n'].value
            #log.trace("child 'n' key is %s" % key)
            if len(child.childNodes[0].childNodes) > 0:
                try:
                    value = child.childNodes[0].firstChild.data
                    dic[key.lower()] = str(value)
                except AttributeError:
                    dic[key.lower()] = 'NONE'
    return dic


def aggregateinfo(input):
    '''
    This function takes a list of job status dicts, and aggregates them by queue,
    ignoring entries without MATCH_APF_QUEUE
    
    Assumptions:
      -- Input has a single level of nesting, and consists of dictionaries.
      -- You are only interested in the *count* of the various attributes and value 
      combinations. 
     
    Example input:
    [ { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '2' },
      { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '1' }
    ]                        
    
    Output:
    { 'UC_ITB' : { 'jobstatus' : { '1': '17',
                                   '2' : '24',
                                   '3' : '17',
                                 },
                   'globusstatus' : { '1':'13',
                                      '2' : '26',
                                      }
                  },
    { 'BNL_TEST_1' :{ 'jobstatus' : { '1':  '7',
                                      '2' : '4',
                                      '3' : '6',
                                 },
                   'globusstatus' : { '1':'12',
                                      '2' : '46',
                                      }
                  }, 
                  
    If input is empty list, output is empty dictionary
                 
    '''
    log=logging.getLogger() # FIXME !!
    log.trace('Starting with list of %d items.' % len(input))
    queues = {}
    for item in input:
        if not item.has_key('match_apf_queue'):
            # This job is not managed by APF. Ignore...
            continue
        apfqname = item['match_apf_queue']
        # get current dict for this apf queue
        try:
            qdict = queues[apfqname]
        # Or create an empty one and insert it.
        except KeyError:
            qdict = {}
            queues[apfqname] = qdict    
        
        # Iterate over attributes and increment counts...
        for attrkey in item.keys():
            # ignore the match_apf_queue attrbute. 
            if attrkey == 'match_apf_queue':
                continue
            attrval = item[attrkey]
            # So attrkey : attrval in joblist
            # Get current attrdict for this attribute from qdict
            try:
                attrdict = qdict[attrkey]
            except KeyError:
                attrdict = {}
                qdict[attrkey] = attrdict
            
            try:
                curcount = qdict[attrkey][attrval]
                qdict[attrkey][attrval] = curcount + 1                    
            except KeyError:
                qdict[attrkey][attrval] = 1
                   
    log.trace('Aggregate: output is %s ' % queues)  # this could be trace() instead of debug()
    log.debug('Aggregate: Created dict with %d queues.' % len(queues))
    return queues

  

def getJobInfo():
    log = logging.getLogger() # FIXME !!
    xml = querycondorxml()
    nl = xml2nodelist(xml)
    log.debug("Got node list of length %d" % len(nl))
    joblist = []
    qd = {}
    if len(nl) > 0:
        for n in nl:
            j = CondorEC2JobInfo(n)
            joblist.append(j)
        
        indexhash = {}
        for j in joblist:
            try:
                i = j.match_apf_queue
                indexhash[i] = 1
            except:
                # We don't care about jobs not from APF
                pass

        for k in indexhash.keys():
        # Make a list for jobs for each apfqueue
            qd[k] = []
        
        # We can now safely do this..
        for j in joblist:
            try:
                index = j.match_apf_queue
                qjl = qd[index]
                qjl.append(j)
            except:
                # again we don't care about non-APF jobs
                pass    
            
    log.debug("Made job list of length %d" % len(joblist))
    log.debug("Made a job info dict of length %d" % len(qd))
    return qd


def getStartdInfoByEC2Id():
    log = logging.getLogger() # FIXME !!
    out = statuscondor()
    nl = xml2nodelist(out)
    infolist = {}
    for n in nl:
        #print(n)
        try:
            ec2iid = n['ec2instanceid']
            state = n['state']
            act = n['activity']
            slots = n['totalslots']
            machine = n['machine']
            j = CondorStartdInfo(ec2iid, machine, state, act)
            #log.trace("Created csdi: %s" % j)
            j.slots = slots
            infolist[ec2iid] = j
        except Exception, e:
            log.error("Bad node. Error: %s" % str(e))
    return infolist
    

def killids(idlist):
    '''
    Remove all jobs by jobid in idlist.
    Idlist is assumed to be a list of complete ids (<clusterid>.<procid>)
     
    '''
    log = logging.getLogger() # FIXME !!
    idstring = ' '.join(idlist)
    cmd = 'condor_rm %s' % idstring
    log.trace('Issuing remove cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.trace('It took %s seconds to perform the command' %delta)
    if p.returncode == 0:
        log.trace('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    

class CondorRequest(object):
    '''
    class to define any arbitrary condor task 
    (condor_submit, condor_on, condor_off...)

    The instances of this class can be piped into a Queue() object
    for serialization
    '''

    def __init__(self):

        self.cmd = None
        self.args = None
        self.out = None
        self.err = None
        self.rc = None
        self.precmd = None
        self.postcmd = None


# FIXME
# if we really need a Singleton, reuse the one in interfaces.py
class Singleton(type):
    '''
    -----------------------------------------------------------------------
    Ancillary class to be used as metaclass to make other classes Singleton.
    -----------------------------------------------------------------------
    '''
    
    def __init__(cls, name, bases, dct):
        cls.__instance = None 
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw): 
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance


class ProcessCondorRequests(threading.Thread):
    '''
    class to process objects
    of class CondorRequest()
    '''

    __metaclass__ = Singleton

    def __init__(self, factory):

        self.started = False
        threading.Thread.__init__(self)
        self.stopevent = threading.Event()
        
        self.factory = factory


    def start(self):
        if not self.started:
            threading.Thread.start(self)
            self.started = True


    def run(self):

        while not self.stopevent.isSet():
            time.sleep(5) # FIXME, find a proper number. Maybe a config variable???
            ###if not self.factory.condorrequestsqueue.empty():
            ###    req = self.factory.condorrequestsqueue.get() 
            if not condorrequestsqueue.empty():
                req = condorrequestsqueue.get() 
                if req.cmd == 'condor_submit':       
                    submit(req)    

    def join(self):
        self.stopevent.set()
        threading.Thread.join(self)


    def submit(self, req):
        '''
        req is an object of class CondorRequest()
        '''
    
        cmd = req.cmd
        if req.args:
            cmd += req.args
        
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        rc = p.returncode
    
        req.out = out
        req.err = err
        req.rc = rc



#############################################################################
#               using HTCondor python bindings
#############################################################################

import htcondor
import classad
import copy

def querycondorlib(remote=None):
    ''' 
    queries condor to get a list of ClassAds objects
    We query for a few specific ClassAd attributes
    (faster than getting everything)
    
    '''
    log = logging.getLogger() # FIXME !!

    if remote:
        # FIXME: to be tested
        log.debug("querying remote pool %s" %remote)
        collector = htcondor.collector(htcondor.param['COLLECTOR_HOST'])
        scheddAd = collector.locate(condor.DaemonTypes.Schedd, remote)
        schedd = htcondor.Schedd(scheddAd) 

    # if local...
    schedd = htcondor.Schedd() # Defaults to the local schedd.
    list_attrs = ['match_apf_queue', 'jobstatus', 'ec2instanceid']
    out = schedd.query('true', list_attrs)
    out = aggregateinfolib(out) 
    log.trace(out)
    return out 

def aggregateinfolib(input):
    
    log = logging.getLogger() # FIXME !!

    emptydict = {'0' : 0,
                 '1' : 0,
                 '2' : 0,
                 '3' : 0,
                 '4' : 0,
                 '5' : 0,
                 '6' : 0}

    queues = {}
    for job in input:
       if not 'match_apf_queue' in job.keys():
           # This job is not managed by APF. Ignore...
           continue
       apfqname = job['match_apf_queue']
       if apfqname not in queues.keys():
           queues[apfqname] = copy.copy(emptydict)

       jobstatus = str(job['jobstatus'])

       queues[apfqname][jobstatus] += 1
    
    log.trace(queues)
    return queues


def querystatuslib():
    ''' 
    Equivalent to condor_status
    We query for a few specific ClassAd attributes 
    (faster than getting everything)
    Output of collector.query(htcondor.AdTypes.Startd) looks like

     [
      [ Name = "slot1@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ], 
      [ Name = "slot2@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ]
     ]
    '''
    # We only want to try to import if we are actually using the call...
    # Later on we will need to handle Condor version >7.9.4 and <7.9.4
    #

    collector = htcondor.Collector()
    list_attrs = ['Name', 'State', 'Activity']
    outlist = collector.query(htcondor.AdTypes.Startd, 'true', list_attrs)
    return outlist





##############################################################################

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)
