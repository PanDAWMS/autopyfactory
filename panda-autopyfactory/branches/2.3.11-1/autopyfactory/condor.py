#!/usr/bin/env python
'''
   Condor-related common utilities and library for AutoPyFactory.
   Focussed on properly processing output of condor_q -xml and condor_status -xml and converting
   to native Python data structures. 

'''
import os
import sys
import signal
import subprocess
import commands
import subprocess
import logging
import time
import traceback
import xml.dom.minidom

import autopyfactory.utils as utils
from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure

from datetime import datetime
from pprint import pprint


def querycondorlib():
    '''
    queries condor to get a list of ClassAds objects
    We query for a few specific ClassAd attributes
    (faster than getting everything)
    '''
    
    # We only want to try to import if we are actually using the call...
    # Later on we will need to handle Condor version >7.9.4 and <7.9.4
    #
    import htcondor
    import classad
    
    schedd = htcondor.Schedd() # Defaults to the local schedd.
    list_attrs = ['match_apf_queue', 'globusstatus', 'jobstatus', 'ec2instanceid']
    outlist = schedd.query('true', list_attrs)
    return outlist


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
    import htcondor
    import classad

    collector = htcondor.Collector()
    list_attrs = ['Name', 'State', 'Activity']
    outlist = collector.query(htcondor.AdTypes.Startd, 'true', list_attrs)
    return outlist


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

    log = logging.getLogger()
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
    log = logging.getLogger()
    (s,o) = commands.getstatusoutput('condor_version')
    if s == 0:
        log.debug('Condor version is: \n%s' % o )       
        CONDOR_CONFIG = os.environ.get('CONDOR_CONFIG', None)
        if CONDOR_CONFIG:
            log.debug('Environment variable CONDOR_CONFIG set to %s' %CONDOR_CONFIG)
        else:
            log.debug("Condor config is: \n%s" % commands.getoutput('condor_config_val -config'))
    else:
        log.error('checkCondor() has been called, but not Condor is available on system.')
        raise ConfigFailure("No Condor available on system.")


def statuscondor(queryargs = None):
    '''
    Return info about job startd slots. 
    '''
    log = logging.getLogger()
    cmd = 'condor_status -xml '
    if queryargs:
        cmd += queryargs
    log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)
    log.info('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s out=%s' %(p.returncode, err, out ))
        out = None
    return out

def statuscondormaster(queryargs = None):
    '''
    Return info about masters. 
    '''
    log = logging.getLogger()
    cmd = 'condor_status -master -xml '
    if queryargs:
        cmd += queryargs
    
    log.debug('Querying cmd = %s' % cmd.replace('\n','\\n'))
    #log.debug('Querying cmd = %s' % cmd)
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)
    log.info('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
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

    log = logging.getLogger()
    log.debug('Starting.')
    querycmd = "condor_q "
    log.debug('_querycondor: using executable condor_q in PATH=%s' %utils.which('condor_q'))


    # adding extra query args from queues.conf
    if queryargs:
        querycmd += queryargs 

    querycmd += " -format ' MATCH_APF_QUEUE=%s' match_apf_queue"
    querycmd += " -format ' JobStatus=%d\n' jobstatus"
    querycmd += " -format ' GlobusStatus=%d\n' globusstatus"
    querycmd += " -xml"

    log.debug('Querying cmd = %s' %querycmd.replace('\n','\\n'))

    before = time.time()          
    p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.info('condor_q: %s seconds to perform the query' %delta)

    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        # lets try again. Sometimes RC!=0 does not mean the output was bad
        if out.startswith('<?xml version="1.0"?>'):
            log.warning('RC was %s but output is still valid' %p.returncode)
        else:
            log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
    log.trace('_querycondor: Out is %s' % out)
    log.debug('_querycondor: Leaving.')
    return out
    


def querycondorxml(queryargs=None):
    '''
    Return human readable info about startds. 
    '''
    log = logging.getLogger()
    cmd = 'condor_q -xml '

    # adding extra query args from queues.conf
    if queryargs:
        querycmd += queryargs 
       
    log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)
    log.info('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    log.trace('Out is %s' % out)
    log.debug('Leaving.')
    return out


def xml2nodelist(input):
    log = logging.getLogger()
    xmldoc = xml.dom.minidom.parseString(input).documentElement
    nodelist = []
    for c in listnodesfromxml(xmldoc, 'c') :
        node_dict = node2dict(c)
        nodelist.append(node_dict)
    log.debug('_parseoutput: Leaving and returning list of %d entries.' %len(nodelist))
    log.info('Got list of %d entries.' %len(nodelist))
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

    log=logging.getLogger()
    log.debug('Starting.')                

    # first convert the XML output into a list of XML docs
    outputs = _out2list(output)

    nodelist = []
    for output in outputs:
        xmldoc = xml.dom.minidom.parseString(output).documentElement
        for c in listnodesfromxml(xmldoc, 'c') :
            node_dict = node2dict(c)
            nodelist.append(node_dict)            
    log.info('Got list of %d entries.' %len(nodelist))       
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
    dic = {}
    for child in node.childNodes:
        if child.nodeType == child.ELEMENT_NODE:
            key = child.attributes['n'].value
            if len(child.childNodes[0].childNodes) > 0:
                value = child.childNodes[0].firstChild.data
                dic[key.lower()] = str(value)
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
    log=logging.getLogger()
    log.debug('Starting with list of %d items.' % len(input))
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
                   
    log.debug('Aggregate: output is %s ' % queues)  # this could be trace() instead of debug()
    log.info('Aggregate: Created dict with %d queues.' % len(queues))
    return queues

  

def getJobInfo():
    log = logging.getLogger()
    xml = querycondorxml()
    nl = xml2nodelist(xml)
    log.info("Got node list of length %d" % len(nl))
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
            
    log.info("Made job list of length %d" % len(joblist))
    log.info("Made a job info dict of length %d" % len(qd))
    return qd


def getStartdInfoByEC2Id():
    log = logging.getLogger()
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
            #log.debug("Created csdi: %s" % j)
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
    log = logging.getLogger()
    idstring = ' '.join(idlist)
    cmd = 'condor_rm %s' % idstring
    log.debug('Issuing remove cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the command' %delta)
    log.info('%s seconds to perform the command' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)
