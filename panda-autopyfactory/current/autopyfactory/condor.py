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
import htcondor
import classad

import autopyfactory.utils as utils


from datetime import datetime
from pprint import pprint


__author__ = "John Hover"
__copyright__ = "2013, John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero, John Hover"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"



def querycondorlib():
    '''
    queries condor to get a list of ClassAds objects
    We query for a few specific ClassAd attributes
    (faster than getting everything)
    '''
    schedd = htcondor.Schedd() # Defaults to the local schedd.
    outlist = schedd.query('true', ['match_apf_queue', 'globusstatus', 'jobstatus', 'ec2instanceid'])

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



def checkCondor():
    '''
    Perform sanity check on condor environment.
    Does condor_q exist?
    Is Condor running?
    '''
    
    # print condor version
    log = logging.getLogger()
    log.debug('Condor version is: \n%s' % commands.getoutput('condor_version'))       

    # check env var $CONDOR_CONFIG
    CONDOR_CONFIG = os.environ.get('CONDOR_CONFIG', None)
    if CONDOR_CONFIG:
        log.debug('Environment variable CONDOR_CONFIG set to %s' %CONDOR_CONFIG)
    else:
        log.debug("Condor config is: \n%s" % commands.getoutput('condor_config_val -config'))
    


def statuscondor():
    '''
    Return human readable info about startds. 
    '''
    log = logging.getLogger()
    cmd = 'condor_status -xml'
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
    return out


def querycondor():
    '''
    Query condor for specific job info and return xml representation string
    for further processing.
    
    '''
    log = logging.getLogger()
    log.debug('Starting.')
    querycmd = "condor_q "
    log.debug('_querycondor: using executable condor_q in PATH=%s' %utils.which('condor_q'))

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
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    log.debug('_querycondor: Leaving. Out is %s' % out)
    return out


def querycondorxml():
    '''
    Return human readable info about startds. 
    '''
    log = logging.getLogger()
    cmd = 'condor_q -xml'
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
    #log.debug('Leaving. Out is %s' % out)
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
    '''
    log=logging.getLogger()
    log.debug('Starting.')                
    xmldoc = xml.dom.minidom.parseString(output).documentElement
    nodelist = []
    for c in listnodesfromxml(xmldoc, 'c') :
        node_dict = node2dict(c)
        nodelist.append(node_dict)            
    log.info('Got list of %d entries.' %len(nodelist))
    return nodelist


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
    

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)