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
    #log.debug('Leaving. Out is %s' % out)
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

def listnodesfromxml( xmldoc, tag):
    return xmldoc.getElementsByTagName(tag)


def node2dict( node):
    '''
 
    '''
    dic = {}
    for child in node.childNodes:
        if child.nodeType == child.ELEMENT_NODE:
            key = child.attributes['n'].value
            if len(child.childNodes[0].childNodes) > 0:
                value = child.childNodes[0].firstChild.data
                dic[key.lower()] = str(value)
    return dic
   

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