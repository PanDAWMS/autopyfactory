#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#
import commands
import subprocess
import logging
import os
import sys
import time
import threading
import traceback
import xml.dom.minidom

from datetime import datetime
from pprint import pprint
from autopyfactory.interfaces import BatchStatusInterface, _thread
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo
from autopyfactory.condorlib import querycondorlib, condor_q
from autopyfactory.mappings import map2info
import autopyfactory.utils as utils



### BEGIN TEST ###
from autopyfactory.condorlib import HTCondorPool
from autopyfactory import info2
### END TEST ###



class CondorJobInfo(object):
    """
    This object represents a Condor job.     
        
    """
    jobattrs = ['match_apf_queue',
                'clusterid',
                'procid',
                'qdate', 
                'ec2instancename',
                'ec2instancetype',
                'enteredcurrentstatus',
                'jobstatus',
                'ec2remotevirtualmachinename',
                'ec2securitygroups',
                'ec2spotprice',
                'gridjobstatus',
                 ]  


    def __init__(self, dict):
        """
        Creates CondorJobInfo object from arbitrary dictionary of attributes. 
        
        """
        self.log = logging.getLogger('autopyfactory.batchstatus')
        self.jobattrs = []
        for k in dict.keys():
            self.__setattr__(k,dict[k])
            self.jobattrs.append(k)
        self.jobattrs.sort()
        #self.log.debug("Made CondorJobInfo object with %d attributes" % len(self.jobattrs))    
        
    def __str__(self):
        s = "CondorJobInfo: %s.%s " % (self.clusterid, 
                                      self.procid)
        #for k in CondorJobInfo.jobattrs:
        #    s += " %s=%s " % ( k, self.__getattribute__(k))
        for k, v in self.__dict__.items():
            s += " %s=%s " % ( k, v)
        return s
    
    def __repr__(self):
        s = str(self)
        return s


class _condor(_thread, BatchStatusInterface):
    """
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    """
    def __init__(self, apfqueue, config, section):
        _thread.__init__(self)
        apfqueue.factory.threadsregistry.add("plugin", self)      
        self.log = logging.getLogger('autopyfactory.batchstatus.%s' %apfqueue.apfqname)
        self.log.debug('BatchStatusPlugin: Initializing object...')

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname
        
        try:
            self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
            self.factoryid = apfqueue.fcl.get('Factory', 'factoryId')
            self.maxage = apfqueue.fcl.generic_get('Factory', 'batchstatus.condor.maxage', default_value=360) 
            self.sleeptime = self.apfqueue.fcl.getint('Factory', 'batchstatus.condor.sleep')
            self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'batchstatus.condor.queryargs') 

            ### BEGIN TEST ###
            self.remoteschedd = None
            self.remotecollector = None
            if self.queryargs:
                l = self.queryargs.split()  # convert the string into a list
                if '-name' in l:
                    self.remoteschedd = l[l.index('-name') + 1]
                if '-pool' in l:
                    self.remotecollector = l[l.index('-pool') + 1]
            ### END TEST ###            

            ### BEGIN TEST ###
            self.htcondor = HTCondorPool(self.remotecollector, self.remoteschedd)
            ### END TEST ###

        except AttributeError:
            self.condoruser = 'apf'
            self.factoryid = 'test-local'
            self.sleeptime = 10
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")

        self._thread_loop_interval = self.sleeptime
        self.currentinfo = None
        self.jobinfo = None              
        self.last_timestamp = 0

        # mappings
        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORBATCHSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)

        # query attributes
        self.condor_q_attribute_l = ['match_apf_queue', 
                                     'jobstatus'
                                    ]
        self.condor_history_attribute_l = ['match_apf_queue', 
                                          'jobstatus', 
                                          'enteredcurrentstatus', 
                                          'remotewallclocktimeqdate'
                                          ]


        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        self.log.info('BatchStatusPlugin: Object initialized.')


    def _run(self):
        """
        Main loop
        """
        self.log.debug('Starting')
        self._updatelib()
        self.log.debug('Leaving')


    def getInfo(self, queue=None):
        """
        Returns a  object populated by the analysis 
        over the output of a condor_q command

        If the info recorded is older than that maxage,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        """           
        self.log.debug('Starting with self.maxage=%s' % self.maxage)
        
        if self.currentinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif self.maxage > 0 and (int(time.time()) - self.currentinfo.lasttime) > self.maxage:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                self.log.debug('Current info is %s' % self.currentinfo)                    
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentinfo))
                return self.currentinfo[queue]
            else:
                self.log.debug('Current info is %s' % self.currentinfo)
                self.log.debug('No queue given, returning entire BatchStatusInfo object')
                return self.currentinfo


    def getJobInfo(self, queue=None):
        """
        Returns a  object populated by the analysis 
        over the output of a condor_q command

        If the info recorded is older than that maxage,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        """           
        self.log.debug('Starting with self.maxage=%s' % self.maxage)
        
        if self.jobinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        else:
            if queue:
                self.log.debug('Current info is %s' % self.jobinfo)                    
                self.log.debug('Leaving and returning info of %d entries.' % len(self.jobinfo))
                return self.jobinfo[queue]
            else:
                self.log.debug('Current info is %s' % self.jobinfo)
                self.log.debug('No queue given, returning entire BatchStatusInfo object')
                return self.jobinfo

    
    def _updatelib(self):
        self._updateinfo()
        self._updatejobinfo()
        ### BEGIN TEST ###
        self._updatenewinfo()
        ### END TEST ###
        ### BEGIN TEST TIMESTAMP ###
        self.last_timestamp = time.time()
        ### END TEST TIMESTAMP ###

        
    ### BEGIN TEST ###
    def getnewInfo(self, algorithm=None):
        """
        Returns a  object populated by the analysis 
        over the output of a condor_q command

        If the info recorded is older than that maxage,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        """           
        self.log.debug('Starting with self.maxage=%s' % self.maxage)
        
        if self.currentnewinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None

        if self.maxage > 0 and\
           (int(time.time()) - self.currentnewinfo.timestamp) > self.maxage:
            self.log.debug('Info too old. Leaving and returning None.')
            return None

        if not algorithm:
            self.log.debug('Returning current info data as it is.')
            return self.currentnewinfo
          
        if algorithm not in self.cache.keys():
            # FIXME !!
            # this trick does not really work
            # 2 instances of class Algorithm, 
            # even though they host the same sequence of Analyzers, 
            # they are different objects, and therefore 2 different keys
            self.log.debug('There is not processed data in the cache for algorithm. Calculating it.')
            out = algorithm.analyze(self.currentnewinfo)
            self.cache[algorithm] = out
        self.log.debug('Returning processed data.')
        return self.cache[algorithm]
        

    
    def _updatenewinfo(self):
        """
        Query Condor for job status, and populate  object.
        It uses the condor python bindings.
        """
        self.log.debug('Starting.')
        try:
            #condor_q_attribute_l = ['match_apf_queue', 
            #                        'jobstatus'
            #                       ]
            condor_q_classad_l = self.htcondor.condor_q(self.condor_q_attribute_l)
            self.log.debug('output of condor_q: %s' %condor_q_classad_l)

            #condor_history_attribute_l = ['match_apf_queue', 
            #                              'jobstatus', 
            #                              'enteredcurrentstatus', 
            #                              'remotewallclocktimeqdate'
            #                             ]
            condor_history_classad_l = self.htcondor.condor_history(self.condor_history_attribute_l)
            self.log.debug('output of condor_history: %s' %condor_history_classad_l)

            rawdata = condor_q_classad_l + condor_history_classad_l

            self.currentnewinfo = info2.StatusInfo(rawdata)

            self.cache = {}

        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())
        self.log.debug('Leaving.')
    ### END TEST ###

        
    def _updateinfo(self):
        """
        Query Condor for job status, and populate  object.
        It uses the condor python bindings.
        """
        self.log.debug('Starting.')
        try:
            strout = querycondorlib(self.remotecollector, 
                                    self.remoteschedd, 
                                    )
            self.log.debug('output of querycondorlib : %s' %strout)
            if strout is None:
                self.log.warning('output of _querycondor is not valid. Not parsing it. Skip to next loop.')
            else:
                newinfo = map2info(strout, BatchStatusInfo(), self.jobstatus2info)
                self.log.info("Replacing old info with newly generated info.")
                self.currentinfo = newinfo
        except Exception, e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())
        self.log.debug('Leaving.')


    def _updatejobinfo(self):
        '''
        Query Condor for job list.
        Return dictionary indexed by queuename, with value a List of CondorJobInfo objects. 
        '''
        self.log.debug('Starting.')
        classadlist = condor_q(CondorJobInfo.jobattrs)
        newjobinfo = {}

        for ca in classadlist:
            if 'match_apf_queue' in ca.keys(): 
                ji = CondorJobInfo(ca)            
                try:
                    ql = newjobinfo[ji.match_apf_queue]
                    ql.append(ji)
                except KeyError:
                    newjobinfo[ji.match_apf_queue] = [ ji]
        self.log.debug("Created jobinfo list of %s items" % len(newjobinfo))
        self.log.info("Replacing old info with newly generated info.")
        self.jobinfo = newjobinfo
        self.log.debug('Leaving.')

    def add_query_attributes(self, new_q_attr_l=None, new_history_attr_l=None):
        """
        adds new classads to be included in condor queries
        :param list new_q_attr_l: list of classads for condor_q
        :param list new_history_attr_l: list of classads for condor_history
        """
        if new_q_attr_l:
            self.condor_q_attribute_l += new_q_attr_l
        if new_history_attr_l:
            self.condor_history_attribute_l += new_history_attr_l
        self._updatelib()



# =============================================================================
#       Singleton wrapper
# =============================================================================
class Condor(object):
   
    instances = {}

    def __new__(cls, *k, **kw): 

        # ---------------------------------------------------------------------
        # get the ID
        apfqueue = k[0]
        conf = k[1]
        section = k[2]
        
        id = 'local'
        if conf.generic_get(section, 'batchstatusplugin') == 'Condor':
            queryargs = conf.generic_get(section, 'batchstatus.condor.queryargs')
            if queryargs:
                l = queryargs.split()  # convert the string into a list
                                       # e.g.  ['-name', 'foo', '-pool', 'bar'....]
                name = ''
                pool = ''
        
                if '-name' in l:
                    name = l[l.index('-name') + 1]
                if '-pool' in l:
                    pool = l[l.index('-pool') + 1]
        
                if name == '' and pool == '':
                    id = 'local'
                else:
                    id = '%s:%s' %(name, pool)
        # ---------------------------------------------------------------------

        if not id in Condor.instances.keys():
            Condor.instances[id] = _condor(*k, **kw)
        return Condor.instances[id]



###############################################################################

def test1():
    from autopyfactory.test import MockAPFQueue
    
    a = MockAPFQueue('BNL_CLOUD-ec2-spot')
    bsp = CondorBatchStatusPlugin(a, condor_q_id='local')
    bsp.start()
    while True:
        try:
            time.sleep(15)
        except KeyboardInterrupt:
            bsp.stopevent.set()
            sys.exit(0)    


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test1()


