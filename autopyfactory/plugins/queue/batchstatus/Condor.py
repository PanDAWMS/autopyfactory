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
###from autopyfactory.info import BatchStatusInfo
###from autopyfactory.info import QueueInfo
###from autopyfactory.condorlib import querycondorlib, condor_q
from autopyfactory.mappings import map2info
import autopyfactory.utils as utils

### BEGIN TEST ###
from autopyfactory.htcondorlib import HTCondorCollector, HTCondorSchedd
from autopyfactory import info2
### END TEST ###


### BEGIN TEST ###
#
# FIXME
#
#   this is a temporary solution
#

#class Job(object):
#    def __init__(self, data_d):
#        self.data_d = data_d
#
#    def __getattr__(self, key):
#        try:
#            return int(self.data_d[key])
#        except Exception, ex:
#            return 0
#
#    def __str__(self):
#        s = "QueueInfo: pending=%d, running=%d, suspended=%d" % (self.pending,
#            self.running,
#            self.suspended)
#        return s

from autopyfactory.info2 import DataItem as Job


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
            if self.remotecollector:
                collector = HTCondorCollector(self.remotecollector)
                self.schedd = collector.getSchedd(self.remoteschedd)
            else:
                self.schedd = HTCondorSchedd()
            ### END TEST ###

        except AttributeError:
            self.condoruser = 'apf'
            self.factoryid = 'test-local'
            self.sleeptime = 10
            self.log.warning("Got AttributeError during init. We should be running stand-alone for testing.")

        self._thread_loop_interval = self.sleeptime
###        self.currentinfo = None
        self.currentnewinfo = None
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
                                          'remotewallclocktime',
                                          'qdate'
                                          ]


        self.rawdata = None

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
        self.Lock.acquire()
###        self._updateinfo()
        self._updatejobinfo()
        ### BEGIN TEST ###
        self._updatenewinfo()
        ### END TEST ###
        ### BEGIN TEST TIMESTAMP ###
        self.last_timestamp = time.time()
        ### END TEST TIMESTAMP ###
        self.Lock.release()


    def getInfo(self, queue=None):
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

        if queue:
            try:
                return self.processednewinfo_d[queue]
            except Exception:
                self.log.warning('there is no info available for queue %s. Returning an empty info object' %queue)
                return Job({})
        else:
            return self.processednewinfo_d
      

    def getrawInfo(self):
        """
        returns the raw status info as results of the queries, 
        without any further processing
        """
        return self.rawdata

    
    def _updatenewinfo(self):
        """
        Query Condor for job status, and populate  object.
        It uses the condor python bindings.
        """
        self.log.debug('Starting.')
        try:
            self.condor_q_classad_l = self.schedd.condor_q(self.condor_q_attribute_l)
            self.log.debug('output of condor_q: %s' %self.condor_q_classad_l)

            self.condor_history_classad_l = self.schedd.condor_history(self.condor_history_attribute_l)
            self.log.debug('output of condor_history: %s' %self.condor_history_classad_l)

            self.rawdata = self.condor_q_classad_l + self.condor_history_classad_l

            self.currentnewinfo = info2.StatusInfo(self.rawdata)
            # --- process the status info 
            self.processednewinfo_d = self.__process(self.currentnewinfo)

            #self.currentnewinfo = rawdata
            #self.last_timestamp = time.time()
            self.cache = {}

        except Exception as e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())
        self.log.debug('Leaving.')
    ### END TEST ###

    
    ### BEGIN TEST ###
    #
    # FIXME
    #
    #   for the time being, all hardcoded in a single method
    #
    def __process(self, info):

        from autopyfactory.info2 import IndexByKey, IndexByKeyRemap, Count

        indexbyqueue = IndexByKey('match_apf_queue')
        indexbystatus = IndexByKeyRemap ('jobstatus', self.jobstatus2info)
        count = Count()

        info = info.indexby(indexbyqueue)
        info = info.indexby(indexbystatus)
        info = info.process(count)

        # convert info into a dictionary of objects Jobs
        # this is just temporary
        raw = info.getraw()
        jobs_d = {}
        for q, data in raw.items():
            job = Job( raw[q] )
            jobs_d[q] = job
        self.log.debug('returning processed information = %s' %jobs_d)    
        return jobs_d
    ### END TEST ###

    def _updatejobinfo(self):
        '''
        Query Condor for job list.
        Return dictionary indexed by queuename, with value a List of CondorJobInfo objects. 
        '''
        self.log.debug('Starting.')
        ### BEGIN TEST ###
        #classadlist = condor_q(CondorJobInfo.jobattrs)
        classadlist = self.schedd.condor_q(CondorJobInfo.jobattrs)
        ### END  TEST ###
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
        self.__add_q_attributes(new_q_attr_l)
        self.__add_history_attributes(new_history_attr_l)
        if new_q_attr_l or new_history_attr_l:
            self._updatelib()


    def __add_q_attributes(self, new_q_attr_l):
        """
        adds new classads to be included in condor_q queries
        :param list new_q_attr_l: list of classads for condor_q
        """
        if new_q_attr_l:
            for attr in new_q_attr_l:
                if attr not in self.condor_q_attribute_l:
                    self.condor_q_attribute_l.append(attr)


    def __add_history_attributes(self, new_history_attr_l):
        """
        adds new classads to be included in condor_history queries
        :param list new_history_attr_l: list of classads for condor_history
        """
        if new_history_attr_l:
            for attr in new_history_attr_l:
                if attr not in self.condor_history_attribute_l:
                    self.condor_history_attribute_l.append(attr)
            




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


