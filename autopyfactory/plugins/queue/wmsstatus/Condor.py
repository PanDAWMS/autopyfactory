#!/bin/env python
#
# AutoPyfactory batch status plugin for Condor
#

import subprocess
import logging
import time
import threading
import traceback
import xml.dom.minidom

from autopyfactory.info import SiteInfo
from autopyfactory.interfaces import WMSStatusInterface, _thread

from libfactory.htcondorlib import HTCondorCollector, HTCondorSchedd, condor_version, condor_config_files
from libfactory.info import StatusInfo, IndexByKey, IndexByKeyRemap, Count, AnalyzerTransform
from libfactory.info import DataItem as Job


class CreateANY(AnalyzerTransform):
    """
    duplicates the list of jobs, 
    adding a class MATCH_APF_QUEUE=ANY to the new ones
    """
    def transform(self, job_l):
        new_job_l = []
        for job in job_l:
            new_job = copy.copy(job)
            new_job['match_apf_queue'] = 'ANY'
            new_job_l.append(job)
            new_job_l.append(new_job)
        return new_job_l


class _condor(_thread, WMSStatusInterface):
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
        
        self.log = logging.getLogger('autopyfactory.wmsstatus.%s' %apfqueue.apfqname)
        self.log.debug('Initializing object...')

        self.apfqueue = apfqueue   
        self.apfqname = apfqueue.apfqname
        self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.condor.sleep')
        self._thread_loop_interval = self.sleeptime
        self.maxage = self.apfqueue.fcl.generic_get('Factory', 'wmsstatus.condor.maxage', default_value=360)
        self.scheddhost = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.scheddhost', default_value='localhost')
        self.scheddport = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.scheddport', default_value=9618 )
        self.collectorhost = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.collectorhost', default_value='localhost') 
        self.collectorport = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.collectorport', default_value=9618 )
        #self.password_file = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.password_file')
        ###self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queryargs')
        self.queueskey = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queueskey', default_value='ANY')

        if self.collectorhost != 'localhost':
            _collector = HTCondorCollector(self.collectorhost, self.collectorport)
            self.schedd = _collector.getSchedd(self.scheddhost, self.scheddport)
        else:
            self.schedd = HTCondorSchedd()

        self.condor_q_attribute_l = ['match_apf_queue', 
                                     'jobstatus'
                                    ]

        # FIXME
        # check if this works with a Singleton, or I need a different Singleton per value

        self.currentcloudinfo = None
        self.currentjobinfo = None
        self.currentsiteinfo = None
        self.rawdata = None
        self.currentnewinfo = None
        self.processednewinfo_d = None
              

        # ================================================================
        #                     M A P P I N G S 
        # ================================================================
        
        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('CONDORWMSSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        self.log.debug('condor_version : %s' %condor_version())
        self.log.debug('condor_config file : %s' %condor_config_files())
        self.log.info('WMSStatusPlugin: Object initialized.')


    def _run(self):
        """
        Main loop
        """
        self.log.debug('Starting')
        self._updatenewinfo()
        self.log.debug('Leaving')


    def getInfo(self, queue=None, maxtime=0):
        """
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a condor_q command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        """           
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentnewinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None

        elif maxtime > 0 and (int(time.time()) - self.currentnewinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None

        else:
            if queue:
                try:
                    return self.processednewinfo_d[queue]
                except Exception:
                    self.log.warning('there is no info available for queue %s. Returning an empty info object' % queue)
                    return Job()
            else:
                return self.processednewinfo_d



    def getCloudInfo(self, cloud=None, maxtime=0):
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentcloudinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentcloudinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if cloud:
                return self.currentcloudinfo[queue]                    
            else:
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentcloudinfo))
                return self.currentcloudinfo


    def getSiteInfo(self, site=None, maxtime=0):
        self.log.debug('Starting with maxtime=%s' % maxtime)
        si = SiteInfo()
        si.status = "ok"
        return si


    def _updatenewinfo(self):
        """
        """
        self.log.debug('Starting.')
        try:
            self.condor_q_classad_l = self.schedd.condor_q(self.condor_q_attribute_l)
            self.log.debug('output of condor_q: %s' %self.condor_q_classad_l)
        
            self.rawdata = self.condor_q_classad_l

            self.currentnewinfo = StatusInfo(self.rawdata)

            # --- process the status info 
            self.processednewinfo_d = self.__process(self.currentnewinfo)

        except Exception as ex:
            self.log.error("Exception: %s" % str(ex))
            self.log.debug("Exception: %s" % traceback.format_exc())

        self.log.debug('Leaving.')

    
    ### BEGIN TEST ###
    #
    # FIXME
    #
    #   for the time being, all hardcoded in a single method
    #
    def __process(self, info):
 
        indexbyqueue = IndexByKey('match_apf_queue')
        indexbystatus = IndexByKeyRemap ('jobstatus', self.jobstatus2info)
        count = Count()
        createany = CreateANY()
 
        info = info.transform(createany)
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
        return jobs_d
    ### END TEST ###


    def add_query_attributes(self, new_q_attr_l=None):
        """
        adds new classads to be included in the condor_q query
        :param list new_q_attr_l: list of classads for the query
        """
        self.__add_q_attributes(new_q_attr_l)


    def __add_q_attributes(self, new_q_attr_l):
        """
        adds new classads to be included in condor_q queries
        :param list new_q_attr_l: list of classads for condor_q
        """
        if new_q_attr_l:
            for attr in new_q_attr_l:
                if attr not in self.condor_q_attribute_l:
                    self.condor_q_attribute_l.append(attr)




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
        
        #id = 'local'
        #if conf.generic_get(section, 'wmsstatusplugin') == 'Condor':
        #    queryargs = conf.generic_get(section, 'wmsstatus.condor.queryargs')
        #    if queryargs:
        #        l = queryargs.split()  # convert the string into a list
        #                               # e.g.  ['-name', 'foo', '-pool', 'bar'....]
        #        name = ''
        #        pool = ''
        #
        #        if '-name' in l:
        #            name = l[l.index('-name') + 1]
        #        if '-pool' in l:
        #            pool = l[l.index('-pool') + 1]
        #
        #        if name == '' and pool == '':
        #            id = 'local'
        #        else:
        #            id = '%s:%s' %(name, pool)
     
        scheddhost = conf.generic_get(section, 'wmsstatus.condor.scheddhost', default_value='localhost')
        scheddport = conf.generic_get(section, 'wmsstatus.condor.scheddport', default_value=9618 )
        collectorhost = conf.generic_get(section, 'wmsstatus.condor.collectorhost', default_value='localhost') 
        collectorport = conf.generic_get(section, 'wmsstatus.condor.collectorport', default_value=9618 )
        id = '%s:%s:%s:%s' %(scheddhost, scheddport, collectorhost, collectorport)

        if not id in Condor.instances.keys():
            Condor.instances[id] = _condor(*k, **kw)
        return Condor.instances[id]
