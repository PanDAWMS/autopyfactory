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

from autopyfactory.interfaces import WMSStatusInterface, _thread

from autopyfactory.info import CloudInfo
from autopyfactory.info import SiteInfo
from autopyfactory.info import JobInfo
from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import WMSQueueInfo

###from autopyfactory.condor import checkCondor
###from autopyfactory.condorlib import querycondorlib
from autopyfactory.mappings import map2info

import autopyfactory.htcondorlib
import autopyfactory.info2


### BEGIN TEST ###
#
# FIXME
#
#   this is a temporary solution
#

class Job(object):
    def __init__(self, data_d):
        self.data_d = data_d

    def __getattr__(self, key):
        try:
            return int(self.data_d[key])
        except Exception, ex:
            return 0

    def __str__(self):
        s = "WMSQueueInfo: notready=%s, ready=%s, running=%s, done=%s, failed=%s, unknown=%s" %\
            (self.notready,
             self.ready,
             self.running,
             self.done,
             self.failed,
             self.unknown
            )
        return s





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
        #try:
        _thread.__init__(self) 
        apfqueue.factory.threadsregistry.add("plugin", self)
        
        self.log = logging.getLogger('autopyfactory.wmsstatus.%s' %apfqueue.apfqname)
        self.log.debug('Initializing object...')

        self.apfqueue = apfqueue   
        self.apfqname = apfqueue.apfqname
        #self.condoruser = apfqueue.fcl.get('Factory', 'factoryUser')
        #self.factoryid = apfqueue.fcl.get('Factory', 'factoryId') 
        self.sleeptime = self.apfqueue.fcl.getint('Factory', 'wmsstatus.condor.sleep')
        self._thread_loop_interval = self.sleeptime
        self.maxage = self.apfqueue.fcl.generic_get('Factory', 'wmsstatus.condor.maxage', default_value=360)
        self.scheddhost = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.scheddhost', default_value='localhost')
        self.scheddport = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.scheddport', default_value=9618 )
        self.collectorhost = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.collectorhost', default_value='localhost') 
        self.collectorport = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.collectorport', default_value=9618 )
        self.password_file = self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.password_file')
        self.queryargs = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queryargs')
        self.queueskey = self.apfqueue.qcl.generic_get(self.apfqname, 'wmsstatus.condor.queueskey', default_value='ANY')

        #if self.queryargs:
        #    l = self.queryargs.split()  # convert the string into a list
        #    if '-name' in l:
        #        self.scheddhost = l[l.index('-name') + 1]
        #    if '-pool' in l:
        #        self.collectorhost = l[l.index('-pool') + 1]
    
        if self.collectorhost != 'localhost':
            _collector = htcondorlib.HTCondorCollector(self.collectorhost, self.collectorport)
            self.schedd = _collector.getSchedd(self.scheddhost, self.scheddport)
        else:
            self.schedd = htcondorlib.Schedd()

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
        ###self.jobstatus2info = {'0': 'ready',
        ###                       '1': 'ready',
        ###                       '2': 'running',
        ###                       '3': 'done',
        ###                       '4': 'done',
        ###                       '5': 'failed',
        ###                       '6': 'running'}

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        #checkCondor()
        self.log.debug('condor_version : %s' %htcondorlib.condor_version())
        self.log.debug('condor_config file : %s' %htcondorlib.condor_config_files())
        self.log.info('WMSStatusPlugin: Object initialized.')


    def _run(self):
        """
        Main loop
        """
        self.log.debug('Starting')
###        self._update()
        self._updatenewinfo()
        self.log.debug('Leaving')


###    #def getInfo(self, queue=None, maxtime=0):
###    def getOldInfo(self, queue=None, maxtime=0):
###        """
###        Returns a BatchStatusInfo object populated by the analysis 
###        over the output of a condor_q command
###
###        Optionally, a maxtime parameter can be passed.
###        In that case, if the info recorded is older than that maxtime,
###        None is returned, as we understand that info is too old and 
###        not reliable anymore.
###        """           
###        self.log.debug('Starting with maxtime=%s' % maxtime)
###        
###        if self.currentjobinfo is None:
###            self.log.debug('Not initialized yet. Returning None.')
###            return None
###        elif maxtime > 0 and (int(time.time()) - self.currentjobinfo.lasttime) > maxtime:
###            self.log.debug('Info too old. Leaving and returning None.')
###            return None
###        else:
###            if queue:
###                return self.currentjobinfo[queue]                    
###            else:
###                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentjobinfo))
###                return self.currentjobinfo


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
                except Exception, ex:
                    self.log.warning('there is no info available for queue %s. Returning an empty info object' %queue)
                    return Job({})
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
       
        #
        # FIXME: temporary solution 
        #        only works is input site is not None, so a single item is expected to be returned 
        #        as opposite to a dictionary
        #
        ###if self.currentsiteinfo is None:
        ###    self.log.debug('Not initialized yet. Returning None.')
        ###    return None
        ###elif maxtime > 0 and (int(time.time()) - self.currentsiteinfo.lasttime) > maxtime:
        ###    self.log.debug('Info too old. Leaving and returning None.')
        ###    return None
        ###else:
        ###    if site:
        ###        return self.currentsiteinfo[queue]                    
        ###    else:
        ###        self.log.debug('Leaving and returning info of %d entries.' % len(self.currentsiteinfo))
        ###        return self.currentsiteinfo
        
        si = SiteInfo()
        si.status = "ok"
        return si


###    def _update(self):
###        """        
###        Query Condor for job status, validate ?, and populate BatchStatusInfo object.
###        Condor-G query template example:
###        
###        condor_q -constr '(owner=="apf") && stringListMember("PANDA_JSID=BNL-gridui11-jhover",Environment, " ")'
###                 -format 'jobStatus=%d ' jobStatus 
###                 -format 'globusStatus=%d ' GlobusStatus 
###                 -format 'gkUrl=%s' MATCH_gatekeeper_url
###                 -format '-%s ' MATCH_queue 
###                 -format '%s\n' Environment
###
###        NOTE: using a single backslash in the final part of the 
###              condor_q command '\n' only works with the 
###              latest versions of condor. 
###              With older versions, there are two options:
###                      - using 4 backslashes '\\\\n'
###                      - using a raw string and two backslashes '\\n'
###
###        The JobStatus code indicates the current Condor status of the job.
###        
###                Value   Status                            
###                0       U - Unexpanded (the job has never run)    
###                1       I - Idle                                  
###                2       R - Running                               
###                3       X - Removed                              
###                4       C -Completed                            
###                5       H - Held                                 
###                6       > - Transferring Output
###
###        The GlobusStatus code is defined by the Globus GRAM protocol. Here are their meanings:
###        
###                Value   Status
###                1       PENDING 
###                2       ACTIVE 
###                4       FAILED 
###                8       DONE 
###                16      SUSPENDED 
###                32      UNSUBMITTED 
###                64      STAGE_IN 
###                128     STAGE_OUT 
###        """
###
###        self.log.debug('Starting.')
###        
###        # These are not meaningful for Local Condor as WMS
###        self.currentcloudinfo = None
###        self.currentsiteinfo = None
###
###        try:
###
###            #### BEGIN TEST ####
###            ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FIXME##
###            #strout = querycondorlib(self.queryargs, self.queueskey)
###            if self.queueskey.lower() != "any":
###                strout = querycondorlib(remotecollector=self.collectorhost, remoteschedd=self.scheddhost, queueskey=None)
###            else:
###                strout = querycondorlib(remotecollector=self.collectorhost, remoteschedd=self.scheddhost, queueskey=self.queueskey )
###            # FIXME: the extra_attributes is missing !!
###            #### END TEST ####
###            
###            if not strout:
###                self.log.warning('output of _querycondor is an empty dictionary. Nothing to be done. Skip to next loop.') 
###            else:
###                newjobinfo = map2info(strout, WMSStatusInfo(), self.jobstatus2info)
###                self.log.info("Replacing old info with newly generated info.")
###                self.currentjobinfo = newjobinfo
###        except Exception, e:
###            self.log.error("Exception: %s" % str(e))
###            self.log.debug("Exception: %s" % traceback.format_exc())            
###
###        self.log.debug('_ Leaving.')



    def _updatenewinfo(self):
        """
        """
        self.log.debug('Starting.')
        try:
            self.condor_q_classad_l = self.schedd.condor_q(self.condor_q_attribute_l)
            self.log.debug('output of condor_q: %s' %self.condor_q_classad_l)
        
            self.rawdata = self.condor_q_classad_l

            self.currentnewinfo = info2.StatusInfo(self.rawdata)

            # --- process the status info 
            self.processednewinfo_d = self.__process(self.currentnewinfo)

        except Exception, ex:
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
        
        id = 'local'
        if conf.generic_get(section, 'wmsstatusplugin') == 'Condor':
            queryargs = conf.generic_get(section, 'wmsstatus.condor.queryargs')
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
 
   



def test():
    list =  [ { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '2' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '1' },
                           { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_1',
                'jobStatus' : '1' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '1' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '2' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' },
              { 'MATCH_APF_QUEUE' : 'BNL_ATLAS_2',
                'jobStatus' : '3' }
            ] 
    
if __name__=='__main__':
    pass



