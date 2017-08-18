#!/bin/env python
#
# AutoPyfactory batch status plugin for Work Queue
#

import subprocess
import logging
import time
import threading
import traceback

import json
import os
import re
import sys

from autopyfactory.interfaces import WMSStatusInterface, _thread

from autopyfactory.info import WMSStatusInfo
from autopyfactory.info import WMSQueueInfo

from autopyfactory.mappings import map2info

try:
    import infoclient
except ImportError:
    pass

class WorkQueueCatalog(object):

    instances = {}

    def __new__(cls, *k, **kw):

        section_name = k[0].apfqname

        if section_name not in WorkQueueCatalog.instances:
            WorkQueueCatalog.instances[section_name] = _WorkQueueCatalog(*k, **kw)

        return WorkQueueCatalog.instances[section_name]



class _WorkQueueCatalog(_thread,  WMSStatusInterface):
    '''
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from WMSStatusInterface
    -----------------------------------------------------------------------
    '''

    def __init__(self, apfqueue, config, section):
        _thread.__init__(self)
        apfqueue.factory.threadsregistry.add("plugin", self)

        self.log = logging.getLogger("main.wmsstatusplugin[WorkQueueCatalog created for masters '%s']" %(apfqueue.apfqname,))
        self.log.debug('Initializing object...')

        self.apfqueue = apfqueue
        self.apfqname = apfqueue.apfqname

        self.mastername = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.mastername', default_value = 'APF_master_.*')
        self.status_exe = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.exe.status', default_value = 'work_queue_status')

        self.sleeptime  = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.sleep',  'getint', default_value = 30)
        self._thread_loop_interval = self.sleeptime

        use_infoclient = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.infoclient.enabled', default_value = False)

        self.catalog    = None
        self.infoclient = None
        if use_infoclient:
            if 'infoclient' in sys.modules:
                self.requestid  = self.apfqueue.fcl.generic_get('Factory', 'requestid')
                self.infoclient = infoclient.InfoClient(self.apfqueue.fcl)
            else:
                raise Exception('infoclient requested, but module was not loaded.')
        else:
            self.catalog = self.apfqueue.qcl.generic_get(self.apfqname, 'workqueue.catalog', None) 
            if not self.catalog:
                self.catalog = 'catalog.cse.nd.edu:9097'
            
        # ================================================================
        #                     M A P P I N G S 
        # ================================================================
        
        self.jobstatus2info = self.apfqueue.factory.mappingscl.section2dict('WORKQUEUEWMSSTATUS-JOBSTATUS2INFO')
        self.log.info('jobstatus2info mappings are %s' %self.jobstatus2info)

        # variable to record when was last time info was updated
        # the info is recorded as seconds since epoch
        self.lasttime = 0
        self.log.info('WMSStatusPlugin: Object initialized.')

    def _run(self):
        '''
        Main loop
        '''
        self.log.debug('Starting')
        self._update()
        self.log.debug('Leaving')


    def getInfo(self, queue=None, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a work_queue_status command.

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('Starting with maxtime=%s' % maxtime)
        
        if self.currentjobinfo is None:
            self.log.debug('Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentjobinfo.lasttime) > maxtime:
            self.log.debug('Info too old. Leaving and returning None.')
            return None
        else:
            if queue:
                return self.currentjobinfo[queue]                    
            else:
                self.log.debug('Leaving and returning info of %d entries.' % len(self.currentjobinfo))
                return self.currentjobinfo

    def _query_catalog(self):
        masters = []

        if self.infoclient:
            self.catalog = None
            info = None

            try:
                info = self.infoclient.getbranch('runtime', self.requestid, 'services', 'cctools-catalog-server')
            except Exception, e:
                # We return and empty list below on error
                pass

            if info:
                try:
                    self.catalog = info['hostname'] + ':' + info['port']
                except KeyError:
                    pass
            if not self.catalog:
                self.log.info('catalog information not available.')
                return masters

        cmd = [self.status_exe, '-l']
        if self.catalog:
            self.log.debug('Using catalog: ' + self.catalog)
            cmd.extend(['--catalog', self.catalog])
        else:
            self.log.debug('Did not find catalog contact information.')
            return masters

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()

        try:
            all = json.loads(stdout)
            for master in all:
                if re.search(self.mastername, master['project']):
                    self.log.info('detected wq master: ' + master['project'])
                    masters.append(master)
        except:
            self.log.warning('output of _query_catalog is not valid. Not parsing it. Skip to next loop.') 
        return masters

    def _aggregateinfo(self, masters):
        aggregated = { self.mastername : {'jobstatus' : {}} }
        self._aggregate_tasks_fields(masters, aggregated[self.mastername]['jobstatus'])

        return aggregated

    def _aggregate_tasks_fields(self, masters, info):
        # for fields defined in mappings.conf
        keys      = self.jobstatus2info.keys()

        for k in keys:
            info[k] = 0

        for master in masters:
            # for fields defined in mappings.conf
            for k in keys:
	        info[k] += master[k]

    def _update(self):
        self.log.debug('Starting.')
        
        try:
            masters    = self._query_catalog()
            aggdict    = self._aggregateinfo(masters)
            newjobinfo = map2info(aggdict, WMSStatusInfo(), self.jobstatus2info)

            newjobinfo.catalog = self.catalog

            self.log.info("Replacing old info with newly generated info.")
            self.log.info("Waiting: %d    Running: %d" % (aggdict[self.mastername]['jobstatus']['tasks_waiting'], aggdict[self.mastername]['jobstatus']['tasks_running']))
            self.currentjobinfo = newjobinfo
        except Exception as e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        self.log.debug('_Leaving.')

def test():
    list =  [
            { 'project' : 'TEST_WQ_1', 'tasks_running' : '2',  'tasks_waiting' : '1' },
            { 'project' : 'TEST_WQ_2', 'tasks_running' : '20', 'tasks_waiting' : '2' },
            ] 
    
if __name__=='__main__':
    pass



