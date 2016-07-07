#!/usr/bin/env python

import json
import logging
import os

from urllib import urlopen

from autopyfactory.apfexceptions import ConfigFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.interfaces import ConfigInterface


class Agis(ConfigInterface):
    """
    creates the configuration files with 
    information retrieved from AGIS
    """

    def __init__(self, factory):

        self.log = logging.getLogger("main.configplugin")
        self.factory = factory
        self.fcl = factory.fcl
        self.log.info('ConfigPlugin: Object initialized.')


    def getConfig(self):

        qcl = Config()

        # FIXME
        # here the code to query AGIS
        # and create the config object


        ##################################################################
        # FOR NOW, JUST TESTING WITH ORIGINAL CODE, COPIED & PASTED HERE
        # BEGIN
        ##################################################################

        class Options:
            activity = 'analysis'
        options = Options()

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas&cloud=US'
        handle = urlopen(url)
        d = json.load(handle, 'utf-8')
        handle.close()

        # loop through PandaQueues
        for key in sorted(d):
            try:
                if options.activity == 'ptest':
                    if not d[key]['ptest']:
                        continue
                elif options.activity != d[key]['type']:
                    continue

                if d[key]['pilot_manager'] != 'APF':
                    print
                    print "# Excluded, not APF: %s/%s (pilot_manager = %s)" % (d[key]['site'], key, d[key]['pilot_manager'])
                    continue
                if d[key]['resource_type'] != 'GRID':
                    print
                    print "# Excluded, not GRID: %s/%s (resource_type = %s)" % (d[key]['site'], key, d[key]['resource_type'])
                    continue
                if d[key]['vo_name'] != 'atlas':
                    print
                    print "# Excluded, not atlas: %s/%s (vo_name = %s)" % (d[key]['site'], key, d[key]['vo_name'])
                    continue
                if d[key]['site_state'] != 'ACTIVE':
                    print
                    print "# Excluded, not ACTIVE: %s/%s (site_state = %s)" % (d[key]['site'], key, d[key]['site_state'])
                    continue

                wmsqueue = d[key]['panda_resource']
                memory = d[key]['memory']
                maxtime = d[key]['maxtime']
                maxmemory = d[key]['maxmemory']
                corecount = d[key]['corecount']
                ismcore = corecount > 1
                maxrss = d[key].get('maxrss', 0)
                maxswap = d[key].get('maxswap', 0)
                pilot_version = d[key].get('pilot_version', 'current')
                
                for q in d[key]['queues']:
                    if q['ce_state'] != 'ACTIVE':
                        print
                        print "# Excluded, not 'ACTIVE': %s/%s (ce_state = %s)" % (key, q['ce_name'], q['ce_state'])
                        continue
                    if q['ce_status'] not in ['production', 'Production']:
                        print
                        print "# Warning, not 'production': %s/%s (ce_status = %s)" % (key, q['ce_name'], q['ce_status'])
#                        continue
                    if q['ce_queue_status'] not in ["", 'production', 'Production']:
                        print
                        print "# Warning, not 'Production': %s/%s (ce_queue_status = %s)" % (key, q['ce_name'], q['ce_queue_status'])
#                        continue
                    gramqueue = None 
                    nordugridrsl = None 
                    submitplugin = None
                    submitpluginstring = None
                    gramqueue = None
                    gramversion = None
                    creamenv = None
                    creamattr = ''
                    condorattr = False
#                    maxwctime = q['ce_queue_maxwctime']
                
                    if q['ce_flavour'] == 'OSG-CE':
                
                        gridresource = '%s/jobmanager-%s' %(q['ce_endpoint'], q['ce_jobmanager'])
                        if q['ce_version'] == 'GT2':
                            submitplugin = 'CondorGT2'
                            submitpluginstring = 'condorgt2'
                            gramversion = 'gram2'
                        if q['ce_version'] == 'GT5':
                            submitplugin = 'CondorGT5'
                            submitpluginstring = 'condorgt5'
                            gramversion = 'gram5'
                        if q['ce_queue_name']:
                            gramqueue = q['ce_queue_name']
                
                    elif q['ce_flavour'] == 'CREAM-CE':
                        gridresource = '%s/ce-cream/services/CREAM2 %s %s' %(q['ce_endpoint'], q['ce_jobmanager'], q['ce_queue_name'])
                        submitplugin = 'CondorCREAM'
                        submitpluginstring = 'condorcream'
                        if pilot_version not in ['current']:
                            creamenv = 'RUCIO_ACCOUNT=pilot PILOT_HTTP_SOURCES=%s' % pilot_version
                        else:
                            creamenv = 'RUCIO_ACCOUNT=pilot'

                        # glue 1.3 uses minutes and this / operator uses floor value
                        creammaxtime = maxtime / 60
                        # https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuideEMI2#Forward_of_requirements_to_the_b
                        if maxrss and creammaxtime:
                            if corecount:
                                creamattr = 'CpuNumber=%d;WholeNodes=false;SMPGranularity=%d;' % (corecount, corecount)
                            if corecount:
                                cputime = corecount * creammaxtime
                            else:
                                cputime = creammaxtime
                            
                            creamattr += 'CERequirements = "other.GlueCEPolicyMaxCPUTime == %d ' % cputime
                            creamattr += '&& other.GlueCEPolicyMaxWallClockTime == %d ' % creammaxtime
                            creamattr += '&& other.GlueHostMainMemoryRAMSize == %d' % maxrss
                            if maxswap:
                                maxvirtual = maxrss + maxswap
                                creamattr += ' && other.GlueHostMainMemoryVirtualSize == %d";' % maxvirtual
                            else:
                                creamattr += '";'
                        else:
                            if corecount:
                                creamattr = 'CpuNumber=%d;WholeNodes=false;SMPGranularity=%d;' % (corecount, corecount)
                            if maxmemory and creammaxtime:
                                creamattr += 'CERequirements = "other.GlueHostMainMemoryRAMSize == %d ' % maxmemory
                                creamattr += '&& other.GlueHostPolicyMaxWallClockTime == %d";' % creammaxtime
                            elif maxmemory:
                                creamattr += 'CERequirements = "other.GlueHostMainMemoryRAMSize == %d";' % maxmemory
                            elif creammaxtime:
                                creamattr += 'CERequirements = "other.GlueHostPolicyMaxWallClockTime == %d";' % creammaxtime


                    elif q['ce_flavour'] == 'LCG-CE':
                        print
                        print "# Skipping GT2 queue: %s (%s)" % (key, q['ce_queue_id'])
                        continue
                        
                    elif q['ce_flavour'] == 'ARC-CE':
                        # ignore :port part
                        gridresource = q['ce_endpoint'].split(':')[0]
                        submitplugin = 'CondorNordugrid'
                        submitpluginstring = 'condornordugrid'
                        nordugridrsl = '(jobname = arc_pilot)'
                        rsladd = '(runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)(runtimeenvironment = ENV/PROXY)'
                        rsladd += '(jobname = arc_pilot)'
                        if corecount:
                            rsladd += '(count = %d)' % corecount
                            rsladd += '(countpernode = %d)' % corecount
                        if maxrss and corecount:
                            percore = maxrss/corecount
                            rsladd += '(memory = %d)' % percore
                        elif maxrss:
                            rsladd += '(memory = %d)' % maxrss
                        elif maxmemory and corecount:
                            percore = maxmemory/corecount
                            rsladd += '(memory = %d)' % percore
                        elif maxmemory:
                            rsladd += '(memory = %d)' % maxmemory
                        if maxtime:
                            rsladd += '(walltime = %d)' % maxtime
                        if maxtime and corecount:
                            totaltime = maxtime*corecount
                            rsladd += '(cputime = %d)' % totaltime

                    elif q['ce_flavour'] == 'HTCONDOR-CE':
                        gridresource = q['ce_endpoint'].split(':')[0]
                        submitplugin = 'CondorOSGCE'
                        submitpluginstring = 'condorosgce'
                        condorattr = True
                    else:
                        print
                        print "# Unknown ce_flavour (%s) for %s (%s)" % (q['ce_flavour'], q['ce_name'], q['ce_queue_id'])
                        continue
                
                    print
                    if options.activity == 'ptest':
                        print '[%s-%s-ptest]' % (d[key]['nickname'], q['ce_queue_id'])
                    else:
                        print '[%s-%s]' % (d[key]['nickname'], q['ce_queue_id'])
                    print 'enabled = True'
                    print 'batchqueue = %s' % key
                    print 'wmsqueue = %s' % wmsqueue
                    print 'batchsubmitplugin = %s' % submitplugin
                    print 'batchsubmit.%s.gridresource = %s' % (submitpluginstring, gridresource)
                    print 'sched.maxtorun.maximum = %d' % 9999
                    if d[key]['country'] == 'France':
                        print 'schedplugin = Ready, Scale, MaxPerCycle, MinPerCycle, StatusTest, StatusOffline, MaxPending'
                        print 'sched.scale.factor = 0.04'
                    if creamenv:
                        print 'batchsubmit.condorcream.environ = %s' % creamenv
                        if creamattr:
                            print 'creamattr = %s' % creamattr
                            print 'batchsubmit.condorcream.condor_attributes = %(req)s,%(hold)s,%(remove)s,cream_attributes = %(creamattr)s,notification=Never'
                        else:
                            print 'batchsubmit.condorcream.condor_attributes = %(req)s,%(hold)s,%(remove)s,notification=Never'
                    if nordugridrsl:
                        print 'batchsubmit.condornordugrid.nordugridrsl = %s' % nordugridrsl
                        print 'batchsubmit.condornordugrid.condor_attributes = %(req)s,%(hold)s,%(remove)s,notification=Never'
                        print 'nordugridrsl.nordugridrsladd = %s' % rsladd
                        print 'nordugridrsl.queue = %s' % q['ce_queue_name']
                        print 'nordugridrsl.addenv.RUCIO_ACCOUNT = pilot'
                        if pilot_version not in ['current']:
                            print 'nordugridrsl.addenv.PILOT_HTTP_SOURCES = %s' % pilot_version
                    if gramqueue:
                        print 'globusrsl.%s.queue = %s' % (gramversion, gramqueue)
                    if condorattr:
                        print 'batchsubmit.condorosgce.condor_attributes = periodic_remove = (JobStatus == 2 && (CurrentTime - EnteredCurrentStatus) > 604800)'
                        if maxrss:
                            print 'batchsubmit.condorosgce.condor_attributes.+maxMemory = %d' % maxrss
                        else:
                            print 'batchsubmit.condorosgce.condor_attributes.+maxMemory = %d' % maxmemory
                        print 'batchsubmit.condorosgce.condor_attributes.+xcount = %d' % corecount
                    if options.activity == 'analysis':
                        if ismcore:
                            print 'batchsubmit.%s.proxy = atlas-analy-mcore' % submitpluginstring
                        else:
                            print 'batchsubmit.%s.proxy = atlas-analysis' % submitpluginstring
                        print 'executable.arguments = %(executable.defaultarguments)s -u user'
                    elif options.activity == 'production':
                        if ismcore:
                            print 'batchsubmit.%s.proxy = atlas-prod-mcore' % submitpluginstring
                        else:
                            print 'batchsubmit.%s.proxy = atlas-production' % submitpluginstring
                        print 'executable.arguments = %(executable.defaultarguments)s -u managed'
                    elif options.activity == 'ptest':
                        if ismcore:
                            print 'batchsubmit.%s.proxy = atlas-production' % submitpluginstring
                        else:
                            print 'batchsubmit.%s.proxy = atlas-prod-mcore' % submitpluginstring
                        print 'executable.arguments = %(executable.defaultarguments)s -u ptest'

            except KeyError, e:
              print '# Key error: %s' % e
              print

        ##################################################################
        # FOR NOW, JUST TESTING WITH ORIGINAL CODE, COPIED & PASTED HERE
        # END
        ##################################################################

        class Options:
            activity = 'analysis'
        options = Options()

        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas&cloud=US'
        handle = urlopen(url)
        d = json.load(handle, 'utf-8')
        handle.close()

        # loop through PandaQueues
        for key in sorted(d):
            try:
                if options.activity == 'ptest':
                    if not d[key]['ptest']:
                        continue
                elif options.activity != d[key]['type']:
                    continue

                if d[key]['pilot_manager'] != 'APF':
                    print
                    print "# Excluded, not APF: %s/%s (pilot_manager = %s)" % (d[key]['site'], key, d[key]['pilot_manager'])
                    continue
                if d[key]['resource_type'] != 'GRID':
                    print
                    print "# Excluded, not GRID: %s/%s (resource_type = %s)" % (d[key]['site'], key, d[key]['resource_type'])
                    continue
                if d[key]['vo_name'] != 'atlas':
                    print
                    print "# Excluded, not atlas: %s/%s (vo_name = %s)" % (d[key]['site'], key, d[key]['vo_name'])
                    continue
                if d[key]['site_state'] != 'ACTIVE':
                    print
                    print "# Excluded, not ACTIVE: %s/%s (site_state = %s)" % (d[key]['site'], key, d[key]['site_state'])
                    continue

                wmsqueue = d[key]['panda_resource']
                memory = d[key]['memory']
                maxtime = d[key]['maxtime']
                maxmemory = d[key]['maxmemory']
                corecount = d[key]['corecount']
                ismcore = corecount > 1
                maxrss = d[key].get('maxrss', 0)
                maxswap = d[key].get('maxswap', 0)
                pilot_version = d[key].get('pilot_version', 'current')
                
                for q in d[key]['queues']:
                    if q['ce_state'] != 'ACTIVE':
                        print
                        print "# Excluded, not 'ACTIVE': %s/%s (ce_state = %s)" % (key, q['ce_name'], q['ce_state'])
                        continue


        self.log.info('queues ConfigLoader object created')
        return qcl

# -------------------------------------------------------------------
#   For stand-alone usage
# -------------------------------------------------------------------
if __name__ == '__main__':

    class MockFactory:
        fcl = ""

    agis = Agis(MockFactory)
    qcl = agis.getConfig()
    qcl.write('/tmp/conf')  
