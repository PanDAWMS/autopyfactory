#!/usr/bin/env python
'''

Design goals/features:
-- Download and parse JSON once, allow subsequent filtering for specific APF config requests to CE
   queues satisfying a set of specific properties (vo, cloud, activity)
-- Calculate scale factor based on *dynamic examination* of the number of valid CEs serving a PQ
-- Include the scale calculation to use a provided number of factories. (useful until distributed load-
   balancing is implemented.  
-- Function both as a one-shot, command-line, config generator and a long-running built-in config plugin
-- Abstract out attribute matching and rejection
-- Ensure exceptions allow clean failure for both modes, so that a bad queue definition in AGIS
   doesn't prevent a reload, and any previous config files can be preserved. 

-- to use it as a script from command line:

    -C cloud
    -V VO
    -D defaults
    -o output file
    
    $ python /usr/lib/python2.6/site-packages/autopyfactory/plugins/factory/config/Agis.py --activity analysis -C US -V ATLAS -D /etc/autopyfactory/agisdefaults-analysis.conf -o /etc/autopyfactory/us-analysis-agis.conf
    
    $ python /usr/lib/python2.6/site-packages/autopyfactory/plugins/factory/config/Agis.py --activity production -C US -V ATLAS -D /etc/autopyfactory/agisdefaults-production.conf -o /etc/autopyfactory/us-production-agis.conf

'''
from __future__ import print_function

import logging

import copy
import datetime
import json
import os
import sys
import traceback

from ConfigParser import NoOptionError
from StringIO import StringIO
from urllib import urlopen

# Added to support running module as script from arbitrary location. 
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-5])
sys.path.insert(0, prepath)

from autopyfactory.apfexceptions import ConfigFailure
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.interfaces import ConfigInterface

# REQ maps list *required* attribute and values. Object is removed if absent. 
# NEG maps list *prohibited* attribute and values. Object is removed if present. 
PQFILTERREQMAP = { 'pilot_manager' : ['apf'],
                   'resource_type' : ['grid'],
                   'site_state' : ['active']
                   } 
PQFILTERNEGMAP = { } 
CQFILTERREQMAP = {'ce_state' : ['active'],
                   'ce_status' : ['production'],
                   'ce_queue_status'   : ['production',''],
               }
CQFILTERNEGMAP = { 'ce_flavour' : ['lcg-ce'], }

class AgisCEQueueCreationError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)    
    
class AgisPandaQueueCreationError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value) 

class AgisFailureError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)     


class AgisPandaQueue(object):
    
    def __init__(self, parent, d, key):
        self.log = logging.getLogger()
        self.parent = parent
        self.panda_queue_name = key
        try:
            self.panda_resource = d[key]['panda_resource']              # AGLT2_LMEM     
            self.cloud = d[key]['cloud'].lower()                        # us
            self.corecount = d[key]['corecount']
            if self.corecount is None:
                self.corecount = 1
            self.maxmemory = int(d[key]['maxmemory'])                   # always present
            self.maxrss = int(d[key].get('maxrss', None))               # not present
            self.maxswap = int(d[key].get('maxswap', None) )            # not present
            self.maxtime = int(d[key].get('maxtime', None) )            # not present
            self.memory = int(d[key].get('memory', None) )              # not present
            self.pilot_manager = d[key]['pilot_manager'].lower()
            self.pilot_version = d[key].get('pilot_version', 'current')
            self.resource_type = d[key]['resource_type'].lower()        # grid
            self.site_state = d[key]['site_state'].lower()
            self.type = d[key]['type'].lower()                          # production (activity)
            self.vo_name = d[key]['vo_name'].lower()                    # atlas
                    
            self.queues = d[key]['queues']                              # list of dictionaries
            self.ce_queues = self._make_cequeues(self.queues)
        
        except Exception, e:
            self.log.error("Problem creating a PandaQueue %s Exception: %s" % (self.panda_queue_name, 
                                                                               traceback.format_exc()))
            raise AgisCEQueueCreationError("Problem creating a PandaQueue %s" % (self.panda_queue_name) )
          
    def __str__(self):
        s = "AgisPandaQueue: "
        s += "panda_resource=%s " %  self.panda_resource
        s += "vo_name=%s " % self.vo_name
        s += "cloud=%s " % self.cloud 
        s += "type=%s " % self.type
        s += "maxtime=%s " % self.maxtime
        s += "memory=%s " % self.memory
        s += "maxmemory=%s " % self.maxmemory      
        s += "maxrss=%s " % self.maxrss
        s += "maxswap=%s " % self.maxswap
        for ceq in self.ce_queues:
            s += " %s " % ceq
        return s

    def _make_cequeues(self, celist):
        '''
          Makes CEqueue objects, key is PQ name 
        '''
        self.log.debug("Handling cequeues for PQ %s" % self.panda_queue_name)
        cequeues = []
        for cedict in celist:
            self.log.debug("Handling cedict %s" % cedict)
            try:
                cqo = AgisCEQueue( self, cedict)
                cequeues.append( cqo)
            except Exception, e:
                self.log.error('Failed to create AgisCEQueue for PQ %s and CE %s' % (self.panda_queue_name, cedict))
                self.log.error("Exception: %s" % traceback.format_exc())
        self.log.debug("Made list of %d CEQ objects" % len(cequeues))
        return cequeues    
    
    
class AgisCEQueue(object):
    '''
    Represents a single CE queue within a Panda queue description.  
    '''
    def __init__(self, parent, cedict ):
        self.log = logging.getLogger()
        self.parent = parent
        self.panda_queue_name = parent.panda_queue_name 
        self.ce_name = cedict['ce_name']                         # AGLT2-CE-gate04.aglt2.org
        self.ce_endpoint = cedict['ce_endpoint']                 # gate04.aglt2.org:2119
        self.ce_host = self.ce_endpoint.split(":")[0]
        self.ce_state = cedict['ce_state'].lower()               # 'active'
        self.ce_status = cedict['ce_status'].lower()             # 
        self.ce_queue_status = cedict['ce_queue_status'].lower()
        self.ce_flavour = cedict['ce_flavour'].lower()           # GLOBUS
        self.ce_version = cedict['ce_version'].lower()           # GT5
        self.ce_queue_name = cedict['ce_queue_name']             # default
        self.ce_jobmanager = cedict['ce_jobmanager'].lower()     # condor
        self.ce_queue_maxcputime = cedict['ce_queue_maxcputime'] # in seconds
        self.ce_queue_maxwctime = cedict['ce_queue_maxwctime']   # in seconds
        
        self.apf_scale_factor = 1.0
        
        # Empty/default attributes:
        self.gridresource = None
        self.submitplugin = None
        self.submitpluginstr = None
        self.gramversion = None
        self.gramqueue = None
        self.creamenv = None
        self.creamattr = ''
        self.condorattr = None
        self.maxmemory = None
        self.maxtime = None

        if self.ce_flavour in ['osg-ce','globus']:
            self._initglobus()
        elif self.ce_flavour == 'htcondor-ce':
            self._initcondorce()
        elif self.ce_flavour == 'cream-ce':
            self._initcream()
        elif self.ce_flavour == 'arc-ce':
            self._initarc()
        elif self.ce_flavour == 'lcg-ce':
            self.log.debug("Ignoring old CE type 'LCG-CE'")
                    
        else:
            self.log.warning("CEQueue %s has unknown ce_flavour: %s" % (self.ce_name, self.ce_flavour))

    def _initcondorce(self):
        self.gridresource = self.ce_endpoint.split(':')[0]
        self.submitplugin = 'CondorOSGCE'
        self.submitpluginstr = 'condorosgce'

    def _initglobus(self):
        self.gridresource = '%s/jobmanager-%s' % (self.ce_endpoint, self.ce_jobmanager)
        if self.ce_version == 'gt2':
            self.submitplugin = 'CondorGT2'
            self.submitpluginstr = 'condorgt2'
        elif self.ce_version == 'gt5':
            self.submitplugin = 'CondorGT5'
            self.submitpluginstr = 'condorgt5'                
            self.gramversion = 'gram5'
            self.gramqueue = self.ce_queue_name

    def _initcream(self):
        self.gridresource = '%s/ce-cream/services/CREAM2 %s %s' % (self.ce_endpoint, 
                                                                       self.ce_jobmanager, 
                                                                       self.ce_queue_name)
        self.submitplugin = 'CondorCREAM'
        self.submitpluginstr = 'condorcream'
        if self.parent.pilot_version not in ['current']:
            self.creamenv = 'RUCIO_ACCOUNT=pilot PILOT_HTTP_SOURCES=%s' % self.parent.pilot_version
        else:
            self.creamenv = 'RUCIO_ACCOUNT=pilot'

        # glue 1.3 uses minutes and this / operator uses floor value
        # https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuideEMI2#Forward_of_requirements_to_the_b       
        self.maxtime = self.parent.maxtime / 60
        self.maxmemory = self.parent.maxmemory
        self.cputime = self.parent.corecount * self.maxtime

        # maxrss and maxtime are expected to be set in AGIS for all queues
        if self.parent.corecount:
            self.creamattr = 'CpuNumber=%d;WholeNodes=false;SMPGranularity=%d;' % (self.parent.corecount,
                                                                                   self.parent.corecount)
        if self.parent.corecount:
            cputime = self.parent.corecount * self.maxtime
        else:
            cputime = self.maxtime

        self.creamattr += 'CERequirements = "other.GlueCEPolicyMaxCPUTime == %d ' % cputime
        self.creamattr += '&& other.GlueCEPolicyMaxWallClockTime == %d ' % self.maxtime
        self.creamattr += '&& other.GlueHostMainMemoryRAMSize == %d' % self.parent.maxrss
        if self.parent.maxswap:
            maxvirtual = self.parent.maxrss + self.parent.maxswap
            self.creamattr += ' && other.GlueHostMainMemoryVirtualSize == %d";' % maxvirtual
        else:
            self.creamattr += '";'

    def _initarc(self):
        # ignore :port part
        self.gridresource = self.ce_endpoint.split(':')[0]
        self.submitplugin = 'CondorNordugrid'
        self.submitpluginstr = 'condornordugrid'

        self.maxmemory = self.parent.maxmemory
####        self.maxtime = self.ce_queue_maxwctime
        self.maxtime = self.parent.maxtime
        
        self.nordugridrsl = '(jobname = arc_pilot)'
        self.rsladd = '(runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)(runtimeenvironment = ENV/PROXY)'
        self.rsladd += '(count = %d)' % self.parent.corecount
        self.rsladd += '(countpernode = %d)' % self.parent.corecount
        if self.parent.maxrss:
            percore = self.parent.maxrss/self.parent.corecount
            self.rsladd += '(memory = %d)' % percore
        else:
            percore = self.parent.maxmemory/self.parent.corecount
            self.rsladd += '(memory = %d)' % percore

        if self.maxtime:
            self.rsladd += '(walltime = %d)' % self.maxtime
            
        if self.maxtime:
            self.rsladd += '(cputime = %d)' % (self.maxtime * self.parent.corecount)    
            
    def getAPFConfigString(self):
        '''
        Returns string of valid APF configuration for this queue-ce entry.
        Calculates scale factor based on how many other CEs serve this PQ
          
        '''
        cp = self.getAPFConfig()
        sio = StringIO()
        s = cp.write(sio)
        return sio.getvalue()
    
    def getAPFConfig(self):
        '''
        Returns ConfigParser object representing config
        
        '''
        self.cp = Config()
        sect = '%s-%s' % ( self.parent.panda_resource, self.ce_host )
        sect = str(sect)
        self.cp.add_section(sect)      
        # Unconditional config
        self.cp.set( sect, 'enabled', 'True')
        self.cp.set( sect, 'batchqueue', self.panda_queue_name)
        self.cp.set( sect, 'wmsqueue', self.parent.panda_resource )
        self.cp.set( sect, 'batchsubmitplugin', self.submitplugin )
        self.cp.set( sect, 'batchsubmit.%s.gridresource' % self.submitpluginstr , self.gridresource )
        #if self.parent.type == 'analysis':
        #    self.cp.set( sect, 'executable.arguments' , '%(executable.defaultarguments)s -u user'  )
        #else:
        #    self.cp.set( sect, 'executable.arguments' , '%(executable.defaultarguments)s -u managed'  )
        
        try:       
            self.apf_scale_factor = ((( 1.0 / float(self.parent.parent.numfactories) ) / len(self.parent.ce_queues) ) / float(self.parent.parent.jobsperpilot) ) 
        except ZeroDivisionError:
            self.log.error("Division by zero. Something wrong with scale factory calc.")
            self.apf_scale_factor = 1.0
        self.cp.set( sect, 'sched.scale.factor', str(self.apf_scale_factor) )
        
        #HTCondor CE
        if self.ce_flavour == 'htcondor-ce':
            pr = 'periodic_remove = (JobStatus == 2 && (CurrentTime - EnteredCurrentStatus) > 604800)'
            self.cp.set( sect, 'batchsubmit.condorosgce.condor_attributes', pr )          
            if self.parent.maxrss is not None:
                self.cp.set( sect, 'batchsubmit.condorosgce.condor_attributes.+maxMemory', str(self.parent.maxrss) )
            else:
                self.cp.set( sect, 'batchsubmit.condorosgce.condor_attributes.+maxMemory', str(self.parent.maxmemory) )

            self.cp.set( sect, 'batchsubmit.condorosgce.condor_attributes.+xcount', str(self.parent.corecount) )
            self.cp.set( sect, 'batchsubmit.condorosgce.condor_attributes.+voactivity', '"%s"' % self.parent.type )
            self.cp.set( sect, 'batchsubmit.condorosgce.condor_attributes.+remote_queue', '"%s"' % self.ce_queue_name)

        # Globus
        if self.ce_flavour in ['osg-ce','globus']:
            self.cp.set( sect, 'globusrsl.%s.queue' % self.gramversion, self.gramqueue )
              
        # Cream-specific JDL
        if self.ce_flavour == 'cream-ce':
            self.cp.set( sect, 'batchsubmit.condorcream.environ', self.creamenv )
            if self.creamattr is not None:
                self.cp.set(sect, 'creamattr', self.creamattr)
                self.cp.set(sect, 'batchsubmit.condorcream.condor_attributes' , '%(req)s,%(hold)s,%(remove)s,cream_attributes = %(creamattr)s,notification=Never' )      
            else:
                self.cp.set(sect, 'batchsubmit.condorcream.condor_attributes' , '%(req)s,%(hold)s,%(remove)s,notification=Never' )

        # Arc-CE
        if self.ce_flavour == 'arc-ce':
            pr = 'periodic_remove = (JobStatus == 2 && (CurrentTime - EnteredCurrentStatus) > 604800)'
            pr = '%(req)s,%(hold)s,%(remove)s,notification=Never'
            self.cp.set( sect, 'batchsubmit.condornordugrid.condor_attributes', pr )
            self.cp.set( sect, 'batchsubmit.condornordugrid.condor_attributes.+remote_queue', self.ce_queue_name )
            self.cp.set( sect, 'batchsubmit.condornordugrid.nordugridrsl', self.nordugridrsl )
            self.cp.set( sect, 'nordugridrsl.nordugridrsladd', self.rsladd )
            self.cp.set( sect, 'nordugridrsl.queue', self.ce_queue_name )
            self.cp.set( sect, 'nordugridrsl.addenv.RUCIO_ACCOUNT', 'pilot' )

        return self.cp

    
    def __str__(self):
        s = "AgisCEQueue: "
        s += "PQ=%s " %  self.panda_queue_name
        s += "wmsqueue=%s " % self.parent.panda_resource
        s += "submitplugin=%s " % self.submitplugin
        s += "host=%s " % self.ce_host
        s += "endpoint=%s " %self.ce_endpoint
        s += "gridresource=%s " % self.gridresource
        s += "maxtime=%s " % self.parent.maxtime
        return s

class Agis(ConfigInterface):
    """
    creates the configuration files with 
    information retrieved from AGIS
    """

    def __init__(self, factory, config, section):
        '''
        Top-level object fo contacting, parsing, and providing APF configs from AGIS
        '''

        self.log = logging.getLogger()
        self.allqueues = None
        self.lastupdate = None
        self.config = config
        self.baseurl = self.config.get('Factory', 'config.agis.baseurl')
        self.sleep = self.config.get('Factory', 'config.agis.sleep')
        self.jobsperpilot = 1.0
        self.numfactories = 1.0
        
        # For vos, clouds, and activities, 'None' means everything in AGIS
        self.vos = None
        self.clouds = None
        self.activities = None
        self.defaultsfile = None
        try:
            self.jobsperpilot = self.config.getfloat('Factory', 'config.agis.jobsperpilot')
        except NoOptionError, noe:
            pass
        
        try:
            self.numfactories = self.config.getfloat('Factory', 'config.agis.numfactories')
        except NoOptionError, noe:
            pass
        
        # For defaultsfile, None means no defaults included in config. Only explicit values returned. 
        try:         
            defaultsfile = self.config.get('Factory', 'config.agis.defaultsfiles')
            if defaultsfile.strip().lower() == 'none':
                self.defaultsfile = None
            else:
                self.defaultsfile = [ default.strip() for default in self.config.get('Factory', 'config.agis.defaultsfiles').split(',') ]
        except NoOptionError, noe:
            pass
        
        try:
            vostr = self.config.get('Factory', 'config.agis.vos')
            if vostr.strip().lower() == 'none':
                self.vos = None
            else:
                self.vos = [ vo.strip().lower() for vo in self.config.get('Factory', 'config.agis.vos').split(',') ]    
        except NoOptionError, noe:
            pass
        
        try:
            cldstr = self.config.get('Factory', 'config.agis.clouds')
            if cldstr.strip().lower() == 'none':
                self.clouds = None
            else:
                self.clouds = [ cl.strip().lower() for cl in self.config.get('Factory', 'config.agis.clouds').split(',') ]
        except NoOptionError, noe:
            pass
        
        try:
            actstr = self.config.get('Factory', 'config.agis.activities')
            if actstr.strip().lower() == 'none':
                self.activities = None
            else:
                self.activities = [ ac.strip().lower() for ac in self.config.get('Factory', 'config.agis.activities').split(',') ]
        except NoOptionError, noe:
            pass
        
        self.log.debug('ConfigPlugin: Object initialized. %s' % self)

    def _updateInfo(self):
        '''
        Contact AGIS and update full queue info.
        '''
        try:
            d = self._downloadJSON()
            self.log.debug("Calling _handleJSON")
            queues = self._handleJSON(d)
            self.log.debug("AGIS provided list of %d total queues." % len(queues))
            self.allqueues = queues
            self.lastupdate =  datetime.datetime.now()
        except Exception, e:
                self.log.error('Failed to contact AGIS or parse problem: %s' %  traceback.format_exc() )
                raise AgisFailureError("Unable to contact AGIS or parsing error.")                                                              
                
###    def getConfigString(self, volist=None, cloudlist=None, activitylist=None, defaultsfile=None):
###        '''
###        For embedded usage. Handles everything in config.
###        Pulls out valid PQ/CEs for specified vo, cloud, activity
###        Returns string APF config. 
###        PQ filtering:
###            VO
###              'vo_name'       : ['atlas'],
###            CLOUD  
###              'cloud'
###            ACTIVITY
###               'type'      
###        '''
###        if self.allqueues is None:
###            self._updateInfo()
###
###        td = datetime.datetime.now() - self.lastupdate
###        #
###        totalseconds = td.seconds + ( td.days * 24 * 3600)
###        if totalseconds > self.sleep:
###            self._updateInfo()
###        
###        # total_seconds() introduced in Python 2.7
###        # Should be used when possible.
###        # Change back when 2.6 not needed.  
###        #if td.total_seconds()  > self.sleep:
###        #    self._updateInfo()
###        
###        # Don't mess with the built-in default filters. 
###        mypqfilter = copy.deepcopy(PQFILTERREQMAP)
###        if self.vos is not None and len(self.vos) > 0:
###            mypqfilter['vo_name'] = self.vos
###        if self.clouds is not None and len(self.clouds ) > 0:
###            mypqfilter['cloud'] = self.clouds         
###        if self.activities is not None and len(self.activities) > 0:
###            mypqfilter['type'] = self.activities
###
###        self.log.debug("Before filtering. allqueues has %d objects" % len(self.allqueues))
###        self.allqueues = self._filterobjs(self.allqueues, mypqfilter, PQFILTERNEGMAP)
###        self.log.debug("After filtering. allqueues has %d objects" % len(self.allqueues))
###        
###        for q in self.allqueues:
###            self.log.debug("Before filtering. ce_queues has %d objects" % len(q.ce_queues))
###            q.ce_queues = self._filterobjs(q.ce_queues, CQFILTERREQMAP, CQFILTERNEGMAP )
###            self.log.debug("After filtering. ce_queues has %d objects" % len(q.ce_queues))
###                
###        s = ""
###        if self.defaultsfile is not None:
###            df = open(self.defaultsfile[0])
###            for line in df.readlines():
###                s += line
###            s += "\n"
###        for q in self.allqueues:
###            for cq in q.ce_queues:
###                s += "%s\n" % cq.getAPFConfigString()        
###        return s

    def getConfigString(self, volist=None, cloudlist=None, activitylist=None, defaultsfile=None):
        self.getConfig()
        return self.strconfig

    
    def getConfig(self):
        '''
        Required for autopyfactory Config plugin interface. 
        Returns ConfigParser representing config
        '''
        if self.allqueues is None:
            self._updateInfo()

        td = datetime.datetime.now() - self.lastupdate
        #
        totalseconds = td.seconds + ( td.days * 24 * 3600)
        if totalseconds > self.sleep:
            self._updateInfo()

        self._filter()

        ## create the config
        cp = Config()
        self.strconfig = '' 

        for i in range(len(self.activities)):

            vo = self.vos[i]
            cloud = self.clouds[i]
            activity = self.activities[i]
            default = self.defaultsfile[i]
    
            tmpcp = Config()    
            tmpfile = open(default)
            tmpcp.readfp(tmpfile)

            tmpfile.seek(0) # to read the file over again
            for line in tmpfile.readlines():
                self.strconfig += line

            for q in self.allqueues:
                if q.vo_name == vo and\
                   q.cloud == cloud and\
                   q.type == activity:
                    for cq in q.ce_queues:
                        try:
                            qc = cq.getAPFConfig()
                            tmpcp.merge(qc)
                            # add content of Config object to the string representation
                            self.strconfig += "\n"
                            self.strconfig += qc.getContent()
                        except Exception, e:
                            self.log.error('Captured exception %s' %e) 
            cp.merge(tmpcp)

        return cp 


    def _filter(self):
    
        # Don't mess with the built-in default filters. 
        mypqfilter = copy.deepcopy(PQFILTERREQMAP)
        if self.vos is not None and len(self.vos) > 0:
            mypqfilter['vo_name'] = self.vos
        if self.clouds is not None and len(self.clouds ) > 0:
            mypqfilter['cloud'] = self.clouds    
        if self.activities is not None and len(self.activities) > 0:
            mypqfilter['type'] = self.activities

        self.log.debug("Before filtering. allqueues has %d objects" % len(self.allqueues))
        self.allqueues = self._filterobjs(self.allqueues, mypqfilter, PQFILTERNEGMAP)
        self.log.debug("After filtering. allqueues has %d objects" % len(self.allqueues))
    
        for q in self.allqueues:
            self.log.debug("Before filtering. ce_queues has %d objects" % len(q.ce_queues))
            q.ce_queues = self._filterobjs(q.ce_queues, CQFILTERREQMAP, CQFILTERNEGMAP )
            self.log.debug("After filtering. ce_queues has %d objects" % len(q.ce_queues))

    


    def getConfigWMSQueue(self, wmsqueue):
        '''
        get the config sections only for a given wmsqueue
        '''

        conf = self.getConfig()
        out = Config()
        for section_name in conf.sections():
            section = conf.getSection(section_name)
            if section.get(section_name, 'wmsqueue') == wmsqueue:
                out.merge(section)
        return out 


  
    def _filterobjs(self, objlist, reqdict=None, negdict=None):
        '''
        Generic filtering method. 
        '''
        newobjlist = []
        kept = 0
        filtered = 0
        for ob in objlist:
            keep = True
            for attrstr in reqdict.keys():
                self.log.debug("Checking object %s attribute %s for values in %s" % (type(ob), 
                                                                    attrstr, 
                                                                    reqdict[attrstr]))
                value = getattr(ob, attrstr)
                self.log.debug("%s: Checking value %s for match..." % (ob, value))
                if getattr(ob, attrstr) not in reqdict[attrstr]:
                    self.log.debug("%s: %s does not contain any entries from %s. Setting to remove." % (ob, 
                                                                               attrstr, 
                                                                               reqdict[attrstr]))
                    keep = False
                else:
                    self.log.debug("%s: %s did contain a value from %s. Retaining..."  % (ob, 
                                                                               attrstr, 
                                                                               reqdict[attrstr]))                                    
            if keep:
                kept += 1
                newobjlist.append(ob)
            else:
                self.log.debug("Remove obj %s" % ob)
                #newobjlist.remove(ob)
                filtered += 1
        self.log.debug("Keeping %d objects, filtered %d objects for required attribute values." % (kept, filtered))
        
        newlist2 = []
        kept = 0
        filtered = 0
        for ob in newobjlist:
            keep = True
            for attrstr in negdict.keys():
                if getattr(ob, attrstr) in negdict[attrstr]:
                    filtered += 1
                    keep = False
            if keep:
                kept += 1
                newlist2.append(ob)
            else:
                self.log.debug("Remove obj %s" % ob)
                filtered += 1
        self.log.debug("Keeping %d objects, filtered %d objects for prohibited attribute values." % (kept, filtered))
        return newlist2

    
    def _downloadJSON(self):
        url = '%s' % self.baseurl
        self.log.debug('Contacting %s' % url)
        handle = urlopen(url)
        d = json.load(handle, 'utf-8')
        handle.close()
        self.log.debug('Done.')
        of = open('/tmp/agis-json.txt', 'w')
        json.dump(d,of, indent=2, sort_keys=True)
        of.close()
        return d

    def _handleJSON(self, jsondoc):
        '''
        Returns all PQ objects in list.  
        '''
        self.log.debug("handleJSON called for activities %s" % self.activities)
        queues = []
        for key in sorted(jsondoc):
            self.log.debug("key = %s" % key)
            try:
                qo = AgisPandaQueue(self, jsondoc, key)
                queues.append(qo)
            except Exception, e:
                self.log.error('Failed to create AgisPandaQueue %s Exception: %s' % (key,
                                                                                     traceback.format_exc()
                                                                                     ) )
        self.log.debug("Made list of %d PQ objects" % len(queues))
        return queues
    
    def __str__(self):
        s = 'Agis top-level object: '
        s+= 'vos=%s ' % self.vos
        s+= 'clouds=%s' % self.clouds
        s+= 'activities=%s ' % self.activities
        s+= 'defaultsfile=%s ' % self.defaultsfile
        s += 'numfactories=%s ' % self.numfactories
        s += 'jobsperpilot=%s ' % self.jobsperpilot
        return s
        
# -------------------------------------------------------------------
#   For stand-alone usage
# -------------------------------------------------------------------
if __name__ == '__main__':
    import logging
    import getopt
    import sys
    import os
    from ConfigParser import ConfigParser, SafeConfigParser
    
    debug = 0
    info = 0
    vo = None
    cloud = None
    activity = None
    jobsperpilot = 1.5
    numfactories = 4
    outfile = '/tmp/agis-apf-config.conf'
    fconfig_file = None
    default_configfile = os.path.expanduser("/etc/autopyfactory/autopyfactory.conf")
    defaultsfile = None
         
    usage = """Usage: Agis.py [OPTIONS]  
    OPTIONS: 
        -h --help                   Print this message
        -d --debug                  Debug messages
        -v --verbose                Verbose information
        -c --config                 Config file [/etc/autopyfactory/autopyfactory.conf]
        -o --outfile                Output file ['/tmp/agis-apf-config.conf']
        -j --jobsperpilot           Scale factor. [1.5]
        -n --numfactories           Multi-factory scale factor. 
        -D --defaults               Defaults file [None]
        -V --vo                     A single virtual organization [<all>]
        -C --cloud                  A single cloud [<all>]
        -A --activity               A single activity (PQ 'type') [<all>]
        
        """
    
    # Handle command line options
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 
                                   "hdvtc:o:j:n:D:V:C:A:", 
                                   ["help", 
                                    "debug", 
                                    "verbose",
                                    "config=",
                                    "outfile=",
                                    "jobsperpilot=",
                                    "numfactories=",
                                    "defaults=",
                                    "vo=",
                                    "cloud=",
                                    "activity=",
                                    ])
    except getopt.GetoptError, error:
        print( str(error))
        print( usage )                          
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(usage)                     
            sys.exit()            
        elif opt in ("-d", "--debug"):
            debug = 1
        elif opt in ("-v", "--verbose"):
            info = 1
        elif opt in ("-c", "--config"):
            fconfig_file = arg
        elif opt in ("-o", "--outfile"):
            outfile = arg        
        elif opt in ('-j', '--jobsperpilot'):
            jobsperpilot = arg
        elif opt in ('-n', '--numfactories'):
            numfactories = arg
        elif opt in ("-D", "--defaults"):
            defaultsfile = arg        
        elif opt in ("-C", "--cloud"):
            cloud = arg.lower() 
        elif opt in ("-V", "--vo"):
            vo = arg.lower()
        elif opt in ("-A", "--activity"):
            activity = arg.lower()            
   
    # Check python version 
    major, minor, release, st, num = sys.version_info
    
    # Set up logging, handle differences between Python versions... 
    # In Python 2.3, logging.basicConfig takes no args
    #
    FORMAT23="[ %(levelname)s ] %(asctime)s %(filename)s (Line %(lineno)d): %(message)s"
    FORMAT24=FORMAT23
    FORMAT25="[%(levelname)s] %(asctime)s %(module)s.%(funcName)s(): %(message)s"
    FORMAT26=FORMAT25
    
    if major == 2:
        if minor ==3:
            formatstr = FORMAT23
        elif minor == 4:
            formatstr = FORMAT24
        elif minor == 5:
            formatstr = FORMAT25
        elif minor == 6:
            formatstr = FORMAT26
        elif minor == 7:
            formatstr = FORMAT26
    
    log = logging.getLogger()
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(formatstr)
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)
    
    log.setLevel(logging.WARNING)
    if debug: 
        log.setLevel(logging.DEBUG) # Override with command line switches
    if info:
        log.setLevel(logging.INFO) # Override with command line switches
    log.debug("Logging initialized.")      
    
    fconfig=Config()
    if fconfig_file is not None:
        fconfig_file = os.path.expanduser(fconfig_file)
        got_config = fconfig.read(fconfig_file)
        log.debug("Read config file %s, return value: %s" % (fconfig_file, got_config))  
    else:
        # Create valid config...
        fconfig.add_section('Factory')

        # Set unconditional defaults
        fconfig.set('Factory', 'config.agis.baseurl', 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all'   )
        fconfig.set('Factory', 'config.agis.sleep', '3600'  )
        fconfig.set('Factory', 'config.agis.jobsperpilot', '1.5' )
        fconfig.set('Factory', 'config.agis.numfactories', '4')
        
        '''
        config.agis.vos = atlas
        config.agis.clouds = US
        config.agis.activities = analysis,production
        config.agis.defaultsfiles = /etc/autopyfactory/agisdefaults.conf
        '''

    # Override defaults with command line values, if given    
    if vo is not None:
        fconfig.set('Factory', 'config.agis.vos', vo)
    if cloud is not None:
        fconfig.set('Factory', 'config.agis.clouds', cloud)
    if activity is not None:
        fconfig.set('Factory', 'config.agis.activities', activity)
    if defaultsfile is not None:
        fconfig.set('Factory', 'config.agis.defaultsfiles', defaultsfile)
    if jobsperpilot is not None:
        fconfig.set('Factory', 'config.agis.jobsperpilot', str(jobsperpilot))
    if numfactories is not None:
        fconfig.set('Factory', 'config.agis.numfactories', str(numfactories))
 
    #parent class Mock
    class Factory:
        pass

    acp = Agis(Factory(), fconfig, "mock_section_name")
    log.debug("Agis object created")

    try:
        configstr = acp.getConfigString()
        if configstr is not None:    
            log.debug("Got config string for writing to outfile %s" % outfile)
            outfile = os.path.expanduser(outfile)
            f = open(outfile, 'w')
            f.write(configstr)
            f.close()
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception, e:
        log.error("Got exception during APF config generation: %s" % traceback.format_exc() )
