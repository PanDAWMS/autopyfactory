#!/usr/bin/env python
# $Id: monitor.py 7686 2011-04-08 21:15:43Z jhover $
#

'''
 Native monitoring system for autopyfactory, signals Monitoring
 webservice at each factory cycle with list of condor jobs

States

    CREATED: condor_submit executed, condor_id returned
    RUNNING: pilot wrapper has started on Worker Node
    EXITING: pilot wrapper is finishing up on Worker Node
    DONE: condor jobstate is Completed (or Removed)
    FAULT: condor jobstate indicates a fault, or job has become stale 

API
Endpoint              Description                           Format
/h/                   hello from factory at start-up        POST with keys: factoryId,monitorURL,factoryOwner,baseLogDirUrl,versionTag
/c/                   list of created jobids                JSON-encoded list of tuples: (cid, nick, fid, label)
/m/                   list of messages w/ status of 'label' JSON-encoded list of tuples: (nick, fid, label, text)
/$APFFID/$APFCID/rn/  ping when wrapper starts              GET request with APFFID=factoryId, APFCID=jobid
/$APFFID/$APFCID/ex/  ping when wrapper exits               GET request with APFFID=factoryId, APFCID=jobid 

Glossary

nick  means e.g. Panda queue
fid    factory id
cid    condor job id
label  

http://apfmon.lancs.ac.uk/mon/c/
data=[["931048.0", "BNL_CVMFS_1-condor", "BNL-gridui09-jhover", "BNL_CVMFS_1-gridgk06"]]

'''

import commands
import logging
import re
import threading
import StringIO
import urllib2

from autopyfactory.factory import Singleton, singletonfactory
from autopyfactory.interfaces import MonitorInterface

try:
    import json as json
except ImportError, err:
    # Not critical (yet) - try simplejson
    log = logging.getLogger('main.monitor')
    log.debug('json package not installed. Trying to import simplejson as json')
    import simplejson as json


__author__ = "Peter Love, Jose Caballero"
__copyright__ = "2010,2011 Peter Love; 2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


#  ==================================================
#
#       CLASSES TO HANDLE HTTP CALLS BETTER
#
#  ==================================================

class NoExceptionHTTPHandler(urllib2.BaseHandler):

    # a substitute/supplement to urllib2.HTTPErrorProcessor
    # that doesn't raise exceptions on status codes 201,204,206

    def http_error_201(self, request, response, code, msg, hdrs):
        return response
    def http_error_204(self, request, response, code, msg, hdrs):
        return response
    def http_error_206(self, request, response, code, msg, hdrs):
        return response


class RequestWithMethod(urllib2.Request):
    # to be used insted of urllib2.Request
    # This class gets the HTTP method (i.e. 'PUT') during the initlization

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method


#  ==================================================


_CIDMATCH = re.compile('\*\* Proc (\d+\.\d+)', re.M)


class APFMonitor2Plugin(MonitorInterface):

    __metaclass__ = singletonfactory(id_var="monitor_id")


    """
    Notifies a monitoring webservice about condor jobs
    """
    def __init__(self, apfqueue, monitor_id):
        '''
        apfqueue is a reference to the APFQueue object creating this plugin.

        monitor_id is the value for id_var (input of the singletonfactory)
        to decide if a new object has to be really created or not.
        It is the name of the section [] in monitor config object
        
        Also sends initial ping to monitor server. 
        
        '''
        self.log = logging.getLogger('main.monitor [singleton created by %s with id %s]' %(apfqueue.apfqname, monitor_id))
        mainlevel = logging.getLogger('main').getEffectiveLevel()
        self.log.setLevel(mainlevel)
        self.log.debug("Start...")


        self.apfqname = apfqueue.apfqname
        self.qcl = apfqueue.factory.qcl
        self.fcl = apfqueue.factory.fcl
        self.mcl = apfqueue.factory.mcl
    
        self.fid = self.fcl.generic_get('Factory','factoryId')
        self.version = self.fcl.generic_get('Factory', 'versionTag')
        self.email = self.fcl.generic_get('Factory','factoryAdminEmail')
        self.owner = self.email
        self.baselogurl = self.fcl.generic_get('Factory','baseLogDirUrl')

        self.monurl = self.mcl.generic_get(monitor_id, 'monitorURL')
        self.crurl = self.monurl + 'c/'
        self.msgurl = self.monurl + 'm/'
        self.furl = self.monurl + 'h/'
        
        self.crlist = []
        self.msglist = []
        
        self.jsonencoder = json.JSONEncoder()
        self.buffer = StringIO.StringIO()
        
        self.log.debug('Instantiated monitor')
        self.registerFactory()     
        self.log.debug('Done.')


    def registerFactory(self):
        '''
        factoryId,monitorURL,factoryOwner,baseLogDirUrl,versionTag

        First check if the factory is already registered. 
        If not, then register it. 
        '''

    def _checkFactory(self):



    def _registerFactory(self):

        attrlist = []
        attrlist.append("factoryId=%s" % self.fid)
        attrlist.append("factoryOwner=%s" % self.owner)
        attrlist.append("versionTag=%s" % self.version)
        attrlist.append("factoryAdminEmail=%s" % self.email)
        attrlist.append("baseLogDirUrl=%s" % self.baselogurl)

        data = '&'.join(attrlist)        
        self._call(method, self.furl, data)


    def registerLabel(self):
        '''
        factoryId,monitorURL,factoryOwner,baseLogDirUrl,versionTag
        '''
        attrlist = []
        attrlist.append("factoryId=%s" % self.fid)
        attrlist.append("factoryOwner=%s" % self.owner)
        attrlist.append("versionTag=%s" % self.version)
        attrlist.append("factoryAdminEmail=%s" % self.email)
        attrlist.append("baseLogDirUrl=%s" % self.baselogurl)
        data = '&'.join(attrlist)        
        self._signal(self.furl, data)


    def registerJobs(self, apfqueue, jobinfolist ):
        '''
        Take a list of JobInfo objects and translate to APFMonitor messages.

        We pass apfqueue as one of the inputs because this class is a singleton,
        so the apfqueue object passed by __init__() may not be the same 
        apfqueue object calling this method. 
        '''

        self.log.debug('updateJobs: starting for apfqueue %s with info list %s' %(apfqueue.apfqname, 
                                                                                       jobinfolist))
        if jobinfolist:
        # ensure jobinfolist has any content, and is not None
            apfqname = apfqueue.apfqname
            nickname = self.qcl.generic_get(apfqname, 'batchqueue') 
            crlist = []
            for ji in jobinfolist:
                data = (ji.jobid, nickname, self.fid, apfqname)
                self.log.debug('updateJobs: adding data (%s, %s, %s, %s)' %(ji.jobid, nickname, self.fid, apfqname))
                crlist.append(data)
            
            jsonmsg = self.jsonencoder.encode(crlist)
            txt = "data=%s" % jsonmsg
            self._signal(self.crurl, txt)

        self.log.debug('updateJobs: leaving.')

       
    def _call(self, method, url, data=None):
        '''
        make the HTTP call
        '''

        self.log.debug('Starting. method=%s, url=%s, data=%s' %(method, url, data))

        opener = urllib2.build_opener(NoExceptionHTTPHandler) 
        if data:
                request = RequestWithMethod(method, url, data)
        else:
                request = RequestWithMethod(method, url)
        try:
                out = opener.open(request)
        except Exception, e:
                self.log.debug('HTTP call failed with error %s' %e)
                return None  # Is this OK

        self.log.debug('Leaving with output %s' %out)
        return out

