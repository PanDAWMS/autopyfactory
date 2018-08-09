#!/usr/bin/env python
# $Id: monitor.py 7686 2011-04-08 21:15:43Z jhover $
#

"""
        PUT HERE SOME DOC 
"""

import commands
import logging
import re
import threading
import StringIO
import urllib
import urllib2

from autopyfactory.interfaces import MonitorInterface

try:
    import json as json
except ImportError, err:
    # Not critical (yet) - try simplejson
    log = logging.getLogger('autopyfactory.monitor')
    log.debug('json package not installed. Trying to import simplejson as json')
    import simplejson as json


#  ==================================================
#
#       CLASSES TO HANDLE HTTP CALLS BETTER
#
#  ==================================================


class NoExceptionHTTPHandler(urllib2.BaseHandler):
    """
    A substitute/supplement to urllib2.HTTPErrorProcessor
    that doesn't raise exceptions on status codes 201,204,206
    For example, when a registration operation (via HTTP PUT command)
    is successful, a code 201 CREATED is returned. 
    urllib2 interprets that as an ERROR, and raises an exception. 
    To avoid it we override the behavior of http_err_<RC> methods.
    """

    def http_error_201(self, request, response, code, msg, hdrs):
        return response
    def http_error_204(self, request, response, code, msg, hdrs):
        return response
    def http_error_206(self, request, response, code, msg, hdrs):
        return response


class RequestWithMethod(urllib2.Request):
    """
    To be used insted of urllib2.Request
    This class gets the HTTP method (i.e. 'PUT') during the initlization
    """

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method


#  ==================================================
# This class is just to make the name of the HTTP
# methods ("POST", "GET", etc) to look like macros

class http:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"  
    
    
#  ==================================================


class _apf(MonitorInterface):
    """
    Notifies a monitoring webservice about condor jobs
    """

    def __init__(self, apfqueue, config, section):

        #monitor_id = config.generic_get(section, "monitorsection")
        monitor_id = section
        self.log = logging.getLogger('autopyfactory.monitor.%s' %apfqueue.apfqname)
        self.log.debug("Start...")

        self.qcl = apfqueue.factory.qcl
        self.fcl = apfqueue.factory.fcl
        self.mcl = apfqueue.factory.mcl
    
        self.fid = self.fcl.generic_get('Factory','factoryId')
        self.version = apfqueue.factory.version
        self.email = self.fcl.generic_get('Factory','factoryAdminEmail')
        self.baselogurl = self.fcl.generic_get('Factory','baseLogDirUrl')

        self.monurl = self.mcl.generic_get(monitor_id, 'monitorURL')

        self.log.debug('Instantiated monitor')
        self.registerFactory()     
        self.registeredlabels = self._getLabels() # list of labels registered
        self.log.debug('Done.')


    def registerFactory(self):
        """
        First check if the factory is already registered. 
        If not, then register it. 
        """

        self.log.debug('Starting')

        #if self._isFactoryRegistered():
        #    self.log.debug('factory is already registered')
        #    out = None
        #else:
        #    self.log.info('factory is not registered yet. Registering.')
        #    out = self._registerFactory()
        out = self._registerFactory()

        self.log.debug('Leaving')
        return out
      

    def _isFactoryRegistered(self):
        """
        queries for the list of factories.
        URL looks like http://py-front.lancs.ac.uk/api/factories
        Output of query looks like (as JSON string):

        [
          {
            "active": true, 
            "email": "admin1@matrix.net", 
            "factory_type": "AutoPyFactory", 
            "id": 137, 
            "ip": "127.0.0.1", 
            "last_cycle": 0, 
            "last_modified": "2013-03-28T13:19:56Z", 
            "last_ncreated": 0, 
            "last_startup": "2013-03-28T13:19:56Z", 
            "name": "dev-factory", 
            "url": "http://localhost/", 
            "version": "0.0.1"
          }, 
          {
            "active": true, 
            "email": "admin2@matrix.net", 
            "factory_type": "AutoPyFactory", 
            "id": 5, 
            "ip": "130.199.3.165", 
            "last_cycle": 0, 
            "last_modified": "2013-03-25T19:33:57Z", 
            "last_ncreated": 0, 
            "last_startup": "2013-03-25T19:33:57Z", 
            "name": "bnl-gridui99-factory", 
            "url": "http://gridui99.usatlas.bnl.gov:25880", 
            "version": "0.0.1"
          }, 
          ]
        """

        url = self.monurl + '/factories'
        out = self._call(http.GET, url)
        out = json.loads(out.read())
        factories = [ factory['name'] for factory in out ] 
        self.log.debug('list of registered factories = %s' %factories)
        
        return self.fid in factories


    def _registerFactory(self):
        """
        register the factory
        """

        self.log.debug('Starting')

        url = self.monurl + '/factories/' + self.fid

        data = {}
        data["version"] = self.version
        data["email"] = self.email
        data["url"] =  self.baselogurl
        data = json.dumps(data)
        out = None
        try:
            out = self._call(http.PUT, url, data)
        except Exception, e:
            self.log.error('Unable to contact monitor')
        self.log.debug('Leaving')
        return out


    def _getLabels(self):
        """
        queries for the list of labels registered for this factory.
        URL looks like
            http://py-front.lancs.ac.uk/api/labels?factory=bnl-gridui99-factory
        Output of query looks like (as JSON string):
            [
              {
                "factory": "bnl-gridui99-factory", 
                "id": 512, 
                "last_modified": "2013-03-28T14:47:20Z", 
                "localqueue": "", 
                "msg": "", 
                "name": "label-1", 
                "ncreated": 100, 
                "ndone": 0, 
                "nexiting": 0, 
                "nfault": 0, 
                "nrunning": 50, 
                "resource": ""
              }, 
              {
                "factory": "bnl-gridui99-factory", 
                "id": 513, 
                "last_modified": "2013-03-28T15:02:56Z", 
                "localqueue": "", 
                "msg": "", 
                "name": "label-2", 
                "ncreated": 0, 
                "ndone": 0, 
                "nexiting": 0, 
                "nfault": 0, 
                "nrunning": 0, 
                "resource": ""
              }
            ]
        """

        self.log.debug('Starting')

        url = self.monurl + '/labels?factory=' + self.fid
        labels = []
        try:
            out = self._call(http.GET, url)
            out = json.loads(out.read())
            labels = [ label['name'] for label in out ] 
            self.log.debug('list of registered labels = %s' %labels)
        except Exception, e:
            self.log.error('Problem querying monitor.')
            labels = ['unkown']
        
        self.log.debug('Leaving')
        return labels


    def registerLabel(self, apfqueue):
        """
        First check if the label is already registered. 
        If not, then register it. 

        Label is the name of the section in queues.conf

        We pass apfqueue as input because this class is a singleton,
        so the apfqueue object passed by __init__() may not be the same 
        apfqueue object calling this method. 
        """

        #####################################################
        #
        #   QUESTION:
        #
        #       We are registering a new label
        #       when registering jobs.
        #       So new labels are registered one by one
        #       if needed.
        #
        #       Should be done at the __init__() at once?
        #       Like getting current list, get all labels 
        #       from qcl, and register all missing ones.
        #
        #####################################################

        self.log.debug('Starting')

        label = apfqueue.apfqname

        if self._isLabelRegistered(label):
            self.log.debug('label %s is already registered' %label)
            out = None
        else:
            self.log.info('label %s is not registered yet. Registering.' %label)
            out = self._registerLabel(apfqueue)

        self.log.debug('Leaving')
        return out


    def _isLabelRegistered(self, label):
        return label in self.registeredlabels


    def _registerLabel(self, apfqueue):
        """
        We pass apfqueue as input because this class is a singleton,
        so the apfqueue object passed by __init__() may not be the same 
        apfqueue object calling this method. 
        """

        self.log.debug('Starting')

        url = self.monurl + '/labels'

        data = [] 

        label = {}
        label['name'] = apfqueue.apfqname
        label['factory'] = self.fid
        label['wmsqueue'] = '' 
        label['batchqueue'] = ''
        label['resource'] = '' 
        label['localqueue'] = '' 

        data.append(label)
        data = json.dumps(data)

        out = self._call(http.PUT, url, data)

        self.registeredlabels.append(apfqueue.apfqname)

        self.log.debug('Leaving')
        return out


    def registerJobs(self, apfqueue, jobinfolist):
        """
        Take a list of JobInfo objects and translate to APFMonitor messages.

        We pass apfqueue as one of the inputs because this class is a singleton,
        so the apfqueue object passed by __init__() may not be the same 
        apfqueue object calling this method. 

        jobinfolist is the output of submit() method.
        It is a list of JobInfo objects
        """

        self.log.debug('Starting for apfqueue %s with info list %s' %(apfqueue.apfqname, 
                                                                     jobinfolist))

        url = self.monurl + '/jobs'

        # jobs can not be registered unless the label is already registered
        self.registerLabel(apfqueue)
        
        out = None

        if jobinfolist:
        # ensure jobinfolist has any content, and is not None
            apfqname = apfqueue.apfqname

            data = [] 

            for ji in jobinfolist:

                job = {}
                
                job['cid'] = ji.jobid 
                job['label'] = apfqname
                job['factory'] = self.fid 

                data.append(job)

                self.log.debug('updateJobs: adding data (%s, %s, %s)' %(ji.jobid, self.fid, apfqname))

            data = json.dumps(data) 
            out = self._call(http.PUT, url, data=data)

        self.log.debug('Leaving.')
        return out


#    def updateLabel(self, label, n):
#        """
#        update each label (==apfqname) in the monitor
#        n is the number of new pilots being submitted
#        """
#
#        self.log.debug('Starting for label %s and number of pilots %s' %(label, n))
#
#        url = "%s/labels/%s:%s" %(self.monurl, self.fid, label)
#        msg = "Attempt to submit %s pilots" %n  
#        data = {'status': msg}
#        data = urllib.urlencode(data)
#        self._call(http.POST, url, data=data) 
#
#        self.log.debug('Leaving')

    def updateLabel(self, label, msg):
        """
        update each label (==apfqname) in the monitor
        """

        self.log.debug('Starting for label %s and message %s' %(label, msg))

        url = "%s/labels/%s:%s" %(self.monurl, self.fid, label)
        data = {'status': msg}
        data = urllib.urlencode(data)
        self._call(http.POST, url, data=data) 

        self.log.debug('Leaving')


    def _call(self, method, url, data=None):
        """
        make the HTTP call
        method is "PUT", "GET", "POST" or "DELETE"
        """

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
            out = None  # Is this OK?

        self.log.debug('Leaving with output %s' %out)
        return out


# =============================================================================
#   Singleton wrapper
# =============================================================================

class APF(object):

    instances = {}

    def __new__(cls, *k, **kw):
    
        # ---------------------------
        # get the id:
        #
        # for APF monitor plugins, 
        # the id is the name of the section
        # in monitor.conf 
        apfqueue = k[0]
        conf = k[1]
        section = k[2]
        id = conf.generic_get(section, 'monitorsection')
        # ---------------------------
        
        if not id in APF.instances.keys():
            APF.instances[id] = _apf(*k, **kw)
        return APF.instances[id]
         
