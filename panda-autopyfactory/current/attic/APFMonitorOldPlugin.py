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

_CIDMATCH = re.compile('\*\* Proc (\d+\.\d+)', re.M)

class APFMonitorOldPlugin(MonitorInterface):

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


    def updateJobs(self, apfqueue, jobinfolist ):
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

    def registerFactory(self):
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
        

    def _signal(self, url, postdata):
        
        self.log.debug('_signal: starting with url %s and postdata %s' %(url, postdata))
        try:
            out = urllib2.urlopen(url, postdata)
            self.log.debug('_signal: urlopen() output=%s' % out.read())
            self.log.debug('_signal: urlopen() OK.')
        except Exception, ex: 
            self.log.debug('_signal: urlopen() failed and raised exception %s' % ex)
            


    def _parse(self, output):
        # return a list of condor job id
        try:
            return _CIDMATCH.findall(output)
        except:
            return []

    def notify(self, nick, label, output):
        """
        Record creation of the condor job

        nick = nickname
        label = queue (what is in [] in the config file)
        output = output of command condor_submit

        """
        msg = "nick: %s, fid: %s, label: %s" % (nick, self.fid, label)
        self.log.debug(msg)

        joblist = self._parse(output)

        msg = "Number of CID found: %d" % len(joblist)
        self.log.debug(msg)

        for cid in joblist:
            data = (cid, nick, self.fid, label)
            self.crlist.append(data)

    def msg(self, nick, label, text):
        """
        Send the latest factory message to the monitoring webservice

        nick = nickname
        label = queue (what is in [] in the config file)
        text = message to be sent to the monitor server

        """
        data = (nick, self.fid, label, text[:140])
        self.msglist.append(data)

    def shout(self, label, cycleNumber):
        """
        Send information blob to webservice

        label = queue (what is in [] in the config file)
        cycleNumber = cycle number
        """

        msg = 'End of queue %s cycle: %d' % (label, cycleNumber)
        self.log.debug(msg)
        msg = 'msglist length: %d' % len(self.msglist)
        self.log.debug(msg)
        msg = 'crlist length: %d' % len(self.crlist)
        self.log.debug(msg)

        jsonmsg = self.json.encode(self.msglist)
        txt = "cycle=%s&data=%s" % (cycleNumber, jsonmsg)
        self._signal(self.msgurl, txt)

        jsonmsg = self.json.encode(self.crlist)
        txt = "data=%s" % jsonmsg
        self._signal(self.crurl, txt)

        self.msglist = []
        self.crlist = []
        
    # Monitor-releated methods moved from factory.py

    def _monitor_shout(self):
        '''
        call monitor.shout() method
        '''

        self.log.debug("__monitor_shout: Starting.")
        if hasattr(self, 'monitor'):
            self.shout(self.apfqname, self.cyclesrun)
        else:
            self.log.debug('__monitor_shout: no monitor instantiated')
        self.log.debug("__monitor_shout: Leaving.")

    def _monitor_note(self, msg):
        '''
        collects messages for the Monitor
        '''

        self.log.debug('__monitor_note: Starting.')

        if hasattr(self, 'monitor'):
            nick = self.qcl.get(self.apfqname, 'batchqueue')
            self.msg(nick, self.apfqname, msg)
        else:
            self.log.debug('__monitor_note: no monitor instantiated')
                
        self.log.debug('__monitor__note: Leaving.')

    def _monitor_notify(self, output):
        '''
        sends all collected messages to the Monitor server
        '''

        self.log.debug('__monitor_notify: Starting.')

        if hasattr(self, 'monitor'):
            nick = self.qcl.get(self.apfqname, 'batchqueue')
            label = self.apfqname
            self.notify(nick, label, output)
        else:
            self.log.debug('__monitor_notify: no monitor instantiated')

        self.log.debug('__monitor_notify: Leaving.')



