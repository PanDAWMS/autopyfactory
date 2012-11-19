#!/usr/bin/env python
# $Id: monitor.py 7686 2011-04-08 21:15:43Z jhover $
#

'''Monitoring system for autopyfactory, signals Monitoring
 webservice at each factory cycle with list of condor jobs
'''

import commands
import logging
import re
import threading
import StringIO
import urllib2

from autopyfactory.factory import Singleton, singletonfactory
from autopyfactory.interfaces import MonitorInterface

###try:
###    import pycurl
###except ImportError:
###    log = logging.getLogger('main.monitor')
###    log.error('module pycurl is not installed. Aborting.')
###    import sys
###    sys.exit()

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

class APFMonitorPlugin(MonitorInterface):

    __metaclass__ = singletonfactory(id_var="monitor_id")


    """
    Notifies a monitoring webservice about condor jobs
    """
    def __init__(self, apfqueue, monitor_id):
        '''
        apfqueue is a reference to the APFQueue object creating this plugin.

        monitor_id is the value for id_var (input of the singletonfactory)
        to decide if a new object has to be really created or not.
        
        Also sends initial ping to monitor server. 
        
        '''
        self.log = logging.getLogger('main.monitor')
        mainlevel = logging.getLogger('main').getEffectiveLevel()
        self.log.setLevel(mainlevel)
        self.log.debug("Start...")


        self.apfqname = apfqueue.apfqname
        self.qcl = apfqueue.factory.qcl
        self.fcl = apfqueue.factory.fcl
        self.mcl = apfqueue.factory.mcl
    
        self.monurl = self.fcl.generic_get('Factory','monitorURL')
        self.fid = self.fcl.generic_get('Factory','factoryId')
        self.version = self.fcl.generic_get('Factory', 'versionTag')
        self.email = self.fcl.generic_get('Factory','factoryAdminEmail')
        self.baselogurl = self.fcl.generic_get('Factory','baseLogDirUrl')
###        self.proxyarg = self.fcl.generic_get('Factory', 'HTTPproxy', default_value=None)
###        self.proxyportarg = self.fcl.generic_get('Factory', 'HTTPproxyport', default_value=None)

        self.owner = self.email
        
        self.crurl = self.monurl + 'c/'
        self.msgurl = self.monurl + 'm/'
        self.furl = self.monurl + 'h/'
        
        self.crlist = []
        self.msglist = []
        
        self.json = json.JSONEncoder()
        self.buffer = StringIO.StringIO()
        
###        # Set up PyCurl
###        self.c = pycurl.Curl()
###        self.c.setopt(pycurl.WRITEFUNCTION, self.buffer.write)
###        self.c.setopt(pycurl.SSL_VERIFYPEER, 0)
###        self.c.setopt(pycurl.CONNECTTIMEOUT, 5)
###        self.c.setopt(pycurl.TIMEOUT, 10)
###        self.c.setopt(pycurl.FOLLOWLOCATION, 1)
###        if self.proxyarg:
###            self.c.setopt(pycurl.PROXY, proxyarg)
###        if self.proxyportarg:
###            proxyportarg = int(proxyportarg)
###            self.c.setopt(pycurl.PROXYPORT, proxyportarg)

        self.log.debug('Instantiated monitor')

        attrlist = []
        attrlist.append("factoryId=%s" % self.fid)
        attrlist.append("factoryOwner=%s" % self.owner)
        attrlist.append("versionTag=%s" % self.version)
        attrlist.append("factoryAdminEmail=%s" % self.email)
        attrlist.append("baseLogDirUrl=%s" % self.baselogurl)
                
        data = '&'.join(attrlist)        
        self._signal(self.furl, data)
             
        self.log.debug('Done.')


    def updateJobStatus(self, jobinfolist ):
        '''
        Take list of JobInfo objects and translate to APFMonitor messages.
        '''


   
    def _signal(self, url, postdata):
        
        self.log.debug('_signal: url is %s and postdata is %s' %(url, postdata))
        try:
            out = urllib2.urlopen(url, postdata)
            self.log.debug('_signal: urlopen() output=%s' % out.read())
        except Exception, ex: 
            self.log.debug('_signal: urlopen() failed and raised exception %s' %ex)
        self.log.debug('_signal: urlopen() OK.')
        
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
            self.monitor.shout(self.apfqname, self.cyclesrun)
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
            self.monitor.msg(nick, self.apfqname, msg)
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
            self.monitor.notify(nick, label, output)
        else:
            self.log.debug('__monitor_notify: no monitor instantiated')

        self.log.debug('__monitor_notify: Leaving.')



###    #def _signal(self, url, postdata):
###    def old_signal(self, url, postdata):
###        """
###        handle posting of payload to URL
###        """
###
###        self.c.setopt(pycurl.URL, url)
###        self.c.setopt(pycurl.POST, 1)
###        self.c.setopt(pycurl.POSTFIELDS, postdata)
###        try:
###            self.c.perform()
###            if self.c.getinfo(pycurl.HTTP_CODE) != 200:
###                msg = "url: %s post: %s" % (url, postdata)
###                self.log.error(msg)
###                self.buffer.seek(0)
###                self.log.error(self.buffer.read())
###                self.buffer.seek(0)
###                return
###        
###        except pycurl.error, e:
###            msg = "PyCurl server problem:", e[1]
###            self.log.debug(msg)
###        # Catch other errors, e.g. URLError. 
###        except Exception, e:
###            self.log.error("Caught exception: %s " % str(e))
###        
###        msg = "%s" % url
###        self.log.debug(msg)
###        self.buffer.seek(0)
