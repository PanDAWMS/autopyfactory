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

try:
        import pycurl
except ImportError:
        log = logging.getLogger('main.monitor')
        log.error('module pycurl is not installed. Aborting.')
        import sys
        sys.exit()

try:
        import json as json
except ImportError, err:
        # Not critical (yet) - try simplejson
        log = logging.getLogger('main.monitor')
        log.warning('json package not installed. Trying to import simplejson as json')
        import simplejson as json

__author__ = "Peter Love, Jose Caballero"
__copyright__ = "2010,2011 Peter Love; 2011 Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

_CIDMATCH = re.compile('\*\* Proc (\d+\.\d+)', re.M)

class Monitor(threading.Thread):
    """
    Notifies a monitoring webservice about condor jobs
    """
    def __init__(self, **args):
        self.log = logging.getLogger('main.monitor')
        mainlevel = logging.getLogger('main').getEffectiveLevel()
        self.log.setLevel(mainlevel)

        msg = "ARGS: %s" % str(args)
        self.log.debug(msg)

        try:
            monurl = args['monitorURL']
            self.fid = args['factoryId']
        except:
            raise

        self.crurl = monurl + 'c/'
        self.msgurl = monurl + 'm/'
        self.furl = monurl + 'h/'
        self.crlist = []
        self.msglist = []
        self.json = json.JSONEncoder()
        self.buffer = StringIO.StringIO()
        self.c = pycurl.Curl()
        self.c.setopt(pycurl.WRITEFUNCTION, self.buffer.write)
        self.c.setopt(pycurl.SSL_VERIFYPEER, 0)
        self.c.setopt(pycurl.CONNECTTIMEOUT, 5)
        self.c.setopt(pycurl.TIMEOUT, 10)
        self.c.setopt(pycurl.FOLLOWLOCATION, 1)
        #### BEGIN TEST ###
        if args.has_key('HTTPproxy'):
                self.c.setopt(pycurl.PROXY, args['HTTPproxy'])
        if args.has_key('HTTPproxyport'):
                self.c.setopt(pycurl.PROXYPORT, int(args['HTTPproxyport']))
        #### END TEST ###

        self.log.debug('Instantiated monitor')

        arglist = ["%s=%s" % (k, v) for k, v in args.items()]
        data = '&'.join(arglist)
        self._signal(self.furl, data)

    def _signal(self, url, postdata):
        """
        handle posting of payload to URL
        """

        self.c.setopt(pycurl.URL, url)
        self.c.setopt(pycurl.POST, 1)
        self.c.setopt(pycurl.POSTFIELDS, postdata)
        try:
            self.c.perform()
            if self.c.getinfo(pycurl.HTTP_CODE) != 200:
                msg = "url: %s post: %s" % (url, postdata)
                self.log.error(msg)
                self.buffer.seek(0)
                self.log.error(self.buffer.read())
                self.buffer.seek(0)
                return
        
        except pycurl.error, e:
            msg = "PyCurl server problem:", e[1]
            self.log.warn(msg)
        
        msg = "%s" % url
        self.log.debug(msg)
        self.buffer.seek(0)
    
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

