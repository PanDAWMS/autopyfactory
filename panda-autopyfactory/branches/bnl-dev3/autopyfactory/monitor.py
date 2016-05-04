# $Id$
#
# Monitoring system for autopyfactory, signals Monitoring
# webservice at each factory cycle with list of condor jobs

import commands
import logging
import pycurl
import re
import StringIO
import threading

try:
    import json as json
except ImportError, err:
    # Not critical (yet) - try simplejson
    import simplejson as json

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
        """
        data = (nick, self.fid, label, text[:140])
        self.msglist.append(data)

    def shout(self, cycleNumber):
        """
        Send information blob to webservice
        """
        msg = 'End of factory cycle: %d' % cycleNumber
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

