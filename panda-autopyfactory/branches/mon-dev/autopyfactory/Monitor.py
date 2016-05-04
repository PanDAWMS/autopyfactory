# $Id: $
#
# Monitoring system for autopyfactory, signals Monitoring
# webservice on each successful condor_submit

import commands
import logging
import pycurl
import re
import StringIO
_CIDMATCH = re.compile('\*\* Proc (\d+\.\d+)', re.M)

class Monitor:
    """
    Notifies a monitoring webservice about condor jobs
    """
    def __init__(self, fid=None, monurl=None, loglevel='DEBUG'):
        self.log = logging.getLogger('main.mon')
        mainlevel = logging.getLogger('main').getEffectiveLevel()
        self.log.setLevel(mainlevel)

        if not fid:
            msg = 'No factory ID defined'
            self.log.error(msg)
            return 
        else:
            self.fid = fid

        if not monurl:
            msg = 'No monitoring URL defined'
            self.log.error(msg)
            return 

        self.crurl = monurl + 'cr/'
        self.msgurl = monurl + 'msg/'
        self.buffer = StringIO.StringIO()
        self.c = pycurl.Curl()
        self.c.setopt(pycurl.WRITEFUNCTION, self.buffer.write)
        self.c.setopt(pycurl.POST, 1)
        self.c.setopt(pycurl.SSL_VERIFYPEER, 0)
        self.c.setopt(pycurl.CONNECTTIMEOUT, 5)
        self.c.setopt(pycurl.TIMEOUT, 10)
        self.c.setopt(pycurl.FOLLOWLOCATION, 1)

        self.log.debug('Instantiated monitor')

    def _signal(self, url, postdata):
        """
        handle posting of payload to URL
        """

        self.c.setopt(pycurl.URL, url)
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
        
        msg = "%s: %s" % (url, postdata)
        self.log.debug(msg)
        self.buffer.seek(0)
        self.log.debug(self.buffer.read())
        self.buffer.seek(0)
    
    def _parse(self, output):
        # return a list of condor job id
        try:
            return _CIDMATCH.findall(output)
        except:
            return []

    def notify(self, nick, label, output):
        msg = "nick: %s, fid: %s, label: %s" % (nick, self.fid, label)
        self.log.debug(msg)

        joblist = self._parse(output)

        msg = "Number of CID found: %d" % len(joblist)
        self.log.debug(msg)

        for cid in joblist:
            txt = "cid=%s&nick=%s&fid=%s&label=%s" % (cid, nick, self.fid, label)
            self._signal(self.crurl, txt)

    def msg(self, nick, label, text):
        """
        Send the latest factory message to the monitoring webservice
        """

        txt = "nick=%s&fid=%s&label=%s&msg=%s" % (nick, self.fid, label, text[:140])
        self.log.debug(txt)
        self._signal(self.msgurl, txt)

