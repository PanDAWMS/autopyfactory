#!/usr/bin/python

"""
Cronjob on condor host
1. get list of CID for stale jobs (this factory only)
2. notify webservice, include AWOL jobs
3. quit
"""

import csv
import commands
import itertools
import logging
import pycurl
import sys
import StringIO
import time
from itertools import izip, chain, repeat
from optparse import OptionParser
try:
    import json as json
except ImportError, err:
    import simplejson as json

_THISFID = 'peter-UK-devel'
_BASEURL = 'http://py-dev.lancs.ac.uk/mon/'
# url to report awol jobs
_AWOLURL = _BASEURL + 'awol/'
# url to retrieve old stale jobs
_OLDURL = _BASEURL + 'old/' + _THISFID
# url to report stale jobs but only after status update
_STALEURL = _BASEURL + 'stale/'

class Signal:
    """
    handle posting of payload to URL
    """
    def __init__(self, url):
        self.buffer = StringIO.StringIO()
        self.url = url
        self.curl = pycurl.Curl()
        self.curl.setopt(pycurl.WRITEFUNCTION, self.buffer.write)
        self.curl.setopt(pycurl.URL, self.url)
        self.curl.setopt(pycurl.POST, 1)
        self.curl.setopt(pycurl.SSL_VERIFYPEER, 0)
        self.curl.setopt(pycurl.CONNECTTIMEOUT, 5)
        self.curl.setopt(pycurl.TIMEOUT, 10)
    
    def post(self, postdata):
        self.curl.setopt(pycurl.POSTFIELDS, postdata)
        # write at start of buffer
        self.buffer.seek(0)
        try:
            self.curl.perform()
        except pycurl.error,e:
            msg = "Curl error: %s" % e
            logging.error(msg)
        # truncate at current position
        self.buffer.truncate()
        if self.curl.getinfo(pycurl.HTTP_CODE) != 200:
            msg = "failed: %s%s" % (self.url, postdata)
            logging.debug(msg)
            msg = "HTTP_CODE=%s" % self.curl.getinfo(pycurl.HTTP_CODE)
            logging.debug(msg)
            # read from start of buffer
            self.buffer.seek(0)
            logging.debug(self.buffer.read())
            return
            
        msg = "%s %s" % (self.url, postdata)
        logging.debug(msg)
        # read from start of buffer
        self.buffer.seek(0)
        msg = self.buffer.read()
        logging.debug(msg)

    def get(self):
        """
        send HTTP_GET to url and return the list
        """
        self.curl.setopt(pycurl.URL, self.url)
        try:
            self.curl.perform()
        except pycurl.error:
            msg = "Problem contacting server: %s" % self.url
            logging.error(msg)
            return []
        self.buffer.seek(0)
        if self.curl.getinfo(pycurl.HTTP_CODE) != 200:
            msg = "failed: %s" % self.url
            logging.debug(msg)
            msg = "HTTP_CODE=%s" % self.curl.getinfo(pycurl.HTTP_CODE)
            logging.debug(msg)
            # read from start of buffer
            self.buffer.seek(0)
            logging.debug(self.buffer.read())
            return []
    
        rows = []
        for row in csv.reader(self.buffer):
            cid = row[0]
            rows.append(cid)

        return rows

class CondorJob:
    def __init__(self):
        self.json = json.JSONEncoder()
        self.stale = []    # raw list of stale jobs
        self.outputs = []  # raw list of condor output
        self.awol = []     # refined list, awol only, no condor record
        self.pending = []  # refined list, condor has record

    def query(self):
        """
        get list of stale jobs from DB
        """
        s = Signal(_OLDURL)
        self.stale = s.get()

        logging.info("OLD jobs: %d" % len(self.stale))
        
    def qcondor(self):
        """
        take list of stale jobs from webservice and get latest status
        create AWOL list if no job record found
        """  
    
        form = '-format "cid=%s." ClusterId -format "%s " ProcId'
        form += ' -format "jobstate=%d " JobStatus -format "globusstate=%d " GlobusStatus'
        form += ' -format "gk=%s" MATCH_gatekeeper_url -format "-%s\\n" MATCH_queue' 
        
        for cid in self.stale:
            cmd = "condor_q %s %s" % (form, cid)
            (exitcode, output) = commands.getstatusoutput(cmd)
            logging.debug("condor_q %s: %s" % (cid, output))
            if output:
                self.outputs.append(output)
            else:
                cmd = "condor_history -backwards %s %s" % (form, cid)
                (exitcode, output) = commands.getstatusoutput(cmd)
                logging.debug("condor_history %s" % output)
                if output:
                    self.outputs.append(output)
                else:
                    # cid not found by condor_q or condor_history
                    self.awol.append(cid)
    
        logging.info("STALE jobs: %d" % len(self.outputs))
        logging.info("AWOL jobs: %d" % len(self.awol))

    def parse(self):
        """
        parse condor output and return dict 
        keys: gk, jobstate, globusstate, cid
        """
        for line in self.outputs:
            items = line.split()
            values = {}
            for item in items:
                try:
                    (key, value) = item.split('=', 1)
                    values[key] = value
                except ValueError:
                    logging.warn('Bad condor output: %s' % line)
                    continue

            self.pending.append(values)

        logging.info("PENDING jobs: %d" % len(self.pending))

    def updatepending(self):
        """
        update pending jobs via webservice
        """
        if not self.pending: return
        s = Signal(_STALEURL)

        chunksize = 1000
        ilists = izip(*[chain(self.pending, repeat(None,chunksize-1))]*chunksize)
        for ilist in ilists:
            jsonmsg = self.json.encode(ilist)
            postdata = "fid=%s&data=%s" % (_THISFID, jsonmsg)
            s.post(postdata)

    def updateawol(self):
        """
        update AWOL jobs via webservice
        """
        if not self.awol: return
        s = Signal(_AWOLURL)
        jsonmsg = self.json.encode(self.awol)
        postdata = "fid=%s&data=%s" % (_THISFID, jsonmsg)
        s.post(postdata)

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-q", action="store_true", default=False,
                      help="quiet mode", dest="quiet")
    parser.add_option("-d", action="store_true", default=False,
                      help="debug mode", dest="debug")
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")
        return 1
    loglevel = 'INFO'
    if options.quiet:
        loglevel = 'WARNING'
    if options.debug:
        loglevel = 'DEBUG'

    logger = logging.getLogger()
    logger.setLevel(logging._levelNames[loglevel])
    fmt = '[APFMON:%(levelname)s %(asctime)s] %(message)s'
    formatter = logging.Formatter(fmt, '%T')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    j = CondorJob()
    # get stale list from webservice
    j.query()
    # get latest condor update j.stale -> j.outputs and j.awol
    j.qcondor()
    # parse j.outputs -> j.pending
    j.parse()
    # signal updated states
    j.updatepending()
    # signal awol jobs
    j.updateawol()

if __name__ == "__main__":
    sys.exit(main())
