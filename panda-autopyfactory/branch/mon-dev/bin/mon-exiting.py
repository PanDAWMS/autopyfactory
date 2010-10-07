#!/usr/bin/python

"""
Cronjob on condor host to move job state from EXITING to 
and end-state (either DONE or FAULT)
1. get list of CID in state EXITING (this factory only)
2. check jobs with condor_q and condor_history
3. update states
4. quit
"""

import csv
import commands
import logging
import pycurl
import sys
import StringIO
import time
from optparse import OptionParser
_THISFID = 'peter-UK-devel'
_BASEURL = 'http://py-dev.lancs.ac.uk/mon/'
# url to update condor state
_STURL = _BASEURL + 'st/'
# url to get list of condorid in EXITING state
_CIDURL = _BASEURL + 'cid/' + _THISFID
_AWOLURL = _BASEURL + 'awol/'

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
    
    def post(self, postdata):
        self.curl.setopt(pycurl.POSTFIELDS, postdata)
        # write at start of buffer
        self.buffer.seek(0)
        self.curl.perform()
        # truncate at current position
        self.buffer.truncate()
        if self.curl.getinfo(pycurl.HTTP_CODE) != 200:
            msg = "failed: %s%s" % (self.url, postdata)
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

def states(line):
    # build list of current job states
    # states is a dict with keys: gk, jobstate, globusstate, cid
    states = []
    items = line.split()
    values = {}
    for item in items:
        try:
            (key, value) = item.split('=', 1)
            values[key] = value
        except ValueError:
            logging.warn('Bad condor_q output: %s' % line)
            continue

    return values

def updatestate(state):
    # update state via webservice
    s = Signal(_STURL)
#    gk = state['gk']
    cid = state['cid']
    js = state['jobstate']
    gs = state['globusstate']

    # js=1 (idle) js=2 (running)
    if js not in ['1','2']:
        postdata = "fid=%s&cid=%s&js=%s&gs=%s" % (_THISFID, cid, js, gs)
        s.post(postdata)
    else:
        msg='EXITING, condor running, slow middleware: %s_%s js=%s, gs=%s' % (_THISFID, cid, js, gs)
        logging.debug(msg)

def updateawol(cid):     
    # update AWOL jobs via webservice
    logging.warn("AWOL: %sjob/%s/%s" % (_BASEURL, _THISFID, cid))
    s = Signal(_AWOLURL)
    # simply tell webservice about awol jobs
    postdata = "fid=%s&cid=%s" % (_THISFID, cid)
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
    fmt = '[PYF:%(levelname)s %(asctime)s] %(message)s'
    formatter = logging.Formatter(fmt, '%T')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # get list of jobs from DB in state EXITING
    pending = []
    buffer = StringIO.StringIO()
    curl = pycurl.Curl()
    curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
    curl.setopt(pycurl.SSL_VERIFYPEER, 0)
    curl.setopt(pycurl.URL, _CIDURL)
    try:
        curl.perform()
    except pycurl.error:
        msg = "Problem contacting server: %s" % _CIDURL
        logging.error(msg)
        return
    buffer.seek(0)
    for row in csv.reader(buffer):
        cid = row[0]
        pending.append(cid)
    
    msg = "PENDING: %d" % len(pending)
    logging.debug(msg)

    form = '-format "cid=%s." ClusterId -format "%s " ProcId'
    form += ' -format "jobstate=%d " JobStatus -format "globusstate=%d " GlobusStatus'
    form += ' -format "gk=%s" MATCH_gatekeeper_url -format "-%s\\n" MATCH_queue' 
    
    # outputs is a list of condor_q info for each EXITING job
    outputs = []
    awolcids = []
    tstart = time.time()
    for cid in pending:
        cmd = "condor_q %s %s" % (form, cid)
        (exitcode, output) = commands.getstatusoutput(cmd)
        logging.debug("condor_q %s: %s" % (cid, output))
        if output:
            outputs.append(output)
        else:
            cmd = "condor_history -backwards %s %s" % (form, cid)
            (exitcode, output) = commands.getstatusoutput(cmd)
            logging.debug("condor_history %s: %s" % (cid, output))
            if output:
                outputs.append(output)
                state = states(output)
                updatestate(state)
            else:
                # cid not found by condor_q or condor_history
                awolcids.append(cid)
                updateawol(cid)

    tend = time.time()

    
    logging.debug("Current number of found jobs: %d" % len(outputs))
    logging.debug("Current number of AWOL jobs: %d" % len(awolcids))

if __name__ == "__main__":
    sys.exit(main())
