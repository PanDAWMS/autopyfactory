#
#

import logging
import logging.handlers
import sys
import time

def setuplogging():

        log = logging.getLogger('autopyfactory')
        logStream = logging.StreamHandler(sys.stdout)

        major, minor, release, st, num = sys.version_info
        if major == 2 and minor == 4:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d : %(message)s'
        else:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'

        formatter = logging.Formatter(FORMAT)
        formatter.converter = time.gmtime  # to convert timestamps to UTC
        logStream.setFormatter(formatter)
        log.addHandler(logStream)
        log.setLevel(logging.DEBUG)


class MockAPFQueue(object):
    """
     Used to build CondorEC2BatchStatusPlugin object for testing...
    """

    def __init__(self, apfqname):
        self.apfqname = apfqname




