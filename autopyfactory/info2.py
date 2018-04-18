#! /usr/bin/env python

import datetime
import logging
import logging.handlers
import threading
import time
import traceback
import os
import pwd
import sys

from pprint import pprint

from autopyfactory.apfexceptions import FactoryConfigurationFailure, CondorStatusFailure, PandaStatusFailure
from autopyfactory.logserver import LogServer

major, minor, release, st, num = sys.version_info


class StatusInfo(object):
    """
    """

    def __init__(self, data):
        """ 
        :param data: the initial set of data
        """ 
        self.log = logging.getLogger('autopyfactory')
        self.timestamp = int(time.time())
        self.data = data 


    def aggregate(self, analyzer):
        """
        :param analyzer: an object implementing method aggregate()
        """
        newdata = {} 
        # assuming for now that data is list of dicts
        for i in self.data:
            key, value = analyzer.aggregate(i)
            if key not in newdata.keys():
                newdata[key] = []
            newdata[key].append(value) 

        newinfo = StatusInfo(newdata)
        return newinfo


    def modify(self, analyzer):
        """
        :param analyzer: an object implementing method modify()
        """
        newdata = []
        # assuming for now that data is list of dicts
        for i in self.data:
            new_i = analyzer.modify(i)
            newdata.append(new_i)
        newinfo = StatusInfo(newdata)
        return newinfo


    def filter(self, analyzer):
        """
        :param analyzer: an object implementing method filter()
        """
        newdata = []
        # assuming for now that data is list of dicts
        for i in self.data:
            if analyzer.filter(i):
                newdata.append(i)
        newinfo = StatusInfo(newdata)
        return newinfo

