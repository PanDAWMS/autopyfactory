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
        new_data = {} 
        # assuming for now that data is list of dicts
        for item in self.data:
            key, value = analyzer.aggregate(item)
            if key not in new_data.keys():
                new_data[key] = []
            new_data[key].append(value) 
        new_info = StatusInfo(new_data)
        new_info.timestamp = self.timestamp
        return new_info


    def modify(self, analyzer):
        """
        :param analyzer: an object implementing method modify()
        """
        new_data = []
        # assuming for now that data is list of dicts
        for item in self.data:
            new_item = analyzer.modify(item)
            new_data.append(new_item)
        new_info = StatusInfo(new_data)
        new_info.timestamp = self.timestamp
        return new_info


    def filter(self, analyzer):
        """
        :param analyzer: an object implementing method filter()
        """
        new_data = []
        # assuming for now that data is list of dicts
        for item in self.data:
            if analyzer.filter(item):
                new_data.append(item)
        new_info = StatusInfo(new_data)
        new_info.timestamp = self.timestamp
        return new_info

