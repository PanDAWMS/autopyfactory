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

    def __init__(self, data, is_raw=True, timestamp=None):
        """ 
        :param data: the initial set of data
        """ 
        self.log = logging.getLogger('autopyfactory')
        self.is_raw = is_raw
        self.data = data 
        if not timestamp:
            self.timestamp = int(time.time())
        else:
            self.timestamp = timestamp


    def aggregate(self, analyzer):
        """
        :param analyzer: an object implementing method aggregate()
        """
        #new_data = {} 
        #for item in self.data:
        #    key, value = analyzer.aggregate(item)
        #    if key not in new_data.keys():
        #        new_data[key] = []
        #    new_data[key].append(value) 
        #new_info = StatusInfo(new_data, False, self.timestamp)
        #return new_info
        tmp_new_data = {} 
        for item in self.data:
            key, value = analyzer.aggregate(item)
            if key not in tmp_new_data.keys():
                tmp_new_data[key] = []
            tmp_new_data[key].append(value) 
        new_data = {}
        for k, v in tmp_new_data:
            new_data[k] = StatusInfo(v, True, self.timestamp)
        new_info = StatusInfo(new_data, False, self.timestamp)
        return new_info


    def modify(self, analyzer):
        """
        :param analyzer: an object implementing method modify()
        """
        new_data = []
        for item in self.data:
            new_item = analyzer.modify(item)
            new_data.append(new_item)
        new_info = StatusInfo(new_data, True, self.timestamp)
        return new_info


    def filter(self, analyzer):
        """
        :param analyzer: an object implementing method filter()
        """
        new_data = []
        for item in self.data:
            if analyzer.filter(item):
                new_data.append(item)
        new_info = StatusInfo(new_data, True, self.timestamp)
        return new_info


    def get(self, *keys, process=None):
        """
        :param *keys: list of keys for each nested dictionary
        :param process: a function that process the raw data, if needed.
        """

