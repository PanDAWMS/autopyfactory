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
        :param data: the data to be recorded
        :param is_raw boolean: indicates if the object is primary or it is composed by other StatusInfo objects
        :param timestamp: the time when this object was created
        """ 

        self.log = logging.getLogger('autopyfactory')
        self.is_raw = is_raw
        self.data = data 
        if not timestamp:
            self.timestamp = int(time.time())
        else:
            self.timestamp = timestamp


    def group(self, analyzer):
        """
        groups the items recorded in self.data into a dictionary
        and creates a new StatusInfo object with it. 
           1. make a dictinary grouping items according to rules in analyzer
           2. convert that dictionary into a dictionary of StatusInfo objects
           3. make a new StatusInfo with that dictionary
        :param analyzer: an object implementing method group()
        :rtype StatusInfo:
        """
        # 1
        tmp_new_data = {} 
        for item in self.data:
            key = analyzer.group(item)
            if key:
                if key not in tmp_new_data.keys():
                    tmp_new_data[key] = []
                tmp_new_data[key].append(item) 
        # 2
        new_data = {}
        for k, v in tmp_new_data:
            new_data[k] = StatusInfo(v, True, self.timestamp)
        # 3
        new_info = StatusInfo(new_data, False, self.timestamp)
        return new_info


    def modify(self, analyzer):
        """
        modifies each item in self.data according to rules
        in analyzer
        :param analyzer: an object implementing method modify()
        :rtype StatusInfo:
        """
        new_data = []
        for item in self.data:
            new_item = analyzer.modify(item)
            new_data.append(new_item)
        new_info = StatusInfo(new_data, True, self.timestamp)
        return new_info


    def filter(self, analyzer):
        """
        eliminates the items in self.data that do not pass
        the filter implemented in analyzer
        :param analyzer: an object implementing method filter()
        :rtype StatusInfo:
        """
        new_data = []
        for item in self.data:
            if analyzer.filter(item):
                new_data.append(item)
        new_info = StatusInfo(new_data, True, self.timestamp)
        return new_info


    def get(self, *keys, analyzer=None):
        """
        returns the item in the tree structure pointed by all keys
        if analyzer is passed, the item is being processed first
        :param *keys: list of keys for each nested dictionary
        :param analyzer: a function that process the raw data, if needed.
        :rtype data:
        """
        if self.is_raw:
            if analyzer:
                return analyzer.process(self.data)
            else:
                return self.data
        else:
            statusinfo = self.data[keys[0]]
            return statusinfo.get(*keys[1:], analyzer)

