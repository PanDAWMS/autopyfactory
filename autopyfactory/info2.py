#! /usr/bin/env python

import datetime
import inspect
import logging
import logging.handlers
import threading
import time
import traceback
import os
import pwd
import sys

from pprint import pprint


# =============================================================================
# Exceptions
# =============================================================================

class MethodMissing(Exception):
    def __init__(self, analyzer, methodname):
        basevalue = "Analyzer object {objectname} does not have a method {methodname}()"
        self.value = basevalue.format(objectname=analyzer.__class__.__name__, 
                                      methodname=methodname)
    def __str__(self):
        return repr(self.value)


class MethodGroupMissing(MethodMissing):
    def __init__(self, analyzer):
        super(MethodGroupMissing, self).__init__(analyzer, "group")


class MethodMapMissing(Exception):
    def __init__(self, analyzer):
        super(MethodGroupMissing, self).__init__(analyzer, "map")


class MethodFilterMissing(Exception):
    def __init__(self, analyzer):
        super(MethodGroupMissing, self).__init__(analyzer, "filter")


class MethodReduceMissing(Exception):
    def __init__(self, analyzer):
        super(MethodGroupMissing, self).__init__(analyzer, "reduce")


class MissingKey(Exception):
    def __init__(self, key):
        self.value = "Key %s is not in the data dictionary" %key
    def __str__(self):
        return repr(self.value)


class ObjectIsNotMutable(Exception):
    def __init__(self, method):
        self.value = "object is not mutable, method %s can not be invoked anymore" %method
    def __str__(self):
        return repr(self.value)

# =============================================================================
# Analyzers
# =============================================================================

class GroupByKey(object):

    def __init__(self, key):
        self.key = key

    def group(self, job):
        return job[self.key]   # FIXME raise Excption if key in not in job classad


class GroupByKeyRemap(object):

    def __init__(self, key, mapping_d):
        self.key = key
        self.mapping_d = mapping_d

    def group(self, job):
        value = job[self.key]   # FIXME raise Excption if key in not in job classad
        if value in self.mapping_d.keys():
            return self.mapping_d[value]
        else:
            return None



# =============================================================================
# Info class
# =============================================================================

class StatusInfo(object):
    """
    """

    def __init__(self, data, is_raw=True, is_mutable=True, timestamp=None):
        """ 
        :param data: the data to be recorded
        :param is_raw boolean: indicates if the object is primary or it is composed by other StatusInfo objects
        :param is_mutable boolean: indicates if the data can still be processed or not
        :param timestamp: the time when this object was created
        """ 
        self.log = logging.getLogger('autopyfactory')
        self.is_raw = is_raw
        self.is_mutable = is_mutable
        self.data = data 
        if not timestamp:
            self.timestamp = int(time.time())
        else:
            self.timestamp = timestamp

    # =========================================================================
    # methods to manipulate the data
    # =========================================================================

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
        if not self.is_mutable:
            raise ObjectIsNotMutable('group')

        if not (hasattr(analyzer, "group") and \
                inspect.ismethod(getattr(analyzer, "group"))):
            raise MethodGroupMissing(analyzer)

        if self.is_raw:
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
            for k, v in tmp_new_data.items():
                new_data[k] = StatusInfo(v, timestamp=self.timestamp)
            # 3
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items():
                new_data[key] = statusinfo.group(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info


    def map(self, analyzer):
        """
        modifies each item in self.data according to rules
        in analyzer
        :param analyzer: an object implementing method map()
        :rtype StatusInfo:
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('map')

        if not (hasattr(analyzer, "map") and \
                inspect.ismethod(getattr(analyzer, "map"))):
            raise MethodMapMissing(analyzer)

        if self.is_raw:
            new_data = []
            for item in self.data:
                new_item = analyzer.map(item)
                new_data.append(new_item)
            new_info = StatusInfo(new_data, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items():
                new_data[key] = statusinfo.map(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info


    def filter(self, analyzer):
        """
        eliminates the items in self.data that do not pass
        the filter implemented in analyzer
        :param analyzer: an object implementing method filter()
        :rtype StatusInfo:
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('filter')

        if not (hasattr(analyzer, "filter") and \
                inspect.ismethod(getattr(analyzer, "filter"))):
            raise MethodFilterMissing(analyzer)

        if self.is_raw:
            new_data = []
            for item in self.data:
                if analyzer.filter(item):
                    new_data.append(item)
            new_info = StatusInfo(new_data, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items(): 
                new_data[key] = statusinfo.filter(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info


    def reduce(self, analyzer):
        """
        process the entire self.data at the raw level
        :param analyzer: an object implementing method reduce()
        :rtype : value output of reduce()
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('reduce')

        if not (hasattr(analyzer, "reduce") and \
                inspect.ismethod(getattr(analyzer, "reduce"))):
            raise MethodReduceMissing(analyzer)

        if self.is_raw:
            new_data = None
            new_data = analyzer.reduce(self.data)
            new_info = StatusInfo(new_data, is_mutable=False, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items(): 
                new_data[key] = statusinfo.reduce(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, is_mutable=False, timestamp=self.timestamp)
            return new_info

    # =========================================================================
    # retrieve the data
    # =========================================================================

    def get(self, *key_l):
        """
        returns the item in the tree structure pointed by all keys
        Main difference with __getitem__() is that get() returns the actual data
        in case of reaching the deepest level, while __getitem__() returns the 
        corresponding Info object
        :param key_l list: list of keys for each nested dictionary
        :rtype data:
        """
        if len(key_l) == 0:
            return self.data
        else:
            key = key_l[0]
            if key not self.data.keys():
                raise MissingKey(key)
            data = self.data[key]
            return data.get(*key_l[1:])
            

    def __getitem__(self, key):
        """
        returns the part of the higher level dictionary 
        corresponding to a given key
        Main difference with get() is that it returns the actual data
        in case of reaching the deepest level, while __getitem__() returns the 
        corresponding Info object
        :param key: the key in the higher level dictionary
        :rtype: the output can be either another Info object or a raw item
        """
        if key not self.data.keys():
            raise MissingKey(key)
        return self.data[key]

