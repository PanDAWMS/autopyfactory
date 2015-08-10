#!/usr/bin/env python
# $Id: monitor.py 7686 2011-04-08 21:15:43Z jhover $
#

#
#   THIS IS JUST TO TEST THE DJANGO CODE
#

import commands
import re
import threading
import StringIO
import urllib
import urllib2


try:
    import json as json
except ImportError, err:
    import simplejson as json


class NoExceptionHTTPHandler(urllib2.BaseHandler):

    def http_error_201(self, request, response, code, msg, hdrs):
        return response
    def http_error_204(self, request, response, code, msg, hdrs):
        return response
    def http_error_206(self, request, response, code, msg, hdrs):
        return response


class RequestWithMethod(urllib2.Request):

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method


class http:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"  

def _add():

        url = 'http://192.153.161.219:8000/add/'

        data = [] 

        #label = {}
        #label['name'] = 'name' 
        #label['factory'] = 'ui18' 
        #label['wmsqueue'] = '' 
        #label['batchqueue'] = ''
        #label['resource'] = '' 
        #label['localqueue'] = '' 

        label = {}
        label['factory'] = 'ui18' 
        label['queues'] = ['ANALY_BNL', 'ANALY_PROD', 'ANALY_MWT2']
        


        data.append(label)
        data = json.dumps(data)

        #out = _call(http.GET, url, data)
        out = _call(http.GET, url, json.dumps(label))


def _get():

    url = 'http://192.153.161.219:8000/get/'
    out = _call(http.GET, url)
    out = json.loads(out.read())
    return out


def _call(method, url, data=None):

    opener = urllib2.build_opener(NoExceptionHTTPHandler)
    if data:
        request = RequestWithMethod(method, url, data)
    else:
        request = RequestWithMethod(method, url)

    try:
        out = opener.open(request)
    except Exception, e:
        out = None  # Is this OK?

    return out



if __name__ == '__main__':
    _add()  
    print _get()
