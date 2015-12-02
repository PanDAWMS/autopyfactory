#!/usr/bin/env python
# $Id: monitor.py 7686 2011-04-08 21:15:43Z jhover $
#

#
#   THIS IS JUST TO TEST THE DJANGO CODE
#

import commands
import re
import threading
import time
import StringIO
import urllib
import urllib2


# QUICK & DIRTY HACK
#BASEURL='http://192.153.161.228:8000'
BASEURL='http://192.153.161.228/loadbalancing'



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

        url = BASEURL+'/add/'


        #label = {}
        #label['name'] = 'name' 
        #label['factory'] = 'ui18' 
        #label['wmsqueue'] = '' 
        #label['batchqueue'] = ''
        #label['resource'] = '' 
        #label['localqueue'] = '' 

        label = {}
        label['factory'] = 'ui18' 
        #label['factory'] = 'ui19' 
        label['queues'] = ['ANALY_BNL', 'ANALY_PROD', 'ANALY_MWT2']
        data = json.dumps(label)

        out = _call(http.GET, url, data)
        #print out


def _add2():

        url = BASEURL+'/add/'

        label = {}
        label['factory'] = 'ui19' 
        label['queues'] = ['ANALY_BNL', 'ANALY_PROD', 'SLAC']
        data = json.dumps(label)

        out = _call(http.GET, url, data)


def _get():

    url = BASEURL+'/get/'
    try:
        data = {}
        data['maxtime']=1
        data = json.dumps(data)
        out = _call(http.GET, url, data)
        out = json.loads(out.read())
    except:
        out = {}
    return out

def _test():
    
    url = BASEURL+'/test/'
    out = _call(http.GET, url)
    out = out.read()
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

    #print _test()
    _add()  
    time.sleep(5)
    out = _get()
    print(out)

    #time.sleep(1)
    #_add2() 
    #out = _get()
    #print(out)

    #time.sleep(15)
    #out = _get()
    #print(out)
