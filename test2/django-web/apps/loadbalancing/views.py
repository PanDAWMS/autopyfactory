# Create your views here.

from django.http import HttpResponse
import apps.loadbalancing.factories as factories
#import json

import time

# FIXME : add a logger here 

def test(request):
    return HttpResponse("Hello, world. This is another test message at %s" %time.strftime("%c"))


def add(request):
    info = factories.InfoManager()
    #print request.method
    #print request.body
    #print request.read()
    info.add( request.body )
    #info.data = request.body
    #data = json.loads(request.body)
    #print data
    return HttpResponse("Hello, world." )

def get(request):
    info = factories.InfoManager()
    data = info.get()
    #data = info.data
    #print data
    return HttpResponse(data, content_type="application/json")

