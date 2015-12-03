# Create your views here.

from django.http import HttpResponse
import apps.apfqueuestatus.factories as factories
#import json

import time

# FIXME : add a logger here 

def test(request):
    return HttpResponse("Hello, world. This is a test message from app apfqueuestatusat %s" %time.strftime("%c"))


def add(request):
    info = factories.InfoManager()
    info.add( request.body )
    return HttpResponse("Hello, world." )

def get(request):
    info = factories.InfoManager()
    data = info.get( request.body )
    return HttpResponse(data, content_type="application/json")

