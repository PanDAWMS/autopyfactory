# Create your views here.

from django.http import HttpResponse
import polls.factories as factories
#import json

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

