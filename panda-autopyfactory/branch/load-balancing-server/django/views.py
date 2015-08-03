# Create your views here.

from django.http import HttpResponse
from polls.mycode import C
import json

def add(request):
    c = C()
    #print request.method
    #print request.body
    #print request.read()
    c.data = request.body
    #data = json.loads(request.body)
    #print data
    return HttpResponse("Hello, world." )

def get(request):
    c = C()
    data = c.data
    #print data
    return HttpResponse(data, content_type="application/json")


