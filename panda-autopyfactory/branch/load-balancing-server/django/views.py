# Create your views here.

from django.http import HttpResponse
from polls.mycode import C
import json

def get(request):
        c = C()
        #print request.method
        #print request.body
        #print request.read()
        data = json.loads(request.body)
        #print data
        return HttpResponse("Hello, world." )

def add(request):
        c = C()
        return HttpResponse("Hello, world." )

