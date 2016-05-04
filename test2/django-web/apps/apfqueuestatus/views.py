# Create your views here.

#    
#    example for custom tag {% mytest %}
#    
#    @register.tag
#    def mytest(parser, token):
#        return MyTestNode()
#
#    from django import template
#    class MyTestNode(template.Node):
#        def render(self, context):
#            return "this is a test"



from django.http import HttpResponse
import apps.apfqueuestatus.factories as factories
#import json

import time

### CUSTOM FILTERS ###
from django.template.defaulttags import register

@register.filter
def get_item(dictionary, key):
   """
   to look up for a field in a dictionary
   when we do not know the key name
   """
   return dictionary.get(key) 

### CUSTOM FILTERS ###



# FIXME : add a logger here 

def test(request):
    return HttpResponse("Hello, world. This is a test message from app apfqueuestatusat %s" %time.strftime("%c"))


def add(request):
    info = factories.InfoManager()
    info.add( request.body )
    return HttpResponse("Hello, world." )


def get(request):
    info = factories.InfoManager()

    if request.body:
        data = info.get( request.body )
        return HttpResponse(data, content_type="application/json")
    else:
        return bget(request)

def bget(request):
    info = factories.InfoManager()
    tables = info.bget( )

    lfactories = tables.keys()
    lfactories.sort()

    t = loader.get_template('apfqueuestatus/index.html')
    c = Context({'factories':lfactories,
                 'tables':tables,
                })

    return HttpResponse(t.render(c))



