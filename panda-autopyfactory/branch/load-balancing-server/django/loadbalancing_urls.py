from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^get/$', 'loadbalancing.views.get'),
    url(r'^add/$', 'loadbalancing.views.add'),
    url(r'^test/$', 'loadbalancing.views.test'),
)
