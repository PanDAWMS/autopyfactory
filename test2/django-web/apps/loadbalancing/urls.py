from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^get/$', 'apps.loadbalancing.views.get'),
    url(r'^add/$', 'apps.loadbalancing.views.add'),
    url(r'^test/$', 'apps.loadbalancing.views.test'),
)
