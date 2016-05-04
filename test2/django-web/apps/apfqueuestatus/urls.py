from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
    url(r'^get/$', 'apps.apfqueuestatus.views.get'),
    url(r'^add/$', 'apps.apfqueuestatus.views.add'),
    url(r'^test/$', 'apps.apfqueuestatus.views.test'),
)
