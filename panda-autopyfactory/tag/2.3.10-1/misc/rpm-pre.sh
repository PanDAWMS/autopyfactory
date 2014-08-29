#!/bin/bash
#if [ -f /etc/apf/factory.conf ] ; then
#	cp -f /etc/apf/factory.conf /etc/apf/factory.conf.bak
#fi

if id apf > /dev/null 2>&1; then
	: # do nothing
else
    /usr/sbin/useradd --comment "AutoPyFactory service account" --shell /bin/bash apf
fi 