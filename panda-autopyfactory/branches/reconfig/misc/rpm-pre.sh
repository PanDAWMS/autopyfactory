#!/bin/bash
#if [ -f /etc/autopyfactory/autopyfactory.conf ] ; then
#	cp -f /etc/autopyfactory/autopyfactory.conf /etc/autopyfactory/autopyfactory.conf.bak
#fi

if id autopyfactory > /dev/null 2>&1; then
	: # do nothing
else
    /usr/sbin/useradd --comment "AutoPyFactory service account" --shell /bin/bash autopyfactory
fi 
