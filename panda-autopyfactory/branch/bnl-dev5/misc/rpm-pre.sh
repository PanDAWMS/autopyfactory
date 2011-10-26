#!/bin/bash
#if [ -f /etc/apf/factory.conf ] ; then
#	cp -f /etc/apf/factory.conf /etc/apf/factory.conf.bak
#fi
/usr/sbin/useradd --comment "AutoPyFactory service account" --shell /sbin/nologin apf 