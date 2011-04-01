#!/bin/bash
if [ -f /etc/apf/factory.conf.bak ] ; then
	cp -f /etc/apf/factory.conf /etc/apf/factory.conf.rpmnew
	cp -f /etc/apf/factory.conf.bak /etc/apf/factory.conf
fi
chmod ugo+x /etc/init.d/factory
/sbin/chkconfig --add factory