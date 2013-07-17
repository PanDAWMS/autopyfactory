#!/bin/bash
#if [ -f /etc/apf/factory.conf.bak ] ; then
#	cp -f /etc/apf/factory.conf /etc/apf/factory.conf.rpmnew
#	cp -f /etc/apf/factory.conf.bak /etc/apf/factory.conf
#fi
chmod ugo+x /etc/init.d/factory
#chmod ugo+x /usr/libexec/wrapper.sh
/sbin/chkconfig --add factory

#  check that factory.sysconfig has been placed in /etc/sysconfig/factory.sysconfig 
SYSCONF=/etc/sysconfig/factory.sysconfig
SYSCONFEXAMPLE=/etc/apf/factory.sysconfig-example
if [ ! -f $SYSCONF ] ; then 
        cp $SYSCONFEXAMPLE $SYSCONF
fi
