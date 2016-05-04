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


# --- install the man pages, only if root  ---
gzip /usr/share/doc/apf/autopyfactory.1
mv -f /usr/share/doc/apf/autopyfactory.1.gz /usr/share/man/man1/

gzip /usr/share/doc/apf/autopyfactory-queues.conf.5
mv -f /usr/share/doc/apf/autopyfactory-queues.conf.5.gz /usr/share/man/man5/

gzip /usr/share/doc/apf/autopyfactory-factory.conf.5
mv -f /usr/share/doc/apf/autopyfactory-factory.conf.5.gz /usr/share/man/man5/ 

gzip /usr/share/doc/apf/autopyfactory-proxy.conf.5
mv -f /usr/share/doc/apf/autopyfactory-proxy.conf.5.gz /usr/share/man/man5/ 

gzip /usr/share/doc/apf/autopyfactory-monitor.conf.5
mv -f /usr/share/doc/apf/autopyfactory-monitor.conf.5.gz /usr/share/man/man5/  
