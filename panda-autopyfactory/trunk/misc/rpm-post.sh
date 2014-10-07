#!/bin/bash
#if [ -f /etc/autopyfactory/autopyfactory.conf.bak ] ; then
#	cp -f /etc/autopyfactory/autopyfactory.conf /etc/autopyfactory/autopyfactory.conf.rpmnew
#	cp -f /etc/autopyfactory/autopyfactory.conf.bak /etc/autopyfactory/autopyfactory.conf
#fi
chmod ugo+x /etc/init.d/autopyfactory
#chmod ugo+x /usr/libexec/wrapper.sh
/sbin/chkconfig --add autopyfactory

# By default on install set factory off?
#/sbin/chkconfig autopyfactory off

####  check that factory.sysconfig has been placed in /etc/sysconfig/factory.sysconfig 
###SYSCONF=/etc/sysconfig/autopyfactory
###SYSCONFEXAMPLE=/etc/autopyfactory/autopyfactory.sysconfig-example
###if [ ! -f $SYSCONF ] ; then 
###        cp $SYSCONFEXAMPLE $SYSCONF
###fi
   

# --- install the man pages, only if root  ---
gzip /tmp/autopyfactory.1
mv -f /tmp/autopyfactory.1.gz /usr/share/man/man1/

gzip /tmp/autopyfactory-queues.conf.5
mv -f /tmp/autopyfactory-queues.conf.5.gz /usr/share/man/man5/

gzip /tmp/autopyfactory-factory.conf.5
mv -f /tmp/autopyfactory-factory.conf.5.gz /usr/share/man/man5/ 

gzip /tmp/autopyfactory-proxy.conf.5
mv -f /tmp/autopyfactory-proxy.conf.5.gz /usr/share/man/man5/ 

gzip /tmp/autopyfactory-monitor.conf.5
mv -f /tmp/autopyfactory-monitor.conf.5.gz /usr/share/man/man5/  
