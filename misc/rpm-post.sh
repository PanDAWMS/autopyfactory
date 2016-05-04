#!/bin/bash

chmod ugo+x /etc/init.d/autopyfactory
#chmod ugo+x /usr/libexec/wrapper.sh
/sbin/chkconfig --add autopyfactory
# By default on install set factory off?
#/sbin/chkconfig autopyfactory off

# WARNING: this should be done by the spec file, in %files section
if [ ! -d /etc/autopyfactory/ ] ; then
    mkdir /etc/autopyfactory/
fi

