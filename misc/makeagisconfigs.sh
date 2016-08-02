#!/bin/bash
# Run Agis.py for prod and analysis in US
# Output files in /etc/autopyfactory/
#
CMD="/usr/lib/python2.7/site-packages/autopyfactory/plugins/factory/Agis.py"
DEFAULTSFILE="/etc/autopyfactory/agisdefaults.conf"
LOGFILE="/var/log/agisconfig.log"
ARGS=" --debug --cloud us -D $DEFAULTSFILE "
CONFDIR="/etc/autopyfactory"

for activity in "production analysis" ; do
	echo '$CMD -o $CONFDIR/us-$activity-q.conf >> $LOGFILE'
	$CMD -o $CONFDIR/us-$activity-q.conf >> $LOGFILE
done

