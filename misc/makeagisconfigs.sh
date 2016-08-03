#!/bin/bash
# Run Agis.py for prod and analysis in US
# Output files in /etc/autopyfactory/
#
ACTS="production analysis"
CMD="python  /usr/lib/python2.7/site-packages/autopyfactory/plugins/factory/config/Agis.py"
DEFAULTSFILE="/etc/autopyfactory/agisdefaults.conf"
LOGFILE="/var/log/agisconfig.log"
ARGS=" --trace --vo atlas --cloud us --defaults $DEFAULTSFILE "
CONFDIR="/etc/autopyfactory"

for activity in $ACTS ; do
	echo "$CMD $ARGS --activity $activity --outfile $CONFDIR/us-$activity-q.conf.tmp >> $LOGFILE"
		$CMD $ARGS --activity $activity --outfile $CONFDIR/us-$activity-q.conf.tmp >> $LOGFILE
		if [ $? -eq 0 ]; then
			mv -v $CONFDIR/us-$activity-q.conf.tmp $CONFDIR/us-$activity-q.conf
		else	
			echo "Command returned non-zero code. See $LOGFILE"
		fi
	echo "Done."
done

