#!/bin/bash
# 
# Quick and dirty local status script
# Assumes default queues.conf, and that you're using Condor-G for everything. 
#
# John Hover <jhover@bnl.gov>
#
#
apfqueues=`cat /etc/apf/queues.conf | grep "^\[" | tr -d '[]' | grep -v DEFAULT | sort | tr "\n" " "`
date=`date`
echo $date
echo "-----------------------------------------------"
for q in $apfqueues; do
        #echo $q
        unsub=`condor_q -format ' MATCH_APF_QUEUE= %s ' match_apf_queue -format ' GridJobStatus=%s\n' gridjobstatus | grep " $q " | grep UNSUBMITTED | wc -l`
        running=`condor_q -format ' MATCH_APF_QUEUE= %s ' match_apf_queue -format ' GridJobStatus=%s\n' gridjobstatus | grep " $q " | grep ACTIVE | wc -l`
        pending=`condor_q -format ' MATCH_APF_QUEUE= %s ' match_apf_queue -format ' GridJobStatus=%s\n' gridjobstatus | grep " $q " | grep PENDING | wc -l`
        stagein=`condor_q -format ' MATCH_APF_QUEUE= %s ' match_apf_queue -format ' GridJobStatus=%s\n' gridjobstatus | grep " $q " | grep STAGE_IN | wc -l`
        stageout=`condor_q -format ' MATCH_APF_QUEUE= %s ' match_apf_queue -format ' GridJobStatus=%s\n' gridjobstatus | grep " $q " | grep STAGE_OUT | wc -l`
        echo -n "$q "
        qlen=${#q}
        fill=$((32-$qlen ))
        #echo -n " $fill "
        #echo -n " $qlen " 
        while [ $fill -gt 0 ]; do
                echo -n " "
                fill=$(($fill-1))
        done
        echo " UNSUB=$unsub	PENDING=$pending	STAGE_IN=$stagein	ACTIVE=$running	STAGE_OUT=$stageout"        
done
sum=`condor_q | tail -1`
echo $sum
