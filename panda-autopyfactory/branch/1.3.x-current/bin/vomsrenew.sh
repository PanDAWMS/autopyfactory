#!/bin/bash
# Renew the voms proxy using a long-lived non-voms one
# 
# Periodically you should set up the longlived proxy
#
# voms-proxy-init -valid 400:00 -out /tmp/x509up_u1000_novoms
#
# Must not be set!
# unset X509_USER_KEY X509_USER_CERT

# Uncomment and/or modify if necessary:
#source /afs/cern.ch/project/gd/LCG-share/sl5/external/etc/profile.d/grid-env.sh
#source /etc/profile.d/grid-env.sh

ID=$(id -u)

#echo "---- Environment ----"
#env | sort
#echo

vomsinit() {
    # Wrap creation of a voms cert with a certain role
    # $1 = input proxy
    # $2 = voms role
    # $3 = output proxy
    voms-proxy-init -voms $2 -out $3-tmp -valid 96:00 -cert=$1
    if [ $? == "0" ]; then
    mv $3-tmp $3
    fi
}

vomsinit /tmp/plainProxy atlas:/atlas/Role=production /tmp/prodProxy
vomsinit /tmp/plainProxy atlas:/atlas/Role=pilot /tmp/pilotProxy
#vomsinit /tmp/x509_u${ID}_novoms atlas:/atlas/uk/Role=poweruser0 /tmp/ukPowerProxy
#vomsinit /tmp/x509_u${ID}_novoms atlas /tmp/atlasProxy

