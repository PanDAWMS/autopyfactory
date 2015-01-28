#!/bin/bash

if id autopyfactory > /dev/null 2>&1; then
	: # do nothing
else
    /usr/sbin/useradd --comment "AutoPyFactory service account" --shell /bin/bash autopyfactory
fi 
