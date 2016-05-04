#!/bin/bash
# $Id$
#
function lfc_test() {
    echo -n "Testing LFC module for $1: "
    which $1 &> /dev/null
    if [ $? != "0" ]; then
        echo "No $1 found in path."
        return 1
    fi
    $1 <<EOF
import sys
try:
    import lfc
    print "LFC module imported ok."
except:
    print "Failed to import LFC module."
    sys.exit(1)
EOF
}

function find_lfc_compatible_python() {
    ## Try to figure out what python to run

    # We _do_not_ now try to use python from the ATLAS release
    # as at this point we do not know what version of python to
    # use or what architecture. Therefore the strategy now is to
    # use the site environment in which to run the pilot and
    # let the pilot setup the correct ATLAS environment for the
    # job.
    
    # python2.6 is still under test, so only use it if we are asked to
    if [ -n "$APF_PYTHON26" ]; then
    	pybin=python2.6
    	lfc_test $pybin
    	if [ $? = "0" ]; then
        	return 0
    	fi
    fi   

    # On many sites python now works just fine (m/w also now
    # distributes the LFC plugin in 64 bit)
    pybin=python
    lfc_test $pybin
    if [ $? = "0" ]; then
        return 0
    fi

    # Now see if python32 exists
    pybin=python32
    lfc_test $pybin
    if [ $? == "0" ]; then
        return 0
    fi

    # Oh dear, we're doomed...
    echo "ERROR: Failed to find an LFC compatible python."
    echo "Going in with pybin=python - this will probably fail..."
    pybin=python
}

function get_pilot() {
    # Extract the pilot via http from CERN (N.B. uudecode now deprecated)
    # You can get custom pilots by having PILOT_HTTP_SOURCES defined
    # Pilot tarballs have no pilot3/ directory stub, so we conform to that...
    mkdir pilot3
    cd pilot3

    get_pilot_http $@
    if [ $? = "0" ]; then
        return 0
    fi

    echo "Could not get pilot code from any source. Self destruct in 5..4..3..2..1.."
    return 1
}



function get_pilot_http() {
    # If you define the environment variable PILOT_HTTP_SOURCES then
    # loop over those servers. Otherwise use CERN, with Glasgow as a fallback.
    # N.B. an RC pilot is chosen once every 100 downloads for production and
    # ptest jobs use Paul's development release.
    if [ -z "$PILOT_HTTP_SOURCES" ]; then
    	if echo $@ | grep -- "-u ptest" > /dev/null; then 
    		echo "DEBUG: This is a ptest pilot. Will use development pilot code"
            PILOT_HTTP_SOURCES="http://project-atlas-gmsb.web.cern.ch/project-atlas-gmsb/pilotcode-dev.tar.gz"
            PILOT_TYPE=PT
    	elif [ $(($RANDOM%100)) = "0" ]; then
            echo "DEBUG: Release candidate pilot will be used."
            PILOT_HTTP_SOURCES="http://pandaserver.cern.ch:25080/cache/pilot/pilotcode-rc.tar.gz"
            PILOT_TYPE=RC
        else
        	echo "DEBUG: Normal production pilot code used." 
            PILOT_HTTP_SOURCES="http://pandaserver.cern.ch:25080/cache/pilot/pilotcode.tar.gz http://svr017.gla.scotgrid.ac.uk/factory/release/pilot3-svn.tgz"
            PILOT_TYPE=PR

    	fi
    fi
    for source in $PILOT_HTTP_SOURCES; do
        echo "Trying to download pilot from $source..."
        curl --connect-timeout 30 --max-time 180 -sS $source | tar -xzf -
        if [ -f pilot.py ]; then
            echo "Downloaded pilot from $source"
            return 0
        fi
        echo "Download from $source failed."
    done
    return 1
}

function set_limits() {
    # Set some limits to catch jobs which go crazy from killing nodes
    
    # 20GB limit for output size (block = 1K in bash)
    fsizelimit=$((20*1024*1024))
    echo Setting filesize limit to $fsizelimit
    ulimit -f $fsizelimit
    
    # Apply memory limit?
    memLimit=0
    while [ $# -gt 0 ]; do
        if [ $1 == "-k" ]; then
            memLimit=$2
            shift $#
        else
            shift
        fi
    done
    if [ $memLimit == "0" ]; then
        echo No VMEM limit set
    else
        # Convert to kB
        memLimit=$(($memLimit*1000))
        echo Setting VMEM limit to ${memLimit}kB
        ulimit -v $memLimit
    fi
}

function monping() {
    echo -n 'Monitor ping: '
    curl -fksS --connect-timeout 10 --max-time 20 ${APFMON}$1/$APFFID/$APFCID/$2
    if [ $? -eq 0 ]; then
        echo
    else
        echo $?
        echo ARGS: ${APFMON}$1/$APFFID/$APFCID/$2
    fi
}

function monpost() {
    echo Monitor debug begin:
    pwd
    ls -l
    echo Finding pandaJobData.out...
    find -name pandaJobData.out

    # scrape PandaIDs from pilot log
    echo 'SCRAPE: '
    find -name pilotlog.*
    cat pilotlog.*
    find -name pilotlog.* -exec egrep ^PandaID= {} \; 

    echo Monitor debug end:
}

function set_forced_env() {
    # Sometimes environment settings via condor fail if they are overwritten
    # by the site. Force env vars by prefixing them with APF_FORCE_
    echo Forced environment variables are
    env | grep APF_FORCE_
    eval $(env | egrep "^APF_FORCE_" | perl -pe 's/^APF_FORCE_//;')
}


## main ##

echo "This is pilot wrapper $Id$"

# notify monitoring, job running
monping rn

# Check what was delivered
echo "Scanning landing zone..."
echo -n "Current dir: "
startdir=$(pwd)
echo $startdir
ls -l
me=$0
echo "Me and my args: $0 $@"
if [ ! -f $me ]; then
    echo "Trouble ahead - cannot find myself. Should I try psychoanalysis?"
fi
echo

# If we have TMPDIR defined, then move into this directory
# If it's not defined, then stay where we are
if [ -n "$TMPDIR" ]; then
    cd $TMPDIR
fi
templ=$(pwd)/condorg_XXXXXXXX
temp=$(mktemp -d $templ)
echo Changing work directory to $temp
cd $temp

# Try to get pilot code...
get_pilot $@
ls -l
if [ ! -f pilot.py ]; then
    echo "FATAL: Problem with pilot delivery - failing after dumping environment"
fi

# Set any limits we need to stop jobs going crazy
echo
echo "---- Setting crazy job protection limits ----"
set_limits $@
echo

# Set any forced environment variables
echo
echo "---- Looking for forced environment variables ----"
set_forced_env
echo

# Environment sanity check (useful for debugging)
echo "---- Host Environment ----"
uname -a
hostname
hostname -f
echo

echo "---- JOB Environment ----"
env | sort
echo

echo "---- Shell Process Limits ----"
ulimit -a
echo

echo "---- Proxy Information ----"
voms-proxy-info -all
echo

# Unset https proxy - this is known to be broken 
# and is usually unnecessary on the ports used by
# the panda servers
unset https_proxy HTTPS_PROXY

# Set LFC api timeouts
export LFC_CONNTIMEOUT=60
export LFC_CONRETRY=2
export LFC_CONRETRYINT=60


# Find the best python to run with
echo "---- Searching for LFC compatible python ----"
find_lfc_compatible_python
echo "Using $pybin for python LFC compatibility"
echo

# OSG or EGEE?
if [ -n "$VO_ATLAS_SW_DIR" ]; then
    echo "Found EGEE flavour site with software directory $VO_ATLAS_SW_DIR"
    ATLAS_AREA=$VO_ATLAS_SW_DIR
elif [ -n "$OSG_APP" ]; then
    echo "Found OSG flavor site with software directory $OSG_APP/atlas_app/atlas_rel"
    ATLAS_AREA=$OSG_APP/atlas_app/atlas_rel
else
    echo "ERROR: Failed to find VO_ATLAS_SW_DIR or OSG_APP. This is a bad site."
    ATLAS_AREA=/bad_site
fi

# Trouble with tags file in VO dir?
echo "---- VO SW Area ----"
ls -l $ATLAS_AREA/
echo
if [ -e $ATLAS_AREA/tags ]; then
  echo Tag file contents:
  cat $ATLAS_AREA/tags
else
  echo Error: Tags file does not exist: $ATLAS_AREA/tags
fi
echo


# Add DQ2 clients to the PYTHONPATH
echo "---- Local DDM setup ----"
echo "Looking for $ATLAS_AREA/ddm/latest/setup.sh"
if [ -f $ATLAS_AREA/ddm/latest/setup.sh ]; then
    echo "Sourcing $ATLAS_AREA/ddm/latest/setup.sh"
    source $ATLAS_AREA/ddm/latest/setup.sh
else
    echo "WARNING: No DDM setup found to source."
fi
echo

# Search for local setup file
echo "---- Local ATLAS setup ----"
echo "Looking for $ATLAS_AREA/local/setup.sh"
if [ -f $ATLAS_AREA/local/setup.sh ]; then
    echo "Sourcing $ATLAS_AREA/local/setup.sh"
    source $ATLAS_AREA/local/setup.sh
else
    echo "WARNING: No ATLAS local setup found to source."
fi
echo

# This is where the pilot rundirectory is - maybe left after job finishes
scratch=`pwd`

echo "---- Ready to run pilot ----"
echo "My Arguments: $@"

# If we know the pilot type then set this
if [ -n "$PILOT_TYPE" ]; then
    pilot_args="-d $scratch $@ -i $PILOT_TYPE"
else
    pilot_args="-d $scratch $@"
fi

# Prd server and pass arguments
cmd="$pybin pilot.py $pilot_args"

echo cmd: $cmd
$cmd

echo
echo "Pilot exit status was $?"

# notify monitoring, job exiting, capture the pilot exit status
if [ -f STATUSCODE ]; then
echo
  scode=`cat STATUSCODE`
else
  scode=$pexitcode
fi
echo -n STATUSCODE:
echo $scode
monping ex $scode
monpost


# Now wipe out our temp run directory, so as not to leave rubbish lying around
echo "Now clearing run directory of all files."
cd $startdir
rm -fr $temp

# The end
exit

