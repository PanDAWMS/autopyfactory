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

    # We first look for a 32bit python in the ATLAS software area
    # which is usually more up to date than the OS version.
    # This python snippet defines the correct comparison of the rel_X-Y 
    pybin=$(ls $VO_ATLAS_SW_DIR/prod/releases/rel_[0-9]*-[0-9]*/sw/lcg/external/Python/*/*/bin/python | python -c'
import sys, re

def compareVersion(path):
    m = re.search("rel_(\d+)-(\d+)", path)
    if m:
        # Return [X, Y] for rel_X-Y
        return [int(x) for x in m.groups()]
    else:
        # Failed path
        return [0, 0]

paths=[]
for path in sys.stdin:
    path = path.strip()
    paths.append(path)

paths.sort(key=compareVersion)
print paths[-1]')

    if [ -z "$pybin" ]; then
        echo "ERROR: No python found in ATLAS SW release - site is probably very broken"
    else
        pydir=${pybin%/bin/python}
        echo Highest versioned ATLAS python is in $pydir
        ORIG_PATH=$PATH
        ORIG_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
        ORIG_PYTHONPATH=$PYTHONPATH
        # Mangle the PYTHONPATH to try and sneak the 32 bit path back in,
        # i.e., make lib64/python -> lib/python
        if file $pybin | grep "32-bit" > /dev/null; then
            PYTHONPATH=$(echo $PYTHONPATH | sed 's/lib64/lib/g')
        fi
        PATH=$pydir/bin:$PATH
        LD_LIBRARY_PATH=$pydir/lib:$LD_LIBRARY_PATH
        lfc_test $pybin
        if [ $? = "0" ]; then
            echo ATLAS python looks good. Set:
            echo PYTHONPATH=$PYTHONPATH
            echo PATH=$PATH
            echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
            return 0
        fi
        # Else reset paths
        PATH=$ORIG_PATH
        LD_LIBRARY_PATH=$ORIG_LD_LIBRARY_PATH
        PYTHONPATH=$ORIG_PYTHONPATH
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
    # Try different methods of extracting the pilot
    #  1. uuencoded attachment of this script
    #  2. http from BNL, then svr017 (or a server of your own choice)

    # BNL tarballs have no pilot3/ directory stub, so we conform to that...
    mkdir pilot3
    cd pilot3

    extract_uupilot $1
    if [ $? = "0" ]; then
        return 0
    fi

    get_pilot_http
    if [ $? = "0" ]; then
        return 0
    fi

    echo "Could not get pilot code from any source. Self desctruct in 5..4..3..2..1.."
    return 1
}


function extract_uupilot() {
    # Try pilot extraction from this script
    echo Attempting to extract pilot from $1
    python - $1 <<EOF
import uu, sys
uu.decode(sys.argv[1])
EOF

    if [ ! -f pilot3.tgz ]; then
        echo "Error uudecoding pilot"
        return 1
    fi

    echo "Pilot extracted successfully"
    tar -xzf pilot3.tgz
    rm -f pilot3.tgz
    return 0
}


function get_pilot_http() {
    # If you define the environment variable PILOT_HTTP_SOURCES then
    # loop over those servers. Otherwise use CERN, with Glasgow as a fallback.
    # N.B. an RC pilot is chosen once every 100 downloads for production.
    if [ -z "$PILOT_HTTP_SOURCES" ]; then
    if [ $(($RANDOM%100)) = "0" -a $USER_PILOT = "0" ]; then
        echo "WARNING: Release canditate pilot will be used."
        PILOT_HTTP_SOURCES="http://pandaserver.cern.ch:25080/cache/pilot/pilotcode-rc.tar.gz"
    else
        PILOT_HTTP_SOURCES="http://pandaserver.cern.ch:25080/cache/pilot/pilotcode.tar.gz http://svr017.gla.scotgrid.ac.uk/factory/release/pilot3-svn.tgz"
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
  echo -n Monitor ID:
  echo $APFMON | tr A-Za-z N-ZA-Mn-za-m
  echo -n 'Monitor ping: '
  curl -fksS --connect-timeout 10 --max-time 20 ${APFMON}$1/$APFFID/$APFCID/$2
  if [ $? = "0" ]; then
    echo
  else
    echo $?
  fi
}

function monpost() {
  echo Monitor info:
  pwd
  ls -l
  cat pandaJobData.out
  echo -n 'Monitor post: '
  curl -fksS -d @pandaJobData.out --connect-timeout 10 --max-time 20 ${APFMON}i/$APFFID/$APFCID/
  if [ $? = "0" ]; then
    echo
  else
    echo $?
  fi
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

# Detect user pilots here - necessary for some pilot RC downloads
echo $@ | grep "user" &> /dev/null
if [ $? = "0" ]; then
    USER_PILOT=1
    echo User pilot detected
else
    USER_PILOT=0
    echo This is not a user pilot
fi

# Updated 2009-07 to prefer TMPDIR over EDG_WL_SCRATCH, which is
# really now an anachronism from the lcg-RB
if [ -n "$TMPDIR" ]; then
    cd $TMPDIR
elif [ -n "$EDG_WL_SCRATCH" ]; then
    cd $EDG_WL_SCRATCH
fi
templ=$(pwd)/condorg_XXXXXXXX
temp=$(mktemp -d $templ)
echo Changing work directory to $temp
cd $temp

# Try to get pilot code...
get_pilot $me
ls -l
if [ ! -f pilot.py ]; then
    echo "FATAL: Problem with pilot delivery - failing after dumping environment"
fi

# Set any limits we need to stop jobs going crazy
echo
echo "---- Setting crazy job protection limits ----"
set_limits $@
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

# Example work around code for sites which are broken in weird
# ways (dates from old broken LFC plugins way back when...)
hostname -f | egrep "this is turned off right now" &> /dev/null
if [ $? -eq 0 ]; then
    echo "Employing LFC workaround"
    wget http://trshare.triumf.ca/~rodwalker/lfc.tgz
    tar -zxf lfc.tgz
    export PYTHONPATH=`pwd`/lib/python:$PYTHONPATH
fi
# Set lfc api timeouts
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

# Prd server and pass arguments
cmd="$pybin pilot.py -d $scratch $@"

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
echo PAL
pwd
ls -l
rm -fr $temp

# The end
exit

