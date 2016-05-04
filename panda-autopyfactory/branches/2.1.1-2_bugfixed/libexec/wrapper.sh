#!/bin/bash 

WRAPPERVERSION="0.9.7"

# 
# A generic wrapper with minimal functionalities
#
# input options:
#   - wrappervo
#   - wrapperwmsqueue
#   - wrapperbatchqueue
#   - wrappergrid
#   - wrapperpurpose
#   - wrapperserverurl
#   - wrappertarballurl
#   - wrapperspecialcmd 
#   - wrapperplugin 
#   - wrapperpilottype
#   - wrapperloglevel
#   - wrappermode
#
# where
#
#     - wrappervo is the VO
#
#     - wrapperwmsqueue is the wms queue (e.g. the panda siteid)
#    
#     - wrapperbatchqueue is the batch queue (e.g. the panda queue)
#    
#     - wrappergrid is the grid flavor, i.e. OSG or EGEE (or gLite). 
#     The reason to include it as an input option,
#     instead of letting the wrapper to discover by itself
#     the current platform is to be able to distinguish
#     between these two scenarios:
#    
#       (a) running on local cluster
#       (b) running on grid, but the setup file is missing
#     
#     (b) is a failure and should be reported, whereas (a) is fine.
#    
#     A reason to include wrappergrid as an option in this very first wrapper
#     is that for sites running condor as local batch system, 
#     the $PATH environment variable is setup only after sourcing the 
#     OSG setup file. And only with $PATH properly setup 
#     is possible to perform actions as curl/wget 
#     to download the rest of files, or python to execute them.
#    
#     - wrapperpurpose will be the VO in almost all cases,
#     but not necessarily when several groups share
#     the same VO. An example is VO OSG, shared by 
#     CHARMM, Daya, OSG ITB testing group...
#    
#     - wrapperserverurl is the url with the PanDA server instance
#    
#     - wrappertarballurl is the base url with the wrapper tarball to be downloaded
#    
#     - wrapperspecialcmd is special command to be performed, 
#     for some specific reason, just after sourcing the Grid environment,
#     but before doing anything else.
#     This has been triggered by the need to execute command
#          $ module load <module_name>
#     at NERSC after sourcing the OSG grid environment. 
#    
#     - wrapperplugin is the plug-in module with the code corresponding to the final wrapper flavor.
#    
#     - wrapperpilottype is the actual  pilot code to be executed at the end.
#
#     - wrapperloglevel is a flag to activate high verbosity mode.
#     Accepted values are debug or info.  
#     
#     - wrappermode allows performing all steps but querying and running a real job.
#
# ----------------------------------------------------------------------------
#
# Note:
#       before the input options are parsed, they must be re-tokenized
#       so whitespaces as part of the value 
#       (i.e. --wrapperspecialcmd='module load osg')
#       create no confussion and are not taken as they are splitting 
#       different input options.
#
#       The format in the condor submission file (or JDL) to address 
#       the multi-words values is:
#
#          arguments = "--in1=val1 ... --inN=valN --cmd=""module load osg"""
#
# ----------------------------------------------------------------------------
#
# This first wrapper perform basic actions:
#      (1)  check the environment, and the availability of basic programs
#               - curl
#               - python            
#               - tar
#               - zip
#       (2) downloads a first tarball with python code
#           as passes all input options to this code.
#           With passed options, the python code will download
#           a second tarball with the final pilot code.
#
# Author jcaballero (AT) bnl.gov
#    
# ----------------------------------------------------------------------------
#

# ------------------------------------------------------------------------- #  
#                 A U X I L I A R Y       F U N C T I O N S                 #
# ------------------------------------------------------------------------- # 


f_init(){
        f_print_line
        echo "=== Pilot wrapper running at"
        echo "wrapper version: "$WRAPPERVERSION
        echo "date (UTC):  " `date --utc`
        echo "hostname:    " `hostname`
        echo "working dir: " `pwd`
        echo "user:        " `id`
        getent passwd `whoami`
}

f_print_line(){
        echo "------------------------------------------------------------"
}
f_print_error_msg(){
        ERRORMSG=$@
        f_print_msg '=== ERROR' $ERRORMSG
}
f_print_warning_msg(){
        WARNINGMSG=$@
        f_print_msg '=== WARNING' $WARNINGMSG
}
f_print_msg(){
        HEADER=$1
        shift
        MSG=$@
        f_print_line
        echo "=== wrapper.sh === "
        echo $HEADER 
        if [ "$MSG" != "" ]; then
                echo $MSG
        fi
}

# ------------------------------------------------------------------------- #  
#                 C H E C K     E N V I R O N M E N T                       #
# ------------------------------------------------------------------------- # 

f_check(){
        # function to check the environment 
        # and the basic programs needed to download the tarball
        f_check_env
        f_check_python
        f_check_python32
        f_check_curl
        f_check_tar
        f_check_gzip

        # if everything went fine...
        return 0
}

f_check_env(){
        # function to print out the environment
        f_print_msg "=== Environment:"
        printenv | sort
}
f_check_python(){
        # function to check if program python is installed
        f_check_program python
        rc=$?
        if [ $rc -eq 0 ]; then
                python -V 2>&1
                PYTHON=python
        fi
}
f_check_python32(){
        # function to check if program python32 is installed
        f_check_program python32
        rc=$?
        if [ $rc -eq 0 ]; then
                python32 -V 2>&1
                PYTHON=python32
        fi
}
f_check_curl(){
        # function to check if program curl is installed
        # curl is needed to download the tarball
        f_check_program curl FORCEQUIT
        rc=$?
        if [ $rc -eq 0 ]; then
                curl -V 
        fi
}
f_check_tar(){
        # function to check if program tar is installed
        # tar is needed to untar the tarball
        f_check_program tar FORCEQUIT
}
f_check_gzip(){
        # function to check if program gzip is installed
        # gzip is needed to untar the tarball
        f_check_program gzip FORCEQUIT
}
f_check_program(){
        # generic function to check if a given program is installed
        PROGRAM=$1
        f_print_msg "=== Checking program $PROGRAM"        

        which $PROGRAM 2> /dev/null
        rc=$?
        if [ $rc -ne 0 ]; then
                f_print_error_msg "program $PROGRAM not installed or not in the PATH"
                if [ "$2" == "FORCEQUIT" ]; then
                        f_exit 1 #FIXME: RC=1 is just a temporary solution
                fi
        fi
        return $rc 
}

# ------------------------------------------------------------------------- #  
#                 S E T      U P          F U N C T I O N S                 #
# ------------------------------------------------------------------------- # 

f_setup_grid(){
        # Function to source the corresponding 
        # source file, depending on the grid flavor
        # The input option is the grid flavor

        GRID=$1
        case $GRID in
                OSG)
                        f_setup_osg
                        return $?
                        ;;
                local|LOCAL|Local)
                        f_print_warning_msg "GRID value setup to LOCAL, doing nothing."
                        return 0
                        ;;
                *) 
                        f_print_warning_msg "GRID value not defined or not recognized"
                        return 0
                        ;;
        esac
}

f_setup_osg(){
        # If OSG setup script exists, run it
        if test ! $OSG_GRID = ""; then
                f_print_msg "=== seting up OSG environment"
                if test -f $OSG_GRID/setup.sh ; then
                        echo "Running OSG setup from $OSG_GRID/setup.sh"

                        source $OSG_GRID/setup.sh
                        return 0 
                else
                        echo "OSG_GRID defined but setup file $OSG_GRID/setup.sh does not exist"
                        return 1
                fi
        else
                echo "No OSG setup script found. OSG_GRID='$OSG_GRID'"
                return 2
        fi
        return 0
}

f_special_cmd(){
        # special setup commands to be performed just after
        # sourcing the grid environment, 
        # but before doing anything else,
        # following instructions from the input options

        if [ "$1" != "" ]; then
                msg='=== Executing special setup command: '$@
                f_print_msg "$msg" 
                $@
                return $?
        fi 
}

# ------------------------------------------------------------------------- #  
#                P A R S I N G    I N P U T    O P T I O N S                #
# ------------------------------------------------------------------------- # 

f_usage(){
        f_print_line
        echo
        echo "wrapper.sh Usage:" 
        echo
        echo " ./wrapper.sh --wrappervo=<vo> --wrapperwmsqueue=<site_name> --wrapperbatchqueue=<queue_name> \
[--wrappergrid=<grid_flavor> ] \
[--wrapperpurpose=<application_type>] \
[--wrapperserverurl=<wms_server_url>] \
[--wrappertarballurl=<wrapper_tarball_url>] \
[--wrapperspecialcmd=<special_setup_command>] \
[--wrapperplugin=<plugin_name>]\
[--wrapperpilottype=<pilot_type]\
[--wrapperloglevel=debug|info]\
[--wrappermode=<operation mode>]"
}

f_parse_arguments(){
        # Function to parse the command line input options.
        #         --wrappervo=...
        #         --wrapperwmsqueue=...
        #         --wrapperbatchqueue=...
        #         --wrappergrid=...
        #         --wrapperpurpose=...
        #         --wrapperserverurl=...
        #         --wrappertarballurl=...
        #         --wrapperspecialcmd=...
        #         --wrapperplugin=...
        #         --wrapperpilottype=...
        #         --wrapperloglevel=...
        #         --wrappermode
        # An error/warning message is displayed in case a different 
        # input option is passed

        # NOTE:
        # when the input is not one of the expected 
        # a warning message is displayed, and that is. 
        # These unexpected input option can be specific for the pilot.
        # If finally a dedicated input option to pass info 
        # to the pilots (i.e. pythonwrapperopts), then the warning message
        # will be replaced by an error message and the function
        # will return a RC != 0

        # first, the input options are re-tokenized to parse properly whitespaces
        items=
        for i in "$@"
        do
            items="$items \"$i\""
        done
        eval set -- $items

        # all unrecognized options are collected in a single variable
        unexpectedopts=""

        for WORD in "$@" ; do
                case $WORD in
                        --*)  true ;
                                case $WORD in
                                        --wrappervo=*) 
                                                WRAPPERVO=${WORD/--wrappervo=/}
                                                shift ;;
                                        --wrapperwmsqueue=*) 
                                                WRAPPERWMSQUEUE=${WORD/--wrapperwmsqueue=/}
                                                shift ;;
                                        --wrapperbatchqueue=*) 
                                                WRAPPERBATCHQUEUE=${WORD/--wrapperbatchqueue=/}
                                                shift ;;
                                        --wrappergrid=*)
                                                WRAPPERGRID=${WORD/--wrappergrid=/}
                                                shift ;;
                                        --wrapperpurpose=*)
                                                WRAPPERPURPOSE=${WORD/--wrapperpurpose=/}
                                                shift ;;
                                        --wrapperserverurl=*) 
                                                WRAPPERSERVERURL=${WORD/--wrapperserverurl=/}
                                                shift ;;
                                        --wrappertarballurl=*) 
                                                WRAPPERTARBALLURL=${WORD/--wrappertarballurl=/}
                                                shift ;;
                                        --wrapperspecialcmd=*) 
                                                WRAPPERSPECIALCMD=${WORD/--wrapperspecialcmd=/}
                                                shift ;;
                                        --wrapperplugin=*) 
                                                WRAPPERPLUGIN=${WORD/--wrapperplugin=/}
                                                shift ;;
                                        --wrapperpilottype=*) 
                                                WRAPPERPILOTTYPE=${WORD/--wrapperpilottype=/}
                                                shift ;;
                                        --wrapperloglevel=*) 
                                                WRAPPERLOGLEVEL=${WORD/--wrapperloglevel=/}
                                                shift ;;
                                        --wrappermode=*)
                                                WRAPPERMODE=${WORD/--wrappermode=/}
                                                shift ;;
                                        *) unexpectedopts=${unexpectedopts}" "$WORD 
                                           shift ;;     
                                esac ;;
                        *) unexpectedopts=${unexpectedopts}" "$WORD 
                           shift ;;
                esac
        done
        f_print_options
        return 0
}

f_print_options(){
        # printing the input options
        f_print_msg "=== Wrapper input options:"
        echo " vo: "$WRAPPERVO
        echo " site: "$WRAPPERWMSQUEUE
        echo " queue: "$WRAPPERBATCHQUEUE
        echo " grid flavor: "$WRAPPERGRID
        echo " purpose: "$WRAPPERPURPOSE
        echo " server url: "$WRAPPERSERVERURL
        echo " code url: "$WRAPPERTARBALLURL
        echo " special commands: "$WRAPPERSPECIALCMD
        echo " plugin module: "$WRAPPERPLUGIN
        echo " pilot type: "$WRAPPERPILOTTYPE
        echo " debug mode: "$WRAPPERLOGLEVEL
        echo " operation mode: "$WRAPPERMODE
        if [ "$unexpectedopts" != "" ]; then
                # warning message for unrecognized input options
                f_print_warning_msg "Unrecognized input options"
                echo $unexpectedopts
        fi

        f_check_mandatory_option "SITE" $WRAPPERWMSQUEUE
        f_check_mandatory_option "QUEUE" $WRAPPERBATCHQUEUE
        f_check_mandatory_option "SERVER URL" $WRAPPERSERVERURL
        f_check_mandatory_option "CODE URL" $WRAPPERTARBALLURL

}

f_check_mandatory_option(){
        # check if every mandatory input option has a value. 
        # A message is displayed and the program exits otherwise.

        if [ "$2" == "" ]; then
                f_print_error_msg "$1 has no value"
                f_usage
                f_exit -1
        fi
}

f_build_extra_opts(){
        # variable unexpectedopts is analyzed, 
        # and a variable extraopts is created to pass them
        # to the python wrapper. 
        # String --extraopts is added as a trick to facilitate parsing
        extraopts=""
        for WORD in $unexpectedopts; do
                extraopts=${extraopts}" --extraopts="$WORD
        done
}

f_build_pythonwrapper_opts(){
        # Not all input options should be passed to the python wrapper. 
        # The complete list of input options to be passed to the python script
        # is created here. 

        f_build_extra_opts

        pythonwrapperopts=""
        pythonwrapperopts=${pythonwrapperopts}" --wrappervo="$WRAPPERVO
        pythonwrapperopts=${pythonwrapperopts}" --wrapperwmsqueue="$WRAPPERWMSQUEUE
        pythonwrapperopts=${pythonwrapperopts}" --wrapperbatchqueue="$WRAPPERBATCHQUEUE
        pythonwrapperopts=${pythonwrapperopts}" --wrappergrid="$WRAPPERGRID
        pythonwrapperopts=${pythonwrapperopts}" --wrapperpurpose="$WRAPPERPURPOSE
        pythonwrapperopts=${pythonwrapperopts}" --wrapperserverurl="$WRAPPERSERVERURL
        pythonwrapperopts=${pythonwrapperopts}" --wrappertarballurl="$WRAPPERTARBALLURL
        pythonwrapperopts=${pythonwrapperopts}" --wrapperplugin="$WRAPPERPLUGIN
        pythonwrapperopts=${pythonwrapperopts}" --wrapperpilottype="$WRAPPERPILOTTYPE
        pythonwrapperopts=${pythonwrapperopts}" --wrapperloglevel="$WRAPPERLOGLEVEL
        pythonwrapperopts=${pythonwrapperopts}" --wrappermode="$WRAPPERMODE
        pythonwrapperopts=${pythonwrapperopts}" "$extraopts

}

# ------------------------------------------------------------------------- # 
#                           M O N I T O R                                   #
# ------------------------------------------------------------------------- # 

f_monping() {
    CMD="curl -fksS --connect-timeout 10 --max-time 20 ${APFMON}$1/$APFFID/$APFCID/$2"
    echo "Monitor ping: $CMD"

    NTRIALS=0
    MAXTRIALS=3
    DELAY=30
    while [ $NTRIALS -lt "$MAXTRIALS" ] ; do 
        out=`$CMD`
        if [ $? -eq 0 ]; then
            echo "Monitor ping: out=$out" 
            NTRIALS="$MAXTRIALS"
        else
            echo "Monitor ping: ERROR: out=$out"
            echo "Monotor ping: http_proxy=$http_proxy"
            NTRIALS=$(($NTRIALS+1))
            echo "Trial number=$NTRIALS"
            sleep $DELAY
        fi
    done
}



# ------------------------------------------------------------------------- #  
#                     E X E C U T I O N                                     #
# ------------------------------------------------------------------------- # 

f_download_wrapper_tarball(){
        # donwload a tarball with scripts in python
        # to complete the wrapper actions chain
        # The address string (WRAPPERTARBALLURL) can actually be a list of comma-split URLs.
        # This function splits that strings and tries them one by one. 

        f_print_msg "=== Downloading the wrapper tarball from $WRAPPERTARBALLURL"

        arr=$(echo $WRAPPERTARBALLURL | tr "," " ")
        for WRAPPERTARBALLURLTRIAL in $arr
        do
                f_print_msg "Trying with tarball from $WRAPPERTARBALLURLTRIAL"
                f_download_wrapper_tarball_trial
                rc=$?
                if [ $rc -eq 0 ]; then
                    # breaks
                    return $rc  
                fi
        done
        # if the loop was not broken, then we return the last RC
        return $rc
}


f_download_wrapper_tarball_trial(){
        # Tries to donwload a tarball with scripts in python for each 
        # field in original WRAPPERTARBALLURL

        f_print_msg "=== Downloading the wrapper tarball from $WRAPPERTARBALLURLTRIAL"

        WRAPPERTARBALLNAME=`/bin/basename $WRAPPERTARBALLURLTRIAL`

        cmd="curl  --connect-timeout 20 --max-time 120 -s -S $WRAPPERTARBALLURLTRIAL -o $WRAPPERTARBALLNAME"
        echo $cmd
        $cmd
        rc=$?
        if [ $rc -eq 0 ]; then
                f_print_msg "apparently, wrapper tarball $WRAPPERTARBALLNAME downloaded successfully"
                f_check_tarball
                rc=$?
        fi
        return $rc
}

f_check_tarball(){
        # check the downloaded file is really a tarball
        f_print_msg "=== checking the wrapper tarball $WRAPPERTARBALLNAME is really a gzip file"
        checkfile=`file $WRAPPERTARBALLNAME`
        [[ "$checkfile" =~ "gzip compressed data" ]]
        rc=$?
        if [ $rc -eq 0 ]; then
            f_print_msg "the tarball $WRAPPERTARBALLNAME is really a tarball"
        else
            f_print_msg "WARNING: the tarball $WRAPPERTARBALLNAME is NOT really a tarball"
        fi
        return $rc
}

f_untar_wrapper_tarball(){
        # untar the wrapper tarball and remove the original file
        f_print_msg "=== Untarring the wrapper tarball"
        tar zxvf $WRAPPERTARBALLNAME
        rm $WRAPPERTARBALLNAME
        return $?
}

f_invoke_wrapper(){
        f_print_msg "=== Executing wrapper.py ..." 
        WRAPPERNAME="wrapper.py"
        $PYTHON ./$WRAPPERNAME $@ 
        return $?
}

f_exit(){
        if [ "$1" == "" ]; then
                RETVAL=0
        else
                RETVAL=$1
        fi
        
        # if we leave job.out there it will appear in the output 
        # for the next pilot, even if it does not run any job 
        # FIXME: that should not be here !!
        #        it should be in a clean() method in the wrapper.py module
        rm -f job.out
        
        f_print_msg "exiting with RC = $RETVAL"

        # notify the monitor just after execution
        f_monping ex $rc

        exit $RETVAL
}

# ------------------------------------------------------------------------- #  
#                           M A I N                                         # 
# ------------------------------------------------------------------------- #  

# notify the monitor
f_monping rn

f_init

# --- parsing input options and initial tests ---
f_parse_arguments "$@"
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

# --- setting up environment ---
f_setup_grid $WRAPPERGRID
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

f_special_cmd $WRAPPERSPECIALCMD
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

f_check

# --- download and execute the wrapper tarball ---
f_download_wrapper_tarball
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

#### --- check the wrapper tarball is really a tarball ---
###f_check_tarball
###rc=$?
###if [ $rc -ne 0 ]; then
###        f_exit $rc
###fi

f_untar_wrapper_tarball
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

# invoking the python wrapper
f_build_pythonwrapper_opts
f_invoke_wrapper $pythonwrapperopts
rc=$?

# exit
f_exit $rc
