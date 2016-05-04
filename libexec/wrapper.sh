#!/bin/bash 

# 
# A generic panda wrapper with minimal functionalities
#
# input options:
#   - pandasite
#   - pandaqueue
#   - pandagrid
#   - pandaproject
#   - pandaserverurl
#   - pandaurl
#   - pandaspecialcmd 
#   - pandaplugin 
#   - pandapilottype
#   - pandadebug
#
# where
#
#     - pandasite is the panda site
#    
#     - pandaqueue is the panda queue
#    
#     - pandagrid is the grid flavor, i.e. OSG or EGEE (or gLite). 
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
#     A reason to include pandagrid as an option in this very first wrapper
#     is that for sites running condor as local batch system, 
#     the $PATH environment variable is setup only after sourcing the 
#     OSG setup file. And only with $PATH properly setup 
#     is possible to perform actions as curl/wget 
#     to download the rest of files, or python to execute them.
#    
#     - pandaproject will be the VO in almost all cases,
#     but not necessarily when several groups share
#     the same VO. An example is VO OSG, shared by 
#     CHARMM, Daya, OSG ITB testing group...
#    
#     - pandaserverurl is the url with the PanDA server instance
#    
#     - pandaurl is the base url with the pyton tarball to be downloaded
#    
#     - pandaspecialcmd is special command to be performed, 
#     for some specific reason, just after sourcing the Grid environment,
#     but before doing anything else.
#     This has been triggered by the need to execute command
#          $ module load <module_name>
#     at NERSC after sourcing the OSG grid environment. 
#    
#     - pandaplugin is the plug-in module with the code corresponding to the final wrapper flavor.
#    
#     - pandapilottype is the actual  pilot code to be executed at the end.
#
#     - pandadebug is a flag to activate high verbosity mode.
#
# Some input options have a default value:
#
#     - PANDAURL="http://www.usatlas.bnl.gov/~caballer/panda/wrapper-devel/"
#     - PANDASERVERURL="https://pandaserver.cern.ch:25443/server/panda"
#
# ----------------------------------------------------------------------------
#
# Note:
#       before the input options are parsed, they must be re-tokenized
#       so whitespaces as part of the value 
#       (i.e. --pandaspecialcmd='module load osg')
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
#                 A U X I L I A R         F U N C T I O N S                 #
# ------------------------------------------------------------------------- # 


f_init(){
        f_print_line
        echo "=== Pilot wrapper running at"
        echo "date:        " `date`
        echo "hostname:    " `hostname`
        echo "working dir: " `pwd`
        echo "user:        " `id`
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
                        f_exit 10 #FIXME: RC=10 is just a temporary solution
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
                *) 
                        f_print_warning_msg "GRID value not defined"
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

f_schedconfig_setup(){
        # special setup commands to be performed just after
        # sourcing the grid environment, 
        # but before doing anything else
        # following instructions from SchedConfig

        f_print_msg "checking schedconfig for specific setup commands" 

        QUEUE=$1
        envsetup=`curl  --connect-timeout 20 --max-time 60 "http://panda.cern.ch:25880/server/pandamon/query?tpmes=pilotpars&getpar=envsetup&queue=$QUEUE" -s -S`
        if [ "$envsetup" != "" ]; then
                f_print_msg "schedconfig setup command found for queue $QUEUE"
                catsetup="`echo $envsetup | sed 's?source ?cat ?'`"
                lssetup="`echo $envsetup | sed 's?source ?ls -alL ?'`"
                echo "Setup command: '$envsetup'"
                echo "Listing: $lssetup"
                $lssetup
                echo "___________ setup content:"
                $catsetup
                echo "___________ running setup"
                $envsetup
                echo "Environment after setup command from SchedConfig:"
                f_check_env
        fi
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
        echo " ./wrapper.sh --pandasite=<site_name> --pandaqueue=<queue_name> \
[--pandagrid=<grid_flavor> ] \
[--pandaproject=<application_type>] \
[--pandaserverurl=<panda_server_url>] \
[--pandaurl=<pilot_code_url>] \
[--pandaspecialcmd=<special_setup_command>] \
[--pandaplugin=<plugin_name>]\
[--pandapilottype=<pilot_type]\
[--pandadebug]"
}

f_default_input_opts(){
        # set default values for some input options

        PANDAURL="http://www.usatlas.bnl.gov/~caballer/panda/wrapper-devel/"
        PANDASERVERURL="https://pandaserver.cern.ch:25443/server/panda"
}

f_parse_arguments(){
        # Function to parse the command line input options.
        #         --pandasite=...
        #         --pandaqueue=...
        #         --pandagrid=...
        #         --pandaproject=...
        #         --pandaserverurl=...
        #         --pandaurl=...
        #         --pandaspecialcmd=...
        #         --pandaplugin=...
        #         --pandapilottype=...
        #         --pandadebug=...
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

        # setup default values for some input options
        f_default_input_opts

        # all unrecognized options are collected in a single variable
        unexpectedopts=""

        for WORD in "$@" ; do
                case $WORD in
                        --*)  true ;
                                case $WORD in
                                        --pandasite=*) 
                                                PANDASITE=${WORD/--pandasite=/}
                                                shift ;;
                                        --pandaqueue=*) 
                                                PANDAQUEUE=${WORD/--pandaqueue=/}
                                                shift ;;
                                        --pandagrid=*)
                                                PANDAGRID=${WORD/--pandagrid=/}
                                                shift ;;
                                        --pandaproject=*)
                                                PANDAPROJECT=${WORD/--pandaproject=/}
                                                shift ;;
                                        --pandaserverurl=*) 
                                                PANDASERVERURL=${WORD/--pandaserverurl=/}
                                                shift ;;
                                        --pandaurl=*) 
                                                PANDAURL=${WORD/--pandaurl=/}
                                                shift ;;
                                        --pandaspecialcmd=*) 
                                                PANDASPECIALCMD=${WORD/--pandaspecialcmd=/}
                                                shift ;;
                                        --pandaplugin=*) 
                                                PANDAPLUGIN=${WORD/--pandaplugin=/}
                                                shift ;;
                                        --pandapilottype=*) 
                                                PANDAPILOTTYPE=${WORD/--pandapilottype=/}
                                                shift ;;
                                        --pandadebug) 
                                                PANDADEBUG="True"
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
        echo " panda site: "$PANDASITE
        echo " panda queue: "$PANDAQUEUE
        echo " grid flavor: "$PANDAGRID
        echo " project: "$PANDAPROJECT
        echo " server url: "$PANDASERVERURL
        echo " code url: "$PANDAURL
        echo " special commands: "$PANDASPECIALCMD
        echo " plugin module: "$PANDAPLUGIN
        echo " pilot type: "$PANDAPILOTTYPE
        echo " debug mode: "$PANDADEBUG
        if [ "$unexpectedopts" != "" ]; then
                # warning message for unrecognized input options
                f_print_warning_msg "Unrecognized input options"
                echo $unexpectedopts
        fi

        f_check_mandatory_option "SITE" $PANDASITE
        f_check_mandatory_option "QUEUE" $PANDAQUEUE

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
        pythonwrapperopts=${pythonwrapperopts}" --pandasite="$PANDASITE
        pythonwrapperopts=${pythonwrapperopts}" --pandaqueue="$PANDAQUEUE
        pythonwrapperopts=${pythonwrapperopts}" --pandagrid="$PANDAGRID
        pythonwrapperopts=${pythonwrapperopts}" --pandaproject="$PANDAPROJECT
        pythonwrapperopts=${pythonwrapperopts}" --pandaserverurl="$PANDASERVERURL
        pythonwrapperopts=${pythonwrapperopts}" --pandaurl="$PANDAURL
        pythonwrapperopts=${pythonwrapperopts}" --pandaplugin="$PANDAPLUGIN
        pythonwrapperopts=${pythonwrapperopts}" --pandapilottype="$PANDAPILOTTYPE
        pythonwrapperopts=${pythonwrapperopts}" --pandadebug="$PANDADEBUG
        pythonwrapperopts=${pythonwrapperopts}" "$extraopts

}

# ------------------------------------------------------------------------- #  
#                     E X E C U T I O N                                     #
# ------------------------------------------------------------------------- # 

f_download_wrapper_tarball(){
        # donwload a tarball with scripts in python
        # to complete the wrapper actions chain
        f_print_msg "=== Dowloading the wrapper tarball from $PANDAURL"

        # URL is the base url. 
        # The name of the tarball (wrapper.tar.gz) must to be added.
        WRAPPERTARBALLNAME="wrapper.tar.gz"
        WRAPPERURL=${PANDAURL}/${WRAPPERTARBALLNAME}

        cmd="curl  --connect-timeout 20 --max-time 120 -s -S $WRAPPERURL -o $WRAPPERTARBALLNAME"
        $cmd
        rc=$?
        if [ $rc -eq 0 ]; then
                echo "wrapper tarball downloaded successfully" 
        fi
        return $rc
}
f_untar_wrapper_tarball(){
        # untar the wrapper tarball and remove the original file
        f_print_msg "=== Untarring the wrapper tarball"
        WRAPPERTARBALLNAME="wrapper.tar.gz"
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
        rm -f $X509_USER_PROXY
        
        f_print_msg "exiting with RC = $RETVAL"
        exit $RETVAL
}

# ------------------------------------------------------------------------- #  
#                           M A I N                                         # 
# ------------------------------------------------------------------------- #  

f_init

# --- parsing input options and initial tests ---
f_parse_arguments "$@"
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

# --- setting up environment ---
f_setup_grid $PANDAGRID
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

# --- setup special commands from schedconfig ---
f_schedconfig_setup $PANDAQUEUE
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi


f_special_cmd $PANDASPECIALCMD
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
f_untar_wrapper_tarball
rc=$?
if [ $rc -ne 0 ]; then
        f_exit $rc
fi

# invoking the python wrapper
f_build_pythonwrapper_opts
f_invoke_wrapper $pythonwrapperopts
f_exit $?








#  ------------------------------------------------------------------    
#                           IDEAS 
#  ------------------------------------------------------------------    
#
#  Maybe check that the mandatory input options (pandasite, pandaqueue, ...)
#  have been really provided, and display an error message and exit
#  otherwise.  
#
#  ------------------------------------------------------------------    
#
#  In each function add code like ...
#
#     f_untar_wrapper_tarball(){
#             # untar the wrapper tarball and remove the original file
#             WRAPPERNAME="wrapper.tar.gz"
#             tar zxvf $WRAPPERNAME
#             rm $WRAPPERNAME
#             return $?
#     }
#     
# -->
#     
#     f_untar_wrapper_tarball(){
#             # untar the wrapper tarball and remove the original file
#             logfile=`mktemp blah`
#             errorfile=`mktemp blah`
#             WRAPPERNAME="wrapper.tar.gz"
#             tar zxvf $WRAPPERNAME > logfile 2> errorfile
#             if [ $? -ne 0 ] ; then
#                   echo "error"
#                   cat $logfile
#                   cat $errorfile
#             fi
#             rm $WRAPPERNAME
#             return $?
#     }
#
