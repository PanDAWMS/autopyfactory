#!/bin/bash  

# ------------------------------------------------------------------------- #  
#                       A P F       M O N I T O R                           # 
# ------------------------------------------------------------------------- #  

f_monping() {
        # code to pass messages to the AutoPyFactory Monitor

        echo -n 'Monitor ping: '
        curl -fksS --connect-timeout 10 --max-time 20 ${APFMON}$1/$APFFID/$APFCID/$2
        if [ $? -eq 0 ]; then
                echo
        else
                echo $?
                echo ARGS: ${APFMON}$1/$APFFID/$APFCID/$2
        fi
}

f_monping $@
