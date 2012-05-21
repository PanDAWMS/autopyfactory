#!/bin/bash
condor_q -format '%s' clusterid -format '.%s ' procid -format ' MATCH_APF_QUEUE="%s" ' match_apf_queue -format ' JobStatus=%d' jobstatus -format ' GlobusStatus=%d\n' globusstatus
