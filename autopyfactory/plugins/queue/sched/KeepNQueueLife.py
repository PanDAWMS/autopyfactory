#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class KeepNQueueLife(SchedInterface):
    """
    This plugin strives to keep a certain number of jobs/pilots/VMs running, regardless 
    of ready/activated or input.
    
    If config keep_running is None, then it changes the sense of input from 
    new jobs (relative) to a target number (absolute) 
    
    Understands Retiring VM job state. 
    
    May output a negative number, if keep_running is less than current running. 
      
    """
    id = 'keepnqueuelife'
    
    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger('autopyfactory.sched.%s' %apfqueue.apfqname)
            self.njobs = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.keepnqueuelife.njobs', 'getint', default_value=None)
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        self.log.debug('Starting.')

        ## BEGIN TEST ##
        import autopyfactory.info2 

        group_by_queue = autopyfactory.info2.GroupByKey('match_apf_queue')
        algorithm = autopyfactory.info2.Algorithm()
        algorithm.add(group_by_queue)
        algorithm.add(autopyfactory.info2.Length())

        data = self.apfqueue.batchstatus_plugin.getnewInfo(algorithm)
        if not data:
            self.log.warning("self.queueinfo is None!")
            out = 0
            msg = "KeepNQueueLife:comment=Invalid queueinfo"
        else:
            self.njobs_in_queue = data.get(self.apfqueue.apfqname)
            (out, msg) = self._calc(n)
            self.log.debug("Returning %d" % out)
        return (out, msg)
        ## END TEST ##


    def _calc(self, input):
        """
        algorithm 
        """

        if self.njobs == None:
            out = input - self.njobs_in_queue
        else:
            out = self.njobs - self.njobs_in_queue 
        msg = "KeepNRunning:in=%s,keep=%s,jobsinqueue=%s,ret=%s" % (str(input), 
                                                                    self.njobs, 
                                                                    self.njobs_in_queue, 
                                                                    out)
        self.log.info(msg)
        return (out, msg)




