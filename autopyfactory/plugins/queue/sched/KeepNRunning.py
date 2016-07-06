#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class KeepNRunning(SchedInterface):
    '''
    This plugin strives to keep a certain number of jobs/pilots/VMs running, regardless 
    of ready/activated or input.
    
    If config keep_running is None, then it changes the sense of input from 
    new jobs (relative) to a target number (absolute) 
    
    Understands Retiring VM job state. 
    
    May output a negative number, if keep_running is less than current running. 
      
    '''
    id = 'keepnrunning'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            self.keep_running = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.keepnrunning.keep_running')
            if self.keep_running:
                self.keep_running = int(self.keep_running)
            self.log.trace("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        self.log.trace('Starting.')

        self.queueinfo = self.apfqueue.batchstatus_plugin.getInfo(queue = self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)
       
        if not self.queueinfo:
            self.log.warning("self.queueinfo is None!")
            out = 0
            msg = "Invalid queueinfo"
        else:
            (out, msg) = self._calc(n)
            self.log.trace("Returning %d" % out)
        return (out, msg)

    def _calc(self, input):
        '''
        algorithm 
        '''
        
        # initial default values. 
        pending_pilots = 0
        running_pilots = 0
        retiring_pilots = 0

        pending_pilots = self.queueinfo.pending  # using the new info objects
        running_pilots = self.queueinfo.running  # using the new info object
        retiring_pilots = self.queueinfo.retiring # using the new info objects

        # Fix. Negative numbers for these values are wrong. Until we can track
        # down the source, explicitly check...
        if pending_pilots < 0: 
            pending_pilots = 0
        if running_pilots < 0: 
            running_pilots = 0        
        if retiring_pilots < 0: 
            retiring_pilots = 0
        # 
        # Output is simply keep_running, minus potentially or currently running, while ignoring retiring jobs
        #
        if self.keep_running is None:
            self.log.debug("keep_running is not set, use input.")
            out = input - ( running_pilots + pending_pilots )
        else:
            self.log.debug("keep_running is set %d, use it." % self.keep_running) 
            out = self.keep_running - ( running_pilots  + pending_pilots)

        msg = "KeepNRunning:in=%s,keep=%s,running=%s,pending=%s,retiring=%s,ret=%s" % (str(input), 
                                                                           self.keep_running, 
                                                                           running_pilots, 
                                                                           pending_pilots, 
                                                                           retiring_pilots, 
                                                                           out)
        self.log.info(msg)
        return (out, msg)




