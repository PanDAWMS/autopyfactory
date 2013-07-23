#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class KeepNRunningSchedPlugin(SchedInterface):
    '''
    This plugin strives to keep a certain number of jobs/pilots/VMs running, regardless 
    of ready/activated or input. 
    
    Understands Retiring VM job state. 
    
    May output a negative number, if keep_running is less than current running. 
      
    '''
    id = 'keepnrunning'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            self.keep_running = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.keepnrunning.keep_running', 'getint')
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        self.log.debug('Starting.')

        self.queueinfo = self.apfqueue.batchstatus_plugin.getInfo(queue = self.apfqueue.apfqname, maxtime = self.apfqueue.batchstatusmaxtime)
       
        if not self.queueinfo:
            self.log.warning("self.queueinfo is None!")
            out = 0
            msg = "Invalid queueinfo"
        else:
            (out, msg) = self._calc(nsub)
            self.log.debug("Returning %d" % out)
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

        # 
        # Output is simply keep_running, minus potentially or currently running, while ignoring retiring jobs
        # 
        out = self.keep_running - ( running_pilots  + pending_pilots)

        self.log.info('_calc() input=%s (ignored); keep_running=%s; pending=%s; running=%s; retiring=%s : Return=%s' %(input,
                                                                                         self.keep_running, 
                                                                                         pending_pilots, 
                                                                                         running_pilots,
                                                                                         retiring_pilots, 
                                                                                         out))
        msg = "KeepNRunning:in=%s,keep=%s,run=%s,pend=%s,retiring=%s,out=%s" % (str(input), 
                                                                           self.keep_running, 
                                                                           running_pilots, 
                                                                           pending_pilots, 
                                                                           retiring_pilots, 
                                                                           out)
        return (out, msg)




