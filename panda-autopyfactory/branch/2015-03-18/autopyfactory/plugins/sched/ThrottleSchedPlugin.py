#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class ThrottleSchedPlugin(SchedInterface):
    id = 'throttle'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            try:
                self.offset = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.ready.offset', 'getint', default_value=0)
                self.log.debug("SchedPlugin: offset = %d" % self.offset)
            except:
                pass 
                # Not mandatory
                
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex


    def calcSubmitNum(self, n=0):
        """ 
        """

        out = n
        self.log.debug('Starting.')
        (out, msg) = self._calc(n)
        return (out, msg)


    def _calc(self, input):
        '''
        algorithm 
        '''
        
        out = max(0, ( activated_jobs - self.offset)  - pending_pilots )
        self.log.info('input=%s; activated=%s; offset=%s pending=%s; running=%s; Return=%s' %(input,
                                                                                         activated_jobs,
                                                                                         self.offset, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        msg = "Ready:in=%s;activated=%d,offset=%d,pending=%d;ret=%d" % (input, activated_jobs, self.offset, pending_pilots, out)
        return (out,msg)
