#! /usr/bin/env python
#


class ActivatedSchedPlugin(object):
    id = 'activated'
    
    def __init__(self):

            self.max_jobs_torun = 3000
            self.max_pilots_per_cycle = 100
            self.min_pilots_per_cycle = 0 
            self.max_pilots_pending = 50000000
            self.min_pilots_pending = None


    def calcSubmitNum(self):

            activated_jobs = 87
            pending_pilots = 20
            running_pilots = 107

            #all_pilots = pending_pilots + running_pilots
            all_pilots = pending_pilots
            out = max(0, activated_jobs - all_pilots)
            print out
            
            if self.max_jobs_torun: 
                out = min(out, self.max_jobs_torun - all_pilots)
                print out

            if self.min_pilots_pending:
                out = max(out, self.min_pilots_pending - pending_pilots)
           
            if self.max_pilots_per_cycle:
                out = min(out, self.max_pilots_per_cycle)
                print out

            if self.min_pilots_per_cycle:
                out = max(out, self.min_pilots_per_cycle)
                print out

            if self.max_pilots_pending:
                out = min(out, self.max_pilots_pending - pending_pilots)
                print out


            # Catch all to prevent negative numbers
            if out < 0:
                out = 0
                print out
            
            print 'calcSubmitNum (activated=%s; pending=%s; running=%s;) : Return=%s' %(activated_jobs, pending_pilots, running_pilots, out)


ac = ActivatedSchedPlugin()
ac.calcSubmitNum()
