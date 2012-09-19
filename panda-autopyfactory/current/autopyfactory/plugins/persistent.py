#!/usr/bin/env python 


"""
first version of the interface to handle a list of pairs
    -- APFQueue
    -- VM instace 
"""


class Pair(object):
    '''
    represents a pair 
        -- APFQueue
        -- VM instance
    '''

    def __init__(self, apfqname=None, instance=None):
        self.apfqname = apfqname
        self.instance = instance

    def createfromline(self, line):
        if line[-1] == '\n':
            line = line[0:-1]
        fields = line.split()
        self.apfqname = fields[0]
        self.instance = fields[1]

    def line(self):
        line = '%s %s' %(self.apfqname, self.instance)
        return line

    def __eq__(self, x):
        return self.apfqname == x.apfqname and self.instance == x.instance


class PairList(object):
    '''
    A list of Pair objects
    '''

    def __init__(self):
        self.pairlist = [] 

    def get(self, apfqname):

        instances = []
        for pair in self.pairlist:
            if pair.apfqname == apfqname:
                instances.append(pair.instance)
        return instances 


    def addpair(self, apfqname, instance):
        self.add(Pair(apfqname, instance))


    def add(self, pair):
        self.pairlist.append(pair)


    def deletepair(self, apfqname, instance):
        candidate = Pair(apfqname, instance)
        self.delete(candidate)

    def delete(self, pair):
        try:
            self.pairlist.remove(pair)
        except:
            pass


    def write(self, backend):
        backend.write(self.pairlist)


    def read(self, backend):
        self.pairlist = backend.read()
        




class FileBackend(object):

    def __init__(self, filename):
        self.filename = filename

    def write(self, pairlist):
        '''
        writes a file with the content of the list        
        '''

        fd = open(self.filename, 'w')
        for pair in pairlist:
            print >> fd, pair.line()
        fd.close()


    def read(self):
        '''
        get the list from a file
        '''

        pairlist = [] 
        fd = open(self.filename)
        for line in fd.readlines():
            newpair = Pair()
            newpair.createfromline(line)
            pairlist.append(newpair)
        fd.close()
        return pairlist


# =================================================================

pl = PairList()
backend = FileBackend('/tmp/apf/file')

pl.read(backend)
print pl.get('a')

pl.addpair('z', 'Z1')
pl.deletepair('c', 'C3')
pl.write(backend)






