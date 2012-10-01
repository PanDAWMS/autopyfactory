#!/usr/bin/env python 


"""
first version of the interface to handle a list of pairs
    -- APFQueue
    -- VM instace 
"""

import os

from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, ForeignKey
from sqlalchemy import orm
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from ConfigParser import SafeConfigParser


Base = declarative_base()
class VMInstance(Base):
    '''
    for info on declarative_base have a look to 
    http://docs.sqlalchemy.org/en/rel_0_7/orm/examples.html?highlight=declarative_base#declarative-reflection 

    it is possible to create an object of this class in two ways 
    (actually they are the same):

        -- VMInstance( apfqname = 'q', vm_instance = 'i', host_name = 'server-486.novalocal' )

        -- d = {'apfqname':'q', 'vm_instance':'i', 'host_name':'server-486.novalocal'}
           VMInstance( **d ) 
    '''

    __tablename__ = "VMInstances"
    id = Column(Integer, primary_key=True)
    apfqname = Column(String)
    vm_instance = Column(String)
    host_name = Column(String)
    condor_host_name = Column(String)
    startd_status = Column(String)

    def __eq__(self, x):
        '''
        tell if two objects of class VMInstance have the same values (except ID)
        '''
        
        # ------------------------------------------------------ 
        #  FIXME:
        #   This algorithm needs to be improved
        # ------------------------------------------------------ 
        if not self.apfqname == x.apfqname:
            return False
        if not self.vm_instance == x.vm_instance:
            return False
        if not self.host_name == x.host_name:
            return False
        if not self.condor_host_name == x.condor_host_name:
            return False
        if not self.startd_status == x.startd_status:
            return False
        return True


class PersistenceDB(object):
    '''
    class to handle the info in the DB
    '''

    def __init__(self, config, type):    

        self.config = config
        self.instance_type = type

        self._setup()

    def _setup(self):
        '''
        Create connection URI/DB string and setup DB if not existing. 
        '''
        
        self.dburi =""
        self.dbengine=self.config.get('persistence', 'persistence.dbengine')
        self.dburi += self.dbengine
        
        self.dbuser=self.config.get('persistence', 'persistence.dbuser')
        self.dbpassword=self.config.get('persistence', 'persistence.dbpassword')
        if self.dbuser and self.dbpassword:
            self.dburi += "%s:%s" % (self.dbuser, self.dbpassword)
        
        self.dbhost=self.config.get('persistence', 'persistence.dbhost')
        self.dbport=self.config.get('persistence', 'persistence.dbport')
        self.dbpath = os.path.expanduser(self.config.get('persistence', 'persistence.dbpath'))
        
        if self.dbhost and self.dbport and self.dbpath:
            self.dburi += "@%s:%s/%s" % ( self.dbhost, self.dbport, self.dbpath)
        elif self.dbpath:
            self.dburi += "/%s" % self.dbpath
        
        self.engine = create_engine(self.dburi)
        
        self.metadata = Base.metadata
        self.metadata.create_all(self.engine)

        # create the session
        _session = sessionmaker()
        _session.configure(bind=self.engine)
        self.session = _session()

        # query
        self.list_vm = self.session.query(self.instance_type).all()
        
    def getinstance(self, reference):
        #
        #  Note: maybe this can be done better 
        #        using filter_by() 
        #        instead of getting everything and 
        #        searching for the object we are interested in
        #
        instances = self.session.query(self.instance_type).all()
        for i in instances:
            if i == reference:
                return i
        return None

    def save(self):
        self.session.flush()
        self.session.commit()
    

# ------ examples --------

#conf = SafeConfigParser()
#conf.readfp( open('path_to_conf') )
#o = PersistenceDB(conf, VMInstance)

#o.session.add(VMInstance(apfqname='APFQ1', vm_instance='i_0000fb'))
#o.save()

#i = o.getinstance(VMInstance(apfqname = 'APFQ1', vm_instance = 'i_0000fb'))
#o.session.delete(i)
#o.save()
