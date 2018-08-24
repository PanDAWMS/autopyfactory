"""
 An APF auth plugin to provide SSH key info. 

"""
import base64
import logging
import os
import traceback


# Does not need to be a thread because it doesn't need to perform asynch actions.
class SSH(object):
    """
        Container for SSH account info.
        Works with provided filepaths. 
        Work with config-only provided base64-encoded tokens, with files created.  
        Files written to 
            <authbasedir>/<name>/<ssh.type>
            <authbasedir>/<name>/<ssh.type>.pub
        
    """    
    def __init__(self, manager, config, section):
        self.log = logging.getLogger('autopyfactory.auth')
        self.name = section
        self.manager = manager
        self.factory = manager.factory
        self.basedir = os.path.expanduser(config.get(section, 'authbasedir'))
        self.sshtype = config.get(section, 'ssh.type' )
        self.privkey = config.get(section, 'ssh.privatekey' )
        self.pubkey = config.get(section, 'ssh.publickey' )
        self.privkeypass = config.get(section, 'ssh.privkeypass')
        self.privkeypath = os.path.expanduser(config.get(section, 'ssh.privatekeyfile' ))
        self.pubkeypath = os.path.expanduser(config.get(section, 'ssh.publickeyfile' ))
        self.privkeypasspath = config.get(section, 'ssh.privkeypassfile')
        #self.passwordfile = config.get(section, 'ssh.passwordfile')
        
        # Handle raw empty values
        if self.privkey.lower() == 'none':
            self.privkey = None
        if self.privkeypass.lower() == 'none':
            self.privkeypass = None
        if self.pubkey.lower() == 'none':
            self.pubkey = None
        
        # Handle path empty values    
        if self.privkeypath.lower() == 'none':
            self.privkeypath = None
        if self.privkeypasspath.lower() == 'none':
            self.privkeypasspath = None
        if self.pubkeypath.lower() == 'none':
            self.pubkeypath = None
        #if self.passwordfile.lower() == 'none':
        #    self.passwordfile = None
              
        # Create files if needed
        if self.privkey is not None:
            fdir = "%s/%s" % (self.basedir, self.name)
            fpath = "%s/%s" % (fdir, self.sshtype)
            try:
                self._ensuredir(fdir)
                self._decodewrite(fpath, self.privkey)
                self.privkeypath = fpath
                os.chmod(fpath, 0o600)
                self.log.debug("Wrote decoded private key to %s and set config OK." % self.privkeypath)
            except Exception as e:
                self.log.error("Exception: %s" % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc())
        
        if self.pubkey is not None:
            fdir = "%s/%s" % (self.basedir, self.name)
            fpath = "%s/%s.pub" % (fdir, self.sshtype)
            try:
                self._ensuredir(fdir)
                self._decodewrite(fpath, self.pubkey)
                self.pubkeypath = fpath
                self.log.debug("Wrote decoded public key to %s and set config OK." % self.pubkeypath)
            except Exception as e:
                self.log.error("Exception: %s" % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc())
                
        self.log.debug("SSH Handler for profile %s initialized." % self.name)

        
    def _ensuredir(self, dirpath):
        self.log.debug("Ensuring directory %s" % dirpath)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
    
    def _decodewrite(self, filepath, b64string ):
        self.log.debug("Writing key to %s" % filepath)
        decoded = SSH.decode(b64string)
        try:
            fh = open(filepath, 'w')
            fh.write(decoded)
            fh.close()
        except Exception as e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())
            raise
        else:
            fh.close()
            
            
    def _validate(self):
        """
        Confirm credentials exist and are valid. 
        """
        return True


    def getSSHPubKey(self):
        pass
    
    def getSSHPrivKey(self):
        pass
    
    def getSSHPubKeyFilePath(self):
        self.log.debug('[%s] Retrieving pubkeypath: %s' % (self.name, self.pubkeypath))
        return self.pubkeypath
    
    def getSSHPrivKeyFilePath(self):
        self.log.debug('[%s] Retrieving privkeypath: %s' % (self.name, self.privkeypath))
        return self.privkeypath
    
    def getSSHPassFilePath(self):
        self.log.debug('[%s] Retrieving passpath: %s' % (self.name, self.privkeypasspath))
        return self.privkeypasspath
##############################################
#        External Utility class methods. 
##############################################

    @classmethod
    def encode(self, string):
        return base64.b64encode(string)
    
    @classmethod
    def decode(self, string):
        return base64.b64decode(string)    
        
