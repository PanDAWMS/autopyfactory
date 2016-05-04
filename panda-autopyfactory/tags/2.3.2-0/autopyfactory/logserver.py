#
# 
'''
   Simple classes for serving up logs via HTTP
   
   @note log_message overridden in order to log to standard logs rather than stderr.
   @note extensions_map expanded to handle Condor logfile extensions so user sees them as text rather
      than being offered a download. 
'''

import logging
import mimetypes
import os
import posixpath
import sys
import threading
import SimpleHTTPServer
import SocketServer
import time

import cgi
import urllib

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO



class MySimpleHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def guess_type(self, path):
        """Guess the type of a file.

        Argument is a PATH (a filename).

        Return value is a string of the form type/subtype,
        usable for a MIME Content-type header.

        The default implementation looks the file's extension
        up in the table self.extensions_map, using application/octet-stream
        as a default; however it would be permissible (if
        slow) to look inside the data to make a better guess.

        """
        self.extensions_map = mimetypes.types_map.copy()
        self.extensions_map.update({
                '': 'application/octet-stream', # Default
                '.py': 'text/plain',
                '.c': 'text/plain',
                '.h': 'text/plain',
                '.log': 'text/plain',
                '.out': 'text/plain',
                '.err': 'text/plain',
                '.jdl': 'text/plain',
                '.conf': 'text/plain',
                '.txt': 'text/plain',
                '.sh': 'text/plain',
        })
        
        base, ext = posixpath.splitext(path)
        if ext in self.extensions_map:
            return self.extensions_map[ext]
        ext = ext.lower()
        if ext in self.extensions_map:
            return self.extensions_map[ext]
        else:
            return self.extensions_map['']

    def log_message(self, format, *args):
            """Log an arbitrary message.
    
            This is used by all other logging functions.  Override
            it if you have specific logging wishes.
    
            The first argument, FORMAT, is a format string for the
            message to be logged.  If the format string contains
            any % escapes requiring parameters, they should be
            specified as subsequent arguments (it's just like
            printf!).
    
            The client host and current date/time are prefixed to
            every message.
    
            """
            
            log = logging.getLogger('main.logserver')
            log.info("%s - - [%s] %s\n" %
                             (self.address_string(),
                              self.log_date_time_string(),
                              format%args))
            
            #sys.stderr.write("%s - - [%s] %s\n" %
            #                 (self.address_string(),
            #                  self.log_date_time_string(),
            #                  format%args))


    def list_directory(self, path):
        """Helper to produce a directory listing (absent index.html).
    
        Return value is either a file object, or None (indicating an
        error).  In either case, the headers are sent, making the
        interface the same as for send_head().
    
        NOTE:
        we override here the method list_directory()
        The default one provided by the built-in web server
        produces a very simple directory listing. 
        We would like a layout as similar as possible as
        Apache would do. 
        For that, we create the table with the directory listing
        by hand.
        """

        # normalize path and verify it is underneath doocroot
        # to prevent some URL like <docroot>/../../../etc/password
        path = os.path.normpath(path)
        if not path.startswith(self.docroot):
            path = self.docroot

        try:
            list = os.listdir(path)
        except os.error:
            self.send_error(404, "No permission to list directory")
            return None
        list.sort(key=lambda a: a.lower())
        f = StringIO()
        displaypath = cgi.escape(urllib.unquote(self.path))

        f.write('<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">')
        f.write("<html>\n<title>Index of%s</title>\n" % path)
        f.write("<body>\n<h2>Index of %s</h2>\n" % path)

        f.write('<table><tr><th>Name</th><th>Last modified</th><th>Size</th></tr><tr><th colspan="3"><hr></th></tr>')

        # --------------------------------------------------------------------
        # When needed, add a link to parent directory, similar to the Apache one 
        if path != self.docroot:
            parent = path.replace(self.docroot,'', 1)
            parent = '/'.join( parent.split('/')[:-1]  )
            if parent == "":
                parent = "/"
            f.write('<tr><td><a href="%s">Parent Directory</a></td>    <td align="right"> </td>     <td align="right"></td>     </tr>' %(parent))
        # --------------------------------------------------------------------

        for name in list:
            fullname = os.path.join(path, name)

            # skip hidden files 
            if name.startswith('.'):
                continue 

            displayname = linkname = name
            # Append / for directories or @ for symbolic links
            if os.path.isdir(fullname):
                displayname = name + "/"
                linkname = name + "/"
            if os.path.islink(fullname):
                displayname = name + "@"
                # Note: a link to a directory displays with @ and links with /

            # --------------------------------------------------------------------
            # calculate the timestamp, with a format similar to Apache
            t_str = time.localtime(os.path.getmtime(fullname))
            date_modified = time.strftime("%d-%b-%Y %H:%M" ,t_str)
            # --------------------------------------------------------------------

            # --------------------------------------------------------------------
            # calculate the file size, with a format similar to Apache
            def sizeof_fmt(num):
                for x in ['','K','M','G','T']:
                    if num < 1024.0:
                        if x == '':
                                return "%d" % (num)
                        else:
                                return "%3.1f%s" % (num, x)
                    num /= 1024.0
            
            if os.path.isfile(fullname):
                size = sizeof_fmt(os.path.getsize(fullname))
            else:
                size = '-'
            # --------------------------------------------------------------------

            f.write('<tr><td><a href="%s">%s</a></td>    <td align="right">%s </td>     <td align="right">%s</td>     </tr>' %(urllib.quote(linkname), cgi.escape(displayname), date_modified, size))

        f.write('<tr><th colspan="3"><hr></th></tr>')
        f.write('</table>')
        
        # --------------------------------------------------------------------
        #  Write a footprint similar to Apache.
        #  It looks like (in italic): AutoPyFactory at my.host.org Port 25880
        import socket
        f.write('<address>AutoPyFactory at %s Port %s</address> ' %( socket.gethostname(), self.port))
        # --------------------------------------------------------------------

        f.write("</body>\n</html>\n")

        length = f.tell()
        f.seek(0)
        self.send_response(200)
        encoding = sys.getfilesystemencoding()
        self.send_header("Content-type", "text/html; charset=%s" % encoding)
        self.send_header("Content-Length", str(length))
        self.end_headers()
        return f




class MyNoListingHTTPRequestHandler(MySimpleHTTPRequestHandler):
    
    def list_directory(self, path):
        return None
        


class LogServer(threading.Thread):
    
    def __init__(self, port=25880, docroot="/home/apf/factory/logs", index = True):
        '''
        docroot is the path to the base directory of the files to be served. 
        '''
        threading.Thread.__init__(self)
        self.log= logging.getLogger('main.logserver')
        self.docroot = docroot
        self.port = int(port)
        self.index = index
        self.stopevent = threading.Event()
        if index:
            self.handler = MySimpleHTTPRequestHandler
        else:
            self.handler = MyNoListingHTTPRequestHandler

        # adding a couple of attributes to class handling
        # the HTTP requests. 
        # They are used to create a web page as similar as possible 
        # as Apache would do
        self.handler.port = port
        self.handler.docroot = docroot

        self.httpd = None
        self.log.debug("Initialized Logserver: port=%d, root=%s, index=%s" %(self.port,
                                                                             self.docroot,
                                                                             self.index))
        
    
    def _init_socketserver(self):
        while not self.httpd:
            try:
                self.log.debug("Attempting to bind to socket for HTTP server on port %s" % self.port)
                self.httpd = SocketServer.TCPServer(("", self.port), self.handler)
                self.log.debug("Initialized HTTP SocketServer port=%d, root=%s, index=%s" % (self.port, 
                                                                                             self.docroot, 
                                                                                             self.index)) 
            except Exception, e:
                self.log.warning("Socket server exception: %s" % str(e))
                self.log.warning("Attempt to initialize HTTP server failed. Will wait 60s and try again.")         
                time.sleep(60)
    
    def run(self):
        self.log.info("Initializing HTTP server...")
        self._init_socketserver()
        
        
        os.chdir(self.docroot)
        self.log.debug("Changing working dir to %s"%  self.docroot)
        while not self.stopevent.isSet():
            try:
                self.httpd.serve_forever()
            except Exception, e:
                self.log.error("HTTP Server threw exception: %s" % str(e))

    def join(self,timeout=None):
        '''
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        '''        
        self.stopevent.set()
        self.log.info('Stopping thread...')
        self.httpd.shutdown()
        threading.Thread.join(self, timeout)


def loop(ls):
        try:
                while True:
                        time.sleep(1)
        except (KeyboardInterrupt):
                ls.join()


                

# simple main for testing during development                
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG)
    ls = LogServer()
    #ls = LogServer(index = False)
    ls.start()
    loop(ls)
        
