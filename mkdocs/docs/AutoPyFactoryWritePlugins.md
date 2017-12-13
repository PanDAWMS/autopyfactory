</div>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>

<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Understanding plugins in AutoPyFactory</a> <ul>
<li> 3.1  Nomenclature</a>
</li> <li> 3.2  Singletons</a>
</li></ul> 
</li> <li> 4  Writing a new plugin</a> <ul>
<li> 4.1  Write a new batchsubmit plugin</a>
</li> <li> 4.2  Write a new batchstatus plugin</a>
</li> <li> 4.3  Write a new sched plugin</a>
</li></ul> 
</li> <li> 5 UML diagrams </a> <ul>
<li> 5.1  Plugins interfaces</a>
</li> <li> 5.2  Info classes in AutoPyFactory</a>
</li></ul> 

<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes how to write new plugins in <a href="../index.html" class="twikiLink">AutoPyFactory</a>
<p />
<p />
Conventions used in this document:
<p />
<p />
<font color="#808080">A <i>User Command Line</i> is illustrated by a green box that displays a prompt:</font>
<p />
<pre class="screen">
  [user@client ~]$
</pre>
<p />
<font color="#808080">A <i>Root Command Line</i> is illustrated by a red box that displays the <em>root</em> prompt:</font>
<p />
<pre class="rootscreen">
  [root@client ~]$
</pre>
<p />
<font color="#808080"><i>Lines in a file</i> are illustrated by a yellow box that displays the desired lines in a file:</font>
<pre class="file">
priorities=1
</pre>
<p />
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Understanding_plugins_in_AutoP"></a> 3  Understanding plugins in AutoPyFactory </h1>
<p />
Basic description of the different types of plugins and their purposes can be found in section "Plugins-based architecture" in <a href="../index.html" target="_top">here</a>
<p />
There are two basic ideas to keep in mind to write a new plugin for AutoPyFactory: <ul>
<li> plugins have an API
</li> <li> plugins use mappings to convert external information into internal abstract AutoPyFactory language
</li> <li> the name of the plugin is also the name of the class implementing it, and the name of the .py module containing it
</li> <li> all plugins' __init__() method always receive the same input parameters:  <ul>
<li> a reference to the calling object
</li> <li> the ConfigParser object containing configuration that may be relevant for the plugin
</li> <li> the corresponding section name for that ConfigParser object
</li></ul> 
</li></ul> 
<p />
Therefore, to write a new plugin, one only needs to pay attention to the API, which is defined in file <code>autopyfactory/interfaces.py</code>, 
and, when needed, add a new conversion table to file <code>etc/mappings.conf</code>
<p />
Plugins are placed in a well defined directory tree, depending on the type:
<pre class="file">
autopyfactory/
    plugins/
        factory/
        queue/
            batchstatus/
            batchsubmit/
            wmsstatus/
            monitor/
            sched/
        authmanager/
            auth/
</pre>
<p />
For example, for the sched plugin <code>Ready</code>, there is a class <code>Ready</code> in module <code>autopyfactory/plugins/queue/sched/Ready.py</code>
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="3_1_Nomenclature"></a> 3.1  Nomenclature </span></h2>
<p />
As mentioned, the name of the plugin is also the name of the class implementing it,  as well the name of the module containing it. 
<p />
In the AutoPyFactory configuration files, there are a few convention rules that help to keep a consistent nomenclature between all plugins: <ul>
<li> the key that refers to a given plugin is built as &lt; type_of_plugin &gt; + "plugin"
</li> <li> the name of the plugin is referred by its exact name (case sensitive)
</li> <li> keys for internal parameters to be digested by the plugins are always lower case, and built as &lt; type_of_plugin &gt; + "." + &lt; name_of_the_plugin_lower_case &gt; + "." + &lt; parameter &gt;
</li></ul> 
<p />
Example:
<p />
<pre class="file">
schedplugin = MinPerCycle
sched.minpercycle.minimum = 1
</pre>
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="3_2_Singletons"></a> 3.2  Singletons </span></h2>
<p />
Some plugins may act on behalf of many AutoPyFactory internal components. For example, the <code>batchstatus</code> plugins in charge of running periodically <code>condor_q</code> commands does it for many APFQueue objects.
In those cases, it makes sense to have that plugin be a <a href="https://en.wikipedia.org/wiki/Singleton_pattern" target="_top">Singleton</a>. 
<p />
Singletons are implemented in AutoPyFactory is by overriding the <strong><em>new</em></strong> method of the plugin class, and moving the actual code of the plugin functionalities to a separate class whose instances are returned by that __new__() implementation.
<p />
Example, for a plugin called MyPlugin, the module MyPlugin.py would have like this:
<p />
<pre class="file">

class _myplugin(....):

        def __init__(self, parent, config, section_name):
        ....
        ....

        def get(self):
        ...
        ...

        def put(self):
        ... 
        ...


class MyPlugin(object):

        instance = None

        def __new__(cls, *k, **kw):
                # here we check if an object
                # has already being created
                if not MyPlugin.instance:
                        MyPlugin.instance = _myplugin(*k, **kw)
                return MyPlugin.instance
</pre>
<p />
There is a special kind of Singletons in AutoPyFactory: the so called "MultiSingletons".
Those are plugins that, even doing the same for several parent calling objects, they may have differences in their setup. A typical example is the <code>batchstatus</code> plugin <code>Condor</code>, when it requires to query 2 different pools. 
In that case, we manage 2 plugin objects, each one being a Singleton. 
The implementation is similar:
<p />
<p />
<pre class="file">

class _myplugin(....):

        def __init__(self, parent, config, section_name):
        ....
        ....

        def get(self):
        ...
        ...

        def put(self):
        ... 
        ...


class MyPlugin(object):

        instances = {}

        def __new__(cls, *k, **kw):
                # here we check if a new object
                # is needed or not,
                # usually by inspecting the content of input options
                # passed to the plugin via *k
                #       parent = k[0]
                #       config = k[1]
                #       section = k[2]
                # and generating, when needed, a new key
                ...
                ...
                if key not in MyPlugin.instances.keys():
                        MyPlugin.instances[key] = _myplugin(*k, **kw)
                return MyPlugin.instances[key]
</pre>
<p />
<p />
<h1><a name="4_Writing_a_new_plugin"></a> 4  Writing a new plugin </h1>
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="4_1_Write_a_new_batchsubmit_plug"></a> 4.1  Write a new batchsubmit plugin </span></h2>
<p />
To write a new batchsubmit plugin, place the new module under directory <code>autopyfactory/plugins/queue/batchsubmit/</code>, and create a class inheriting the API.
<p />
<pre class="file">

from autopyfactory.interfaces import BatchSubmitInterface

class MySubmit(BatchSubmitInterface):

        def __init__(self, apfqueue, config, section):
            qname = apfqueue.apfqname
            foo = conf.get(qname, "batchsubmit.mysubmit.bar")    
   
        def submit(self, n):
             ...
             ...

</pre>
<p />
The method submit receives the number of pilots to be submitted. This method implements the code to perform the actual submission. 
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="4_2_Write_a_new_batchstatus_plug"></a> 4.2  Write a new batchstatus plugin </span></h2>
<p />
To write a new batchstatus plugin, place the new module under directory <code>autopyfactory/plugins/queue/batchstatus/</code>, and create a class inheriting the API.
<p />
<pre class="file">

from autopyfactory.interfaces import BatchStatusInterface
from autopyfactory.info import BatchStatusInfo
from autopyfactory.info import QueueInfo

class MyStatus(BatchStatusInterface):

        def __init__(self, apfqueue, config, section):
            qname = apfqueue.apfqname
            foo = conf.get(qname, "batchstatus.mystatus.bar")    
            mytarget2info_dict = apfqueue.factory.mappingscl.section2dict('MAPPINGS-FOR-MY-TARGET')

       def getInfo(self):
            ...

</pre>
<p />
An example of mappings is 
<p />
<pre class="file">
[CONDORBATCHSTATUS-APFINFO]
0 = pending
1 = pending
2 = running
3 = done
4 = done
5 = suspended
6 = running
</pre>
<p />
The current list of internal AutoPyFactory statuses is 
<p />
<pre class="file">
pending
running
suspended
done
</pre>
<p />
<p />
Method getInfo() returns aggregate statistics about jobs in batch system, indexed by queue.
It returns an object of class BatchStatusInfo, defined in <code>autopyfactory/info.py</code>, 
which is a dictionary of QueueInfo objects, also defined in <code>autopyfactory/info.py</code>.
<p />
The BatchStatusInfo dictionary is indexed by APFQueue name, the QueueInfo dictionary is filled via its method fill(), which receives as inputs a dictionary with status values and the corresponding mapping to convert those values into internal AutoPyFactory agnostic vocabulary. 
An example of dictionary to be passed would be:
<pre class="file">
{'0':5,
'2':10}
</pre>
where the mappings will then convert '0' into 'pending' and '2' into 'running'
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="4_3_Write_a_new_sched_plugin"></a> 4.3  Write a new sched plugin </span></h2>
<p />
The Sched plugins have a very simple interface, implemented in module SchedInterface: just a method <code>calcSubmitNum(self, n=0)</code>.
This method calculates how many new pilots to submit next time, implementing a given algorithm or policy. 
<p />
The output returned by the method is a tuple of two items <code>(out, msg)</code> where <code>out</code> is the number of pilots to be submitted (can be a negative number), and <code>msg</code> is a string representing the decision made. 
This string message can then be propagated to some monitoring services. 
<p />
<pre class="file">
...
from autopyfactory.interfaces import SchedInterface

class MySched(SchedInterface):
   
        def __init__(self, apfqueue, config, section):
                ...
   
        def calcSubmitNum(self, n=0):
                ...
                out = 123
                msg = 'MySched:in=%s;ret=%s' %(n, out)
                return (out, msg)
</pre>
<p />
<p />
<p />
<p />
<h1><a name="5_UML_diagrams"></a> 5  UML diagrams </h1>
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="5_1_Plugins_interfaces"></a> 5.1  Plugins interfaces </span></h2>
<p />
<img src="https://twiki.grid.iu.edu/twiki/pub/Documentation/Release3/AutoPyFactoryWritePlugins/APF_plugins_interfaces.png">
<p />
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="5_2_Info_classes_in_AutoPyFactor"></a> 5.2  Info classes in AutoPyFactory </span></h2>
<p />
<img src="https://twiki.grid.iu.edu/twiki/pub/Documentation/Release3/AutoPyFactoryWritePlugins/APF_info_classes.png">
<p />




</body></html>
