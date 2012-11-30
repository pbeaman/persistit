.. _Configuration:

Configuration
=============

To initialize Akiban Persistit the embedding application specifies a configuration and then invokes the ``com.persistit.Persistit#initialize()`` method. The configuration defines parameters used to determine locations of files, sizes of buffer pool and journal files, policies and other elements required when Persistit starts up. These parameters are managed by the ``com.persistit.Configuration`` class.

An application can define the configuration in one of two equivalent ways:

- Create a ``com.persistit.Configuration`` instance and set its properties through methods such as ``com.persistit.Configuration#setJournalPath``.
- Specify properties by name in a ``java.util.Properties`` instance and then pass the ``Properties`` to a ``Configuration`` constructor.

The following code samples show different ways of configuring and starting Persistit:

.. code-block:: java

  final Persistit db = new Persistit();
  final Properties p = new Properties();
  p.setProperty("buffer.count.16384", "1000");
  p.setProperty("journalpath", "/home/akiban/data/journal");
  ...
  db.setProperties(p);
  db.initialize();

.. code-block:: java

  final Persistit db = new Persistit();
  db.setPropertiesFromFile("/home/akiban/my_config.properties");
  db.initialize();

.. code-block:: java

  final Persistit db = new Persistit();
  final Configuration c = new Configuration();
  c.getBufferPoolMap().get(16384).setCount(1000);
  c.setJournalPath("/home/akiban/data/journal");
  ...
  db.setConfiguration(c);
  db.initialize();

There are three essential elements in a Persistit configuration:

- Memory for the buffer pool(s)
- Specifications for ``com.persistit.Volume`` instances
- Journal file path

Configuring the Buffer Pool
---------------------------

During initialization Persistit allocates a fixed amount of heap memory for use as buffers. Depending on the application, it is usually desirable to allocate most of a server’s physical memory to the JVM heap (using the ``-Xmx`` and ``-Xms`` JVM options) and then to allocate a large fraction of the heap to Persistit buffers. The buffer pool allocation is determined during initialization and remains constant until the embedding application calls ``com.persistit.Persistit#close`` to release all resources.

The number of buffers, and therefore the heap memory consumed, is determined during initialization by the available heap memory and configuration parameters. The heap memory size is obtained from the platform MemoryMXBean which in turn supplies the value given by the ``-Xmx`` JVM property. Configuration parameters are specified by a collection of five ``com.persistit.Configuration.BufferPoolConfiguration`` objects, one for each of the possible buffer sizes 1,024, 2,048, 4,096, 8,192 and 16,384 bytes. The method ``com.persistit.Configuration#getBufferPoolMap(int)`` gets the ``BufferPoolConfiguration`` for a specified buffer size.

A ``BufferPoolConfiguration`` contains the following attributes:

  ``minimumCount``
      lower bound on the number of buffers. (Default is zero.)
  ``maximumCount``
      upper bound on the number of buffers. (Default is zero.)
  ``minimumMemory``
      lower bound on memory to allocate. (Default is zero bytes.)
  ``maximumMemory``
      upper bound on memory to allocate. (Default is Long.MAX_VALUE bytes.)
  ``reservedMemory``
      minimum number of bytes to reserve for use other than buffers.  (Default is zero.)
  ``fraction``
      floating point value between 0.0f and 1.0f indicating how much of available memory too allocate.  (Default is 1.0f.)

Persistit uses the following algorithm to determine the number of buffers to allocate for each buffer size:

.. code-block:: java

  memoryToUse = fraction * (maxHeap - reservedMemory)
  memoryToUse = min(memoryToUse, maximumMemory)
  bufferCount = memoryToUse / bufferSizeWithOverhead
  bufferCount = max(minimumCount, min(maximumCount, bufferCount))
  if (bufferCount * bufferSize > maximumMemory) then FAIL
  if ((bufferCount + 1) * bufferSize < minimumMemory) then FAIL
  allocate bufferCount buffers

In other words, Persistit computes a buffer count based on the memory parameters, bounds it by ``minimumCount`` and ``maximumCount`` and then checks whether the resulting allocation fits within the memory constraints. Note that ``bufferSizeWithOverhead`` is about 14% larger than the buffer size; the additional memory is reserved for indexing data and other overhead associated with the buffer.

Typically an application uses a single buffer size, specifying either an absolute count or memory-based constraints for that size. This can be done by setting the attributes of the appropriate ``BufferPoolConfiguration`` object directly, or using Property values.

The property named ``buffer.count.SSSS`` where ``SSSS`` is “1024”, “2048”, “4096”, “8192” or “16384” specifies an absolute count.  For example,

.. code-block:: java

  buffer.count.8192 = 10000

causes Persistit to allocate 10,000 buffers of size 8192.

The property ``buffer.memory.SSSS`` specifies memory constraints as shown in this example

.. code-block:: java

  buffer.memory.8192 = 512K,20M,4M,0.6

where 512K, 20M, 4M and 0.6 are the ``minimumMemory``, ``maximumMemory``, ``reservedMemory`` and ``fraction``, respectively.

The MemoryMXBean supplies as its maximum heap size value the size given by the ``-Xmx`` JVM parameter.

Heap Tuning
-----------

This section pertains to the Oracle HotSpot(tm) Java virtual machine.

.. note:: 

   Buffer instances are long-lived objects. To avoid severe garbage collector overhead it is important for all of them    
   to fit in the heap’s tenured generation. This issue becomes especially significant with multi-gigabyte heaps.

By default the HotSpot server JVM allocates 1/3 of the heap to the new generation and 2/3 to the tenured generation, meaning that allocating more than 2/3 of the heap to buffers will result in bad performance.

You can increase the fraction by specifying ``-XX:NewRatio=N`` where ``N`` indicates the ratio of tenured generation space to new generation space, or by using the ``-Xmn`` parameter to specify an absolute amount of memory for the new generation.  Also, setting ``-Xms`` equal to ``-Xmx`` will avoid numerous garbage collection cycles during the start-up process.

See [http://www.oracle.com/technetwork/java/javase/gc-tuning-6-140523.html] for further information on tuning the heap and garbage collector for the HotSpot JVM.

Multiple Buffer Pools
---------------------

In some cases it may be desirable to allocate two or more buffer pools having buffers of different sizes. For example, it may be beneficial to use a large number of small buffers to hold secondary index pages.

When specifying multiple memory constraints for multiple buffer pools, the ``fraction`` property applies to the available memory before any buffers are allocated. So, for example,

.. code-block:: java

  buffer.memory.2048=64M,512G,2G,.2
  buffer.memory.16384=64M,512G,2G,.5

results in two buffer pools having buffers of size 2,048 bytes and 16,384 bytes, respectively. Assuming that the ``-Xmx`` value is 12G, then 2,048 byte buffers will be allocated to fill 20% of 10GByte, 16,384 byte buffers will be allocated to fill 50% of 10GByte, and approximately 5GByte (30% of 10GByte plus 2GByte reserved) will be available to application code.

Configuring Volumes
-------------------

Persistit creates and/or opens a set of database volume files during start-up. An application can create, open and close additional volumes, but it is often convenient for volumes to be defined in the confiuration, outside of application code.

The ``com.persistit.Configuration#getVolumeList`` method returns a List of ``com.persistit.VolumeSpecification`` objects. An application can construct and add new ``VolumeSpecification`` instances to this list before calling ``com.persistit.Persistit#initialize(Configuration)``.  Alternatively, the application can define volume specifications as property values using the syntax:

``volume.N = path[,attrname[:attrvalue]]...``

where ``N`` is an arbitrary integer, ``path`` is the path specification of the volume file, and ``attrnames`` include:

- ``pageSize``: Fixed length unit representing one page. Value must be one of 1024, 2048, 4096, 8192 or 16384. To open and use the Volume, the buffer pool must have available buffers of the same size.

- ``create``: Persistit attempts to open an existing volume file with the specified *path*, or create a new one if the file does not exist.

- ``createOnly``: Persistit throw a VolumeAlreadyExistsException if the file specified by path already exists. Otherwise it creates a new file with the specified path.

- ``readOnly``: Opens a volume in read-only mode. An attempt to modify the volume results in a ReadOnlyVolumeException.

- ``initialPages`` or ``initialSize``: Specifies the initial size of the newly created volume file, either as the count of pages or as the size in bytes.

- ``extensionPages`` or ``extensionSize``: Specifies the extension size of the newly created volume, either as the count of pages or as the size in bytes. This is the size by which the volume file will expand when the volume needs to be enlarged.

- ``maximumPages`` or ``maximumSize``: An upper limit on the number of pages this Volume may hold, either as the count of pages or as the size in bytes. An attempt to further enlarge the Volume will generate a VolumeFullException.

- ``alias``: The name of this Volume used in constructing ``Exchange`` instances.  If unspecified, the name is the simple file name given in the *path*, not including its dotted suffix.

For example::

  volume.1=/home/akiban/ffdemo,create,pageSize:16K,\
      initialSize:10M,extensionSize:10M,maximumSize:1G

specifies a volume having the name “ffdemo” in the /home/akiban directory. A new volume will be created if there is no existing volume file, and when created it will have the initial, extension and maximum sizes of 10MByte, 10MByte and 1GByte, respectively. Its page size will be 16KByte, meaning that the configuration must also have a buffer pool of 16KByte buffers.

System Volume
-------------

One volume in a Persistit configuration must be designated as the system volume. It contains class meta data for objects stored serialized in Persistit Values. When a configuration specifies only one volume, that volume implicitly becomes the system volume by default. However, when a configuration specifies multiple volumes, you must indicate which volume will serve as the system volume. There are two ways to do this. By default, Persistit looks for a unique volume named “_system”. You can simply create a volume whose file name is “_system”.

Alternatively, you can specify a system volume name explicitly with the ``sysvolume`` property (or ``com.persistit.Configuration#setSysVolume``). The value is the name or alias of the selected volume.

Configuring the Journal Path
----------------------------

The :ref:`Journal` consists of a series of sequentially numbered files located in directory specified by the configuration parameter ``journalpath``. The application can set this property by calling ``com.persistit.Configuration#setJournalPath`` prior to initializing Persistit or through the property::

  journalpath=/ssd/data/my_app_journal

The value specified can be either a 

- directory, in which case files named ``persistit_journal.NNNNNNNNNNNN`` will be created, 
- or a file name, in which case journal files will be created by appending the suffix ``.NNNNNNNNNNNN``.

Recommendations for Physical Media
----------------------------------

The journal is written by appending records to the end of the highest-numbered file. Read operations occur while copying page images from the journal to their home volume files. While copying, Persistit attempts to perform large sequential read operations from the journal. Read operations also occur at random when Persistit needs to reload the image of a previously evicted page.

Because of these characteristics a modern enterprise SSD (solid disk drive) is ideally suited for maintaining the journal. If no SSD is available in the server, placing the journal on a different physical disk drive than the volume file(s) can significantly improve performance.

However, beware of consumer-grade SSDs. Some may cache writes and record them on non-volatile storage much later. In a power failure event, committed transactions recorded only in the write cache can be lost. The durability of committed transactions can only be guaranteed if the hardware stack cooperates in making sure all writes are durable.

Buffer Pool Preload
-------------------

Persistit includes an optional facility to record periodically an inventory of pages in the buffer pool. When Persistit starts up it attempts to re-read the same pages that were previously in the buffer pool so that performance after restart on a similar workload (e.g., queries that exhibit a moderate or strong degree of locality-of-reference) will run at full speed. This facility can be especially important in large buffer pool configurations in which under normal workloads a server may otherwise require minutes to hours to reach optimal speed due to a large number of random reads. Re-reading the inventory at startup can be much faster since pages are read in approximate physical order.

This facility is controlled by two configuration parameters:

- ``com.persistit.Configuration#setBufferInventoryEnabled`` controls whether Persistit records the inventory. If enabled, the recording happens once per Checkpoint and once at normal shutdown.  The inventory is stored in the system volume.

- ``com.persistit.Configuration#setBufferPreloadEnabled`` controls whether Persistit attempts to preload the buffer pool during startup. If there is a recorded inventory Persistit attempts to re-read the pages that were previously present; otherwise it silently continues the startup.

If upon restart the buffer pool has become smaller so that the inventory is larger than the current buffer pool, Persistit only loads as many pages as there currently are buffers.

Although by default these configuration properties are false, we recommend enabling them for production servers because the inventory process takes very little time, and buffer preload can restore the buffer pool to a useful working set orders of magnitude faster than warming it up through normal load.

Other Configuration Parameters
------------------------------

The following additional properties are defined for Persistit. Other properties may also reside in the Properties object or its backing file; Persistit simply ignores any property not listed here.

  ``journalsize``: (``com.persistit.Configuration#setJournalSize``) 
      Journal file block size. Default is 1,000,000,000 bytes. A new Persistit rolls over to a new journal file when this 
      size is reached. Generally there is no reason to adjust this setting.

  ``appendonly``: (``com.persistit.Configuration#setAppendOnly``), True or false (default).  
      When true, Persistit’s journal starts up in *append-only* mode in which modified pages are only written to the 
      journal and not copied to their home volumes. As a consequence, all existing journal files are preserved, and new 
      modifications are written only to newly created journal files. The append-only flag can also be enabled or disabled 
      by application code and through the JMX and RMI interfaces.

  ``rmiport``: (``com.persistit.Configuration#setRmiPort``) 
      Specifies a port number on which Persistit will create a temporary Remote Method Invocation registry.  If this 
      property is specified, Persistit creates a registry and registers a ``com.persistit.Management`` server on it. This 
      allows remote access to management facilities within Persistit and permits the Swing-based administrative utility to 
      attach to and manage a Persistit instance running on a headless server. The ``rmihost`` and ``rmiport`` properties 
      are mutually exclusive.

  ``rmihost``: (``com.persistit.Configuration#setRmiHost``) 
      Specifies the URL of an Remote Method Invocation registry.  If present, Persistit registers its a server for its 
      ``com.persistit.Management`` interface at the specified external registry. The ``rmihost`` and ``rmiport`` 
      properties are mutually exclusive.

  ``jmx``: (``com.persistit.Configuration#setJmxEnabled``), True (default) or false. 
      Specifies whether Persistit registers MXBeans with the platform MBean server. Set this value to ``true`` to enable 
      access from ``jconsole`` and other management tools.

  ``serialOverride``, ``constructorOverride``: (``com.persistit.Configuration#setSerialOverride`` ``com.persistit.Configuration#setConstructorOverride``) 
      Control aspects of object serialization. See :ref:`Serialization`.

  ``showgui``: (``com.persistit.Configuration#setShowGUI``), True or False (default).  
      If true, Persistit attempts to create and display an instance of the AdminUI utility panel within the current JVM. 
      Alternatively, AdminUI uses RMI and can be launched and run remotely if ``rmiport`` or ``rmihost`` has been 
      specified.

  ``logfile``: (``com.persistit.Configuration#setLogFile``) 
      Name of a log file to which Persistit’s default logger will write diagnostic log entries. Applications generally 
      install a logging adapter to reroute messages through Log4J, SLF4J or other logger. The ``logfile`` property is used 
      only when no adapter has been installed.

  ``bufferinventory``: (``com.persistit.Configuration#setBufferInventoryEnabled``), True or False (default).
      If true, Persistit periodically records an inventory of all the pages in the buffers pools to the System Volume. The inventory
      enables Persistit to preload the buffer pools then next time it starts up with approximately the same pages that were present
      before shutdown. To enable buffer preloading, the bufferpreload property must also be true.
      
  ``bufferpreload``: (``com.persistit.Configuration#setBufferPreloadEnabled``), True or False (default).
      If true, and if a buffer pool inventory was previously recorded, Persistit attempts to "warm up" the buffer pool
      by preloading pages that were present in the buffer pool when Persistit last shut down. This may allow a freshly started
      Persistit instance to begin servicing a workload similar to what it had previously been handling without incurring the
      cost of many random disk reads to load pages.
        

For all integer-valued properties, the suffix “K” may be used to represent kilo, “M” for mega, “G” for giga and “T” for tera. For example, “2M” represents the value 2,097,152.

A Configuration Example
-----------------------

Following is an example of a Persistit configuration properties file::

  datapath = /var/opt/persistit/data
  logpath = /var/log/persistit
  logfile = ${logpath}/${timestamp}.log

  buffer.count.16384 = 5000

  volume.1 = ${datapath}/demo_data, create, pageSize:16K, \
  	  initialSize:1M, extensionSize:1M, maximumSize:10G, alias:data

  volume.2 = ${datapath}/demo_system, create, pageSize:16K, \
	  initialSize:100K, extensionSize:100K, maximumSize:1G

  sysvolume = demo_system

  journalpath = /ssd/persistit_journal

With this configuration there will be 5,000 16K buffers in the buffer pool consuming heap space of approximately 93MB including overhead. Persistit will open or create volume files named ``/var/opt/persistit/data/demo_data`` and ``/var/opt/persistit/data/demo_system`` and a journal file named ``/ssd/persistit_journal.0000000000000000``. Persistit will write diagnostic logging output to a file such as ``/var/log/persistit/20110523172213.log``.

The ``demo_data`` volume has the alias ``data``. Application code uses the name "data" to refer to it. The ``sysvolume`` property specifies that the ``demo_system`` volume is designated to hold class meta data for serialized objects.

Property Value Substitution
---------------------------

This example also illustrates how property value substitution can be used within a Persistit configuration.  The value of the ``datapath`` replaces ``${datapath}`` in the volume specification. The property name ``datapath`` is arbitrary; you may use any valid property name as a substitution variable. Similarly, the value of ``logpath`` replaces ``${logpath}`` and the pseudo-property ``${timestamp}`` expands to a timestamp in the form ``*yyyyMMddHHmm*`` to provides a unique time-based log file name.

Incorporating Java System Properties
------------------------------------

You may also specify any configuration property as a Java system property with the prefix ``com.persisit.`` System properties override values specified as properties. For example, you can override the value of ``buffer.count.8192`` specifying::

  java -Dcom.persistit.buffer.count.8192=10K -jar MyJar

This is also true for substitution property values. For example, ``-Dcom.persistit.logpath=/tmp/`` will place the log files in the ``/tmp`` directory rather than ``/var/log/persistit`` as specified by the configuration file.
