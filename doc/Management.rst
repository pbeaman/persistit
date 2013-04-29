.. _Management:

Management
==========

Akiban Persistit provides three main avenues for measuring and managing its internal resources: an RMI interface, a JMX interface and a command-line interface capable of launching various utility tasks. 

The RMI interface is primarily intended for the com.persistit.ui.AdminUI utility. AdminUI is a JFC/Swing program that runs on a device with graphical UI capabilities.  For example, in Linux and Unix it requires an XServer. Since production servers are usually headless it is often necessary to run AdminUI remotely, via its RMI interface. To do this, the Persistit configuration must specify either the ``rmiport`` or ``rmihost`` property so that it can start an RMI server.

Suppose a Persistit-based application is running on a host named “somehost” and has specified the configuration property ``rmiport=1099`` in its configuration.  Then the AdminUI can be launched as follows to connect with it:

.. code-block:: java

  java -cp classpath  com.persistit.ui.AdminUI somehost:1099

where classpath includes the Persistit ``com.persistit.ui`` package. 

MXBeans
-------

The JMX interface can be used by third-party management utilities, from applications such as ``jconsole`` and ``visualvm``, and from command-line JMX clients such as ``jmxterm``. To enable JMX access, the configuration must specify the property ``jmx=true``.  This causes Persistit to register several MBeans with the platform MBean server during initialization.

The following JMX MXBeans are available:

  ``com.persistit:type=Persistit``
      See ``com.persistit.mxbeans.ManagementMXBean``
  ``com.persistit:type=Persistit,class=AlertMonitorMXBean``
      See ``com.persistit.mxbeans.AlertMonitorMXBean``.
      Accumulates, logs and emits notifications about abnormal events such as IOExceptions and measurements outside of 
      expected thresholds.
  ``com.persistit:type=Persistit,class=CleanupManagerMXBean``
      See ``com.persistit.mxbeans.CleanupManagerMXBean``.
      View current state of the Cleanup Manager. The Cleanup Manager performs background pruning and tree maintenance 
      activities.
  ``com.persistit:type=Persistit,class=IOMeter``
      See ``com.persistit.mxbeans.IOMeterMXBean``.
      Maintains statistics on file system I/O operations.
  ``com.persistit.type=Persistit,class=JournalManager``
      See ``com.persistit.mxbeans.JournalManagerMXBean``.
      Views current journal status.
  ``com.persistit.type=Persistit,class=RecoveryManager``
      See ``com.persistit.mxbeans.RecoveryManagerMXBean``.
      Views current status of the recovery process. Attributes of this MXBean change only during the recovery process.
  ``com.persistit:type=Persistit,class=TransactionIndexMXBean``
      See ``com.persistit.mxbeans.TransactionIndexMXBean``.
      View internal state of transaction index queues and tables.
  ``com.persistit.type=Persistit,class=BufferPool.*SSSS*``
      where *SSSS* is a buffer size (512, 1024, 2048, 4096 or 16394). See ``com.persistit.mxbeans.BufferPoolMXBean``. View utilization statistics for buffers of the 
      selected size.


For details see the JavaDoc API documentation for each MXBean interface.

Management Tasks
----------------

Persistit provides several ways to launch and administer ``com.persistit.Task`` instances.  A ``Task`` is a management operation that may take a significant amount of time and usually runs in a separate thread. For example, ``com.persistit.IntegrityCheck`` is a ``Task`` that verifies the internal structural integrity of one or more trees and can run for minutes to hours, depending on the size of the database.  The AdminUI tool, ``com.persistit.mxbeans.ManagementMXBean`` and the command-line interface (:ref:`CLI`) provide mechanisms to launch, suspend or stop a task, and to monitor a task’s progress.

Currently the following built-in Tasks are available:

  ``icheck``
      Check the integrity of one or more trees or volumes.
  ``save``
      Save selected key-value pairs from one or more trees to a flat file.
  ``load``
      Load selected key-value pairs from a flat file written by ``save``.
  ``backup``
      Control and/or perform a concurrent backup of one more more volumes.
  ``stat``
      Aggregate various performance statistics and either return them immediately, or write them periodically to a file.
  ``task``
      Check the status of an existing task.  This task can also suspend, resume or stop an existing task. This task, which 
      immediately returns status information, can be used by external tools to poll the status of other tasks.
  ``cliserver``
      Start a simple command-line server on a specified port.  This enables a client program to execute commands sending 
      them directly to that port.
  *other tasks*
      Various commands allow you to select and view pages and journal records.


Executing a Task from an Application
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``com.persistit.mxbeans.ManagementMXBean#execute`` and ``com.persistit.mxbeans.ManagementMXBean#launch`` methods both take a single String-valued argument, parse it to set up a ``Task`` and return a String-valued result. For example:

.. code-block:: java

  String taskId = db.getManagement().launch(“backup -z file=/tmp/mybackup.zip”);
  String status = db.getManagement().execute(“task -v -m -c taskId=” + taskId);

launches the backup task and then queries its status.

Executing a Task from a JMX Client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``com.persistit.mxbeans.ManagementMXBean#execute`` and ``com.persistit.mxbeans.ManagementMXBean#launch`` methods are exposed as operations on the ``com.persistit.mxbeans.ManagementMXBean`` object.  You can invoke tasks

- via ``jconsole`` by typing the desired command line as the argument of the ``execute`` operation.
- via a third-party JMX client such as ``jmxterm``.
- via the ``cliserver`` feature

Executing a Task Using a Third-Party JMX client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can use the ``jmxterm`` program, for example, (see [http://www.cyclopsgroup.org/projects/jmxterm]) to execute commands with the following shell script::

  #!/bin/sh
  java -jar jmxterm-1.0-alpha-4-uber.jar --verbose silent --noninteract --url $1 <<EOF
  run -d com.persistit -b com.persistit:type=Persistit execute $2
  EOF

To use this script, specify either the JMX URL or the process ID as the first command argument, and the command line as the second argument.  Example::

  peter:~/workspace/sandbox$ jmxterm-execute 1234 ‘stat\ -a’
  hit=3942334 miss=14 new=7364 evict=0 jwrite=81810 jread=2 jcopy=63848 tcommit=0 troll=0 CC=0 RV=12 RJ=2 WJ=81810 EV=0 FJ=529 IOkbytes=1134487 TOTAL

This command invokes the ``stat`` task with the flag ``-a`` on a JVM running with process id 1234.  Note that with jxmterm white-space must be quoted by backslash (‘\’) even though the argument list is also enclosed in single-quotes.  The backslash marshals the space character through ``jmxterm``’s parser. Commas and other delimiters also need to be quoted.

.. _cliserver:

Executing a Task Using the Built-In ``cliserver``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``cliserver`` is a simple text-based server that receives a command line as a text string and emits the generated output as its response. To start it, enter the command::

  cliserver port=9999

programmatically or through JMX. (You may specify any valid, available port.) Then use a command-line client to send command lines to that port and display their results. Persistit includes a primitive command-line client within the ``com.persistit.CLI`` class itself.  Create a script to invoke it as follows::

  #!/bin/sh
  java -cp classpath com.persistit.CLI localhost:9999 $*

Where ``classpath`` includes the Persistit library. Assuming the name of the script is ``pcli`` you can then invoke commands from a shell as shown in this example::

  /home/akiban:~$ pcli icheck -v -c "trees=*:Acc*"
  Volume,Tree,Faults,IndexPages,IndexBytes,DataPages,DataBytes,LongRecordPages,LongRecordBytes,MvvPages,MvvRecords,MvvOverhead,MvvAntiValues,IndexHoles,PrunedPages
  "persistit","AccumulatorRecoveryTest",0,3,24296,1519,15560788,0,0,1506,52192,721521,2397,0,0
  "*","*",0,3,24296,1519,15560788,0,0,1506,52192,721521,2397,0,0
  /home/akiban:~$

Alternatively, you can use ``curl`` as follows::

  #!/bin/sh
  echo "$*" | curl --silent --show-error telnet://localhost:9999

to issue commands.

.. caution::
   
   ``cliserver`` has no access control and sends potentially sensitive data in cleartext form. Therefore it should be used with care and only in a secure 
   network environment. Its primary mission is to allow easy inspection of internal data structures within Persistit.

.. _CLI:

The Command-Line Interface
--------------------------

The String value passed to the ``execute`` and ``launch`` operations specifies the name of a task and its arguments. The general form is::

  commandname -flag -flag argname=value argname=value

where the order of arguments and flags is not significant.


Command: ``icheck``
^^^^^^^^^^^^^^^^^^^

Performs a com.persistit.IntegrityCheck task. Arguments:

  ``trees``
      Specifies volumes and/or trees to check. See com.persistit.TreeSelector for details syntax. Default is all trees in all volumes.
  ``-r``
      Tree specification uses Java RegEx syntax (Default is to treat ‘*’ and ‘?’ as standard single-character and multi-character wildcards.
  ``-u``
      Don't freeze updates (Default is to freeze updates)
  ``-h``
      Fix index holes. An *index hole* is an anomaly that occurs rarely in normal operation such that a page does not have an index entry in the index page level 
      immediately above it
  ``-p``
      Prune obsolete MVV (multi-version value) instances while checking.
  ``-P``
      Prune obsolete MVV instances, and clear any remaining aborted TransactionStatus instances.  Use with care.
  ``-v``
      Emit verbose output. For example, emit statistics for each tree.
  ``-c``
      Display tree statistics in comma-separated-variable format suitable for import into a spreadsheet program.

Example::

  icheck trees=vehicles/* -h

Checks all trees in the ``vehicles`` volume and repairs index holes.

Command: ``save``
^^^^^^^^^^^^^^^^^

Starts a com.persistit.StreamSaver task. Arguments:

  ``file``
      Name of file to save records to (required)
  ``trees``
      Specifies volumes and/or trees to save. See com.persistit.TreeSelector for details syntax. Default is all trees in all volumes.
  ``-r``
      Tree specification uses Java RegEx syntax (Default is to treat ‘*’ and ‘?’ as standard single-character and multi-character wildcards.)
  ``-v``
      emit verbose output
  
...‘*’ and ‘?’ are standard wildcards.

Example::

  save -v file=/home/akiban/save.dat trees=vehicles/*{[“Edsel”:”Yugo”]}

Saves the records for “Edsel” through “Yugo”, inclusive, from any tree in the volume named ``vehicles``. See com.persistit.TreeSelector for selection syntax details.

Command: ``load``
^^^^^^^^^^^^^^^^^

Starts a com.persistit.StreamLoader task. Arguments:

  ``file``
      Name of file to load records from
  ``trees``
      Specifies volumes and/or trees to load. See com.persistit.TreeSelector for details syntax. Default is all trees in all volumes.
  ``-r``
      Tree specification uses Java RegEx syntax (Default is to treat ‘*’ and ‘?’ as standard single-character and multi-character wildcards.)
  ``-n``
      Don't create missing Volumes (Default is to create them)
  ``-t``
      Don't create missing Trees (Default is to create them)
  ``-v``
      Emit verbose output

‘*’ and ‘?’ are standard wildcards.

Example::

  load file=/home/akiban/save.dat trees=*/*{[“Falcon”:”Firebird”]}

For any tree in any volume, this command loads all records having keys between “Falcon” and “Firebird”, inclusive.

Command: ``backup``
^^^^^^^^^^^^^^^^^^^

Starts a ``com.persistit.BackupTask`` task to perform concurrent (hot) backup. Arguments:

  ``file``
      Archive file path. If this argument is specified, BackupTask will back up the database in .zip format to the specified file.  This is intended only for small 
      databases. It is expected that ``backup`` will be used in conjunction with high-speed third-party data copying utilities for production use. The ``-a`` and       
  ``-e`` 
      flags are incompatible with operation when the ``file`` argument is specified and are ignored.
  ``-a``
      Start appendOnly mode - for use with third-party backup tools.  ``backup -a`` should be invoked before data copying begins.
  ``-e``
      End appendOnly mode - for use with third-party backup tools.  ``backup -e`` should be invoked after data copying ends.
  ``-c``
      Request checkpoint before backup.
  ``-z``
      Compress output to ZIP format - meaningful only in conjunction with the ``file`` argument.
  ``-f``
      Emit a list of files that need to be copied. In this form the task immediately returns with a list of files currently comprising the Persistit database,  
      including Volume and journal files.
  ``-y``
      Copy pages from journal to Volumes before starting backup.  This reduces the number of journal files in the backup set.

Example::

    backup -y -a -c -y -f
    … invoke third-party backup tool to copy the database files
    backup -e

Uses the ``backup`` task twice, once to set *append-only* mode, checkpoint the journal and perform a full copy-back cycle (a process that attempts to shorten the journal), and then write out a list of files that need to be copied. The second call to ``backup`` restores normal operation.  Between these two calls a third party backup tool is used to copy the data.

Example::

    backup -z file=/tmp/my_backup.zip

Uses the built-in file copy feature with ZIP compression.

Command: ``task``
^^^^^^^^^^^^^^^^^

Queries, stops, suspends or resumes a background task.  Arguments:

  ``taskId``
      Task ID to to check, or -1 for all
  ``-v``
      Verbose - returns detailed status messages from the selected task(s)
  ``-m``
      Keep previously delivered messages. Default is to remove messages once reported.
  ``-k``
      Keep the selected task or tasks even if completed.  Default is to remove tasks once reported.
  ``-x``
      Stop the selected task or tasks
  ``-u``
      Suspend the selected task or tasks
  ``-r``
      Resume the selected task or tasks

Unlike other commands, the ``task`` command always runs immediately even if invoked through the ``launch`` method. 

You can use the ``task`` command to poll and display progress of long-running tasks. Invoke::

  task  -v -m -c taskId=nnn

until the result is empty.

Command: ``cliserver``
^^^^^^^^^^^^^^^^^^^^^^

Starts a simple text-based server that receives a command line as a text string and emits the generated output as its response. Argument:

  ``port``
      Port number on which to listen for commands.

Command: ``exit``
^^^^^^^^^^^^^^^^^

Ends a running ``cliserver`` instance.

Commands for Viewing Data
^^^^^^^^^^^^^^^^^^^^^^^^^

The following commands execute immediately, even if invoked through the ``launch`` method.  They provide a mechanism to examine individual database pages or journal records.

Command: ``select``
^^^^^^^^^^^^^^^^^^^

Selects a volume and optionally a tree for subsequent operations such as ``view``. Arguments:

  ``tree``
      Specifies volume and/or tree to select as context for subsequent operations. See com.persistit.TreeSelector for details syntax.
  ``-r``
      Tree specification uses Java RegEx syntax (Default is to treat ‘*’ and ‘?’ as standard single-character and multi-character wildcards.)

Command: ``list``
^^^^^^^^^^^^^^^^^

Lists volumes and trees.  Arguments:

  ``trees``
      Specifies volumes and/or trees to list. See com.persistit.TreeSelector for details syntax. Default is all trees in all volumes.
  ``-r``
      Tree specification uses Java RegEx syntax (Default is to treat ‘*’ and ‘?’ as standard single-character and multi-character wildcards.

All volumes, and all trees within those volumes, that match the ``trees`` specification are listed. By default, this command lists all trees in all volumes.

Command: ``pview``
^^^^^^^^^^^^^^^^^^

Displays contents of a database page. Arguments:

  ``page``
      page address
  ``jaddr``
      journal address - displays a page version stored at the specified journal address
  ``key``
      a key specified as a String defined in the com.persistit.Key class
  ``level``
      tree level of the desired page
  ``find``
      selected records in an index page surrounding a key that points to the specified page address
  ``-a``
      all records. If specified, all records in the page will be displayed.  Otherwise the output is abbreviated to no more than 20 lines.
  ``-s``
      summary - only header information in the page is displayed

The ``pview`` command identifies a page in one of three distinct ways: by page address, by journal address, or by key.  Only one of the three parameters ``page``, ``jaddr`` or ``key`` (with ``level``) may be used.

``page`` specifies the current version of a page having the specified address.  If there is a copy of the page in the buffer pool, that copy is displayed even if it contains updates that are not yet written to disk.

``jaddr`` specifies an address in the journal. Typical use is to invoke the ``jview`` command to view a list of journal records, and then to see a detailed view of one page record in the journal, invoke the ``pview`` command with its journal address.

``key`` specifies a key. By default the data page associated with that key will be displayed.  The data page is defined as level 0. The ``level`` parameter allows pages at various index levels to be viewed; for example ``level=1`` refers to the index page that points to the data page containing the specified key.

When examining an index page with potentially hundreds of records it is sometimes convenient to find the record that points to a particular child page, and also the records immediately before and after. Specifying the ``find`` parameter when viewing an index page abbreviates the displayed records to include just the first and last records in the page, plus a small range of records surrounding the one that points to the specified page. This mechanism provides a convenient way to find sibling pages.


Command: ``path``
^^^^^^^^^^^^^^^^^

For a specified key displays the sequence of pages from root of the tree to the data page containing they key. Argument:

  ``key``
      a key specified as a String defined in the com.persistit.Key class


Command: ``jview``
^^^^^^^^^^^^^^^^^^

Displays journal records.  Arguments:

  ``start``
      starting journal address (default = 0)
  ``end``
      end journal address (address = infinite)
  ``timestamps``
      range selection of timestamp values, e.g., “132466-132499” for records having timestamps between these two numbers, inclusive. Default is all timestamps.
  ``types``
      comma-delimted list of two-character record types, e.g., “JH,IV,IT,CP” to select only Journal Header, Identify Volume, Identify Tree and Check Point records 
      (see ``com.persistit.JournalRecord`` for definitions of all types.) Default value is all types.
  ``pages``
      range selection of page address for PA (Page) records, e.g., “1,2,13-16” to include pages, 1, 2, 13, 14, 15 or 16.
  ``maxkey``
      maximum display length of key values in the output. Default value is 42.
  ``maxvalue``
      maximum display length of values in the output. Default value is 42.
  ``path``
      journal file path. Default is the journal file path of the currently instantiated Persistit instance.
  ``-v``
      verbose format. If specified, causes PM (Page Map) and TM (TransactionMap) records to be be display all map elements.


Note that the journal on a busy system contains a large number of records, so entering the ``journal`` command without constraining the address range or record types may result in extremely lengthy output.

Command: ``open``
^^^^^^^^^^^^^^^^^

Opens a Persistit database for analysis. This task can only be used to examine a copy of a Persistit database that is not currently in use by an application. It works by attempting to open the volume and journal files using a synthesized configuration. It finds a collection of journal files and volume files specified by the ``datapath``, ``journalpath`` and ``volumepath`` arguments; from these it derives a set of properties that will allow it to examine those journals and volumes. By default all volumes are opened in read-only mode and cannot be changed by operations executed from the command-line interface.

If there already is an open Persistit instance, this command detaches it. For example, if you start ``cliserver`` from a live Persistit instance and then issue the ``open`` command, the live instance will continue to operate but ``cliserver`` will no longer be attached to it.

Note that you cannot ``open`` volumes that are already open in a running Persistit instance due to their file locks. However, you can copy open volumes and journal files to another location and ``open`` the copy. This is the primary use case for the ``open`` command: to analyze a copy of a database (for example a copy recovered from backup) without having to a launch the application software that embeds Persistit.

Arguments:

  ``datapath``
      a directory path for volume and journal files to be analyzed
  ``volumepath``
      overrides ``datapath`` to specify an alternative location for volume files.
  ``journalpath``
      overrides ``datapath`` to specify an alternative location for journal files.
  ``rmiport``
      specifies an RMI port to which an instance of the AdminUI can attach.
  ``-g``
      launch a local copy of AdminUI
  ``-y``
      attempt to recover committed transactions .

Note that even if you specify ``-y`` to recover transactions, the volume files will not be modified. But the ``open`` command will add a new journal file containing modifications caused by the recovery process. You can simply delete that file when done.

Command: ``close``
^^^^^^^^^^^^^^^^^^

Detach and close the current Persistit instance. If the CLI was started with a live Persistit instance then this command merely detaches from it; if the instance was created with the ``open`` command then ``close`` closes it and releases all related file locks, buffers, etc.

Command: ``source``
^^^^^^^^^^^^^^^^^^^

Execute command lines from a specified text file. Argument:

  ``file``
      file name of command input file
