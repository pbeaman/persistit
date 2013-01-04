************************************
Akiban Persistit Version 3.2.2
************************************

Overview
========
See http://www.akiban.com/akiban-persistit for a summary of features and benefits, licensing information and how to get support.

Documentation
=============
Users Guide: http://www.akiban.com/ak-docs/admin/persistit

JavaDoc: http://www.akiban.com/sites/all/libraries/persistit-api/index.html

Building Akiban-Persistit
=========================
Use Maven (maven.apache.org) to build Persistit.

To build::

  mvn install

The resulting jar files are in the ``target`` directory. To build the Javadoc::

  mvn javadoc:javadoc

The resulting Javadoc HTML files are in ``target/site/apidocs``.

Building and Running the Examples
---------------------------------

Small examples are located in the ``examples`` directory. Each has a short README file describing the example, and an Ant build script (http://ant.apache.org). After building the main akiban-persisit jar file using Maven, you may run::

  ant run

in each of the examples subdirectories to build and run the examples.

Buffer Pool Configuration
=========================
For optimal performance, proper configuration of the Persistit buffer pool is required.  See section "Configuring the Buffer Pool" in the configuration document http://www.akiban.com/ak-docs/admin/persistit/Configuration.html

.. note:: Especially when used with multi-gigabyte heaps, the default Hotspot JVM server heuristics are be suboptimal for Persistit applications. Persistit is usually configured to allocate a large fraction of the heap to Buffer instances that are allocated at startup and held for the duration of the Persistit instance. For efficient operation, all of the Buffer instances must fit in the tenured (old) generation of the heap to avoid very significant garbage collector overhead.  Use either -XX:NewSize or -Xmn to adjust the relative sizes of the new and old generations.

|
|

Version History
===============

+---------+--------------------+--------------------------------------------------------------------------+
| Version | Release Date       |  Summary                                                                 |
+=========+====================+==========================================================================+
| 3.2.2   | November 30, 2012  | Better support for Spring Framework. Fix rare but serious bugs found in  |
|         |                    | stress tests. Fix issue related to locale and make sure Persistit builds |
|         |                    | everywhere.                                                              |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.2.1   | November 13, 2012  | Fix several low-priority bugs.                                           |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.2.0   | October 15, 2012   | Fix several critical and other bugs, one of which requires a modified    |
|         |                    | journal file format. This version also significantly improves            |
|         |                    | performance for I/O-intensive concurrent transaction loads.              |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.1.7   | September 11, 2012 | Fix several bugs, add buffer pool preload ("warm-up"),                   |
|         |                    | reformat code base                                                       |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.1.6   | August 24, 2012    | Fix bug 1036422                                                          |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.1.5   | August 6, 2012     | Fix bug 1032701, modify pom.xml for Eclipse Juno                         |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.1.4   | August 3, 2012     | License changed to Eclipse Public License, various other enhancements    |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.1.2   | July 13, 2012      | Fix several bugs                                                         |
+---------+--------------------+--------------------------------------------------------------------------+
| 3.1.1   | May 31, 2012       | First open source release of Akiban Persistit                            |
+---------+--------------------+--------------------------------------------------------------------------+


Resolved Issues
===============

{{bug-list}}

Changes and New Features
========================

Persistit 3.2.2 - Spring Framework
-----------------------------------------------------
Prior to this release Persistit was needlessly difficult to configure and initialize within Spring Framework.
This version provides new setter methods and constructors on the com.persistit.Persistit object to allow easy
injection of configuration properties and make it possible to inject a fully instantiated
Persistit instance within a Spring project. In addition, new methods were added to the 
com.persistit.Configuration class to simplify supplying buffer pool and initial volume specifications.
Three of the ``com.persistit.Persistit#initialize`` methods were deprecated.

This release also adds a new sample application that shows how a configured Persistit instance can be created. For
Maven users, note that the pom.xml file now includes a dependency on Spring Framework in test scope only; Persistit 
can still be deployed without any external dependencies.

Persistit 3.2.2 - Bug Fixes
-----------------------------------------------------
Version 3.2.2 corrects two issues that were identified through stress tests. For this release
we added hundreds of hours of stress-testing experience and will continue to invest in ongoing testing.

This version also fixes a unit test with string literals containing numbers formatted according to en_US
conventions. The test has been corrected and the Persistit build has been tested in several locales.

Persistit 3.2.1 - Bug Fixes
-----------------------------------------------------

Version 3.2.1 is a maintenance release that fixes a number of non-critical bugs, primarily in less frequently
used sections of the API. See the associated bug list for full descriptions of each resolved.

Persistit 3.2.0 - Default Journal File Format Changed
-----------------------------------------------------

Version 3.2.0 fixes problems related to Volumes created and opened by the com.persistit.Persistit#loadVolume 
method rather than being specified by the initial system configuration. In previous versions, journal files 
contained insufficient information to properly recover such volumes, even during normal startup.

To solve this problem the format of the IV (identify volume) journal record changed to include 
the com.persistit.VolumeSpecification rather than the volume name alone. By default, journal 
files written by earlier versions of Persistit continue to be supported, but once Persistit 3.2.0 has 
added one or more new journal files to a database, earlier versions of Persistit are unable to 
open the database.

In most cases it is never necessary to revert to an earlier version, but to support sites where 
backward-compatibility may be important, it is possible to specify a new configuration parameter 
(see com.persistit.Configuration#setUseOldVSpec). When this property is +true+, Persistit writes 
journal files that are backward-compatible, but incapable of supporting dynamically created volumes.

Persistit 3.2.0 - Performance Improvements
------------------------------------------

Version 3.2.0 significantly improves I/O performance for concurrent transactions and
better controls the number of journal files created during by very aggressive loads. A new attribute
in the com.persistit.mxbeans.JournalManagerMXBean class called urgentFileCountThreshold controls the
maximum number of journal files Persistit will create before ramping the up the urgency of copying pages to
allow purging old files. Several other issues related to I/O scheduling and management of 
dirty pages were resolved.

Version 3.2.0 significantly improves scheduling of version pruning operations.  Pruning is the process by
which obsolete versions are removed from multi-version values (MVVs). Better scheduling results in a
significant reduction in the amount of space consumed by obsolete version and also results in better
transaction throughput.

Persistit 3.1.7 - Code Base Reformatted
---------------------------------------

To simplify diffs and improve legibility, the entire code base was reformatted and "cleaned up" 
by the Eclipse code formatting tool. The maven build now automatically formats all
source to ensure coherent diffs in the future.  The settings for formatting and code style 
cleanup by Eclipse are found in the ``src/etc`` directory.

Persistit 3.1.7 - Buffer Pool Preload
-------------------------------------

On a server with a large buffer pool (many gigabytes), a Persistit instance can run for a long 
time before the buffer pool becomes populated with a suitable working set of database pages. 
Until then performance is degraded due to a potentially large number of random reads. 
For a production server the result may be poor performance for minutes to hours after restart.

The preload facility periodically records an inventory of the pages currently in the buffer 
pool(s) and optionally reloads the same set of pages when Persistit is restarted. During the 
preload process Persistit attempts to read pages in approximate file-address order to 
accelerate reads. In one of our experiments Persistit preloads a buffer pool with over 
800,000 16Kbyte buffers in about a minute, which is orders of magnitude faster than the 
same process would take with reads performed incrementally at random.

Two new configuration properties com.persistit.Configuration#setBufferInventoryEnabled and 
com.persistit.Configuration#setBufferPreloadEnabled control this behavior. These settings 
are turned off by default in version 3.1.7.

Persistit 3.1.4 - Detecting and Ignoring Missing Volumes
--------------------------------------------------------

Every time Persistit writes a modified page to disk, it does so first to the journal. 
During recovery processing, the page images from the journal are analyzed and reinserted 
into volumes in such a way that all B+Trees are restored to a consistent state. The issue 
addressed in this change is how Persistit behaves during recovery if it discovers that a 
volume referred to by a page in the journal no longer exists.

Recognizing that under some circumstances an administrator may indeed wish to remove a 
volume from an existing Database, this change provides a configurable switch to optionally 
allow pages from missing volumes to be skipped (with logged warning messages) during recovery 
processing.  The switch can be enabled by setting the configuration parameter 
com.persistit.Configuration#setIgnoreMissingVolumes to true.


Persistit 3.1.4 - Reduce KeyCoder Serialized Object Size
--------------------------------------------------------

.. note::
   Any Database containing objects serialized by a custom KeyCoder from a previous version 
   of Persistit is incompatible with this change

Minimize the per-instance overhead for application objects written into Persistit Keys by 
reducing the size of the internal identifier.

Persistit has rich support for serializing standard Java primitive and object types into a 
Key. Additionally, the KeyCoder class allows for any application level object to also be 
appended to a Key right next to any other type. This is tagged internally with per-class 
handles. This change lowers the initial offset to reduce and in many cases halve the serialized size. 

Persistit 3.1.4 - Maven POM Changes For Eclipse Juno 
----------------------------------------------------

The latest version of Eclipse, code named Juno, features a wide array of changes, including a 
new release of the m2eclipse plugin. In an effort to make getting started with Persistit as 
easy as possible, we have included the required m2e configuration sections in our pom.

Please contact Akiban if you have encounter any issues getting up and running with Persistit.   

Persistit 3.1.2 - Asserts Added to Check for Correct Exchange Thread Behavior
-----------------------------------------------------------------------------

A bug in the Akiban Server code caused an Exchange to be used concurrently by two Threads, 
causing serious and seemingly unrelated failures in Persistit including instances of IllegalMonitorException and IllegalStateException. To guard against future occurrences, asserts were added to catch such concurrent use by multiple threads.  Applications should be tested with asserts enabled to verify correct thread usage.



Known Issues
============

Transactional Tree Management
-----------------------------

All operations within Trees such as store, fetch, remove and traverse are correctly supported 
within transactions. However, the operations to create and delete Tree instances currently do 
not respect transaction boundaries. For example, if a transaction creates a new Tree, it is 
immediately visible within other Transactions and will continue to exist even if the original 
transaction aborts.  (However, records inserted or modified by the original transaction will 
not be visible until the transaction commits.) Prior to creating/removing trees, transaction 
processing should be quiesced and allowed to complete.


Out of Memory Error, Direct Memory Buffer
------------------------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/985117

Out of Memory Error, Direct Memory Buffer.  Can cause failed transactions under extreme load 
conditions as a result of threads getting backed up writing to the journal file. However, 
this error is transient and recoverable by by retrying the failed transaction.

* Workaround: Ensure your application has the ability to retry failed transactions

Tree#getChangeCount may return inaccurate result
-------------------------------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/986465

The getChangeCount method may return inaccurate results as its not currently transactional.  
The primary consumer is the PersistitMap. As a result of this bug Persistit may not generate 
java.util.ConcurrentModificationException when it is supposed to.

Multi-Version-Values sometimes not fully pruned
-------------------------------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/1000331

Multi-version values are not always pruned properly causing volume growth.  The number of 
MVV records and their overhead size can be obtaining by running the IntegrityCheck task. 

* Workaround 1: Run the IntegrityCheck task (CLI command icheck) with the -P option which will prune the MVVs. This will remove obsolete MVV instances and in many cases free up pages in which new data can be stored.  However, it will not reduce the actual size of the volume file.

* Workaround 2: To reduce the size of the volume you can use the CLI commands ``save`` and ``load`` to reload the data into a newly created volume file. See http://www.akiban.com/ak-docs/admin/persistit/Management.html#management for more information about these operations.

Note: although the methods described here may be helpful in reducing MVV clutter, Persistit Version 3.2.0 significantly improves the algorithms used to prune obsolete versions and the 
techniques described here are unlikely to be necessary.


