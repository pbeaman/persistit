************************************
Akiban Persistit
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

************************************
3.1.2
************************************

Release Date
============
June 15, 2012

Overview
========
This is a bug fix release of the Persistit project (https://launchpad.net/akiban-persistit).  

Fixed Issues
============

Infinite Loop When Repacking Buffer
-----------------------------------

https://bugs.launchpad.net/bugs/1005206

This was introduced late into the 3.1.1 development cycle. This could occur if a buffer required restructuring during pruning of a long value that was previously stored under a transaction. Upon the next save of this buffer to disk (e.g. shutdown), an infinite loop would occur.

Corruption Exceptions During Various Operations
-----------------------------------------------

https://bugs.launchpad.net/bugs/1010079

.. note::
   Only the message indicates a database corruption. The data volume is actually correct and intact.

This was introduced late into the 3.1.1 development cycle. This could occur if pruning a buffer containing a long record previously stored under a transaction required removal of keys and then that buffer was reused without further modification. A parallel structure associated with the every ``Buffer``, the ``FastIndex``, was not maintained during this operation.

Slow Accumulator Operations
---------------------------

https://bugs.launchpad.net/bugs/1012859

This bug preexisted, but was unknown to, the 3.1.1 release. If a thread starting a new transaction was interrupted during the call to ``begin()``, there was a chance for an internal object to wind up in an invalid state. This invalid state caused no visible consequences other than slower than expected ``Accumulator`` actions if this had occurred many times.

Known Issues
============
As described in the *3.1.1 Known Issues*.

|
|

************************************
3.1.1
************************************

Release Date
============
May 31, 2012

Overview
========
This is the first open source release of the Persistit project (https://launchpad.net/akiban-persistit).  

Known Issues
============

Transactional Tree Management
-----------------------------

All operations within Trees such as store, fetch, remove and traverse are correctly supported within transactions. However, the operations to create and delete Tree instances currently do not respect transaction boundaries. For example, if a transaction creates a new Tree, it is immediately visible within other Transactions and will continue to exist even if the original transaction aborts.  (However, records inserted or modified by the original transaction will not be visible until the transaction commits.) Prior to creating/removing trees, transaction processing should be quiesced and allowed to complete.

Problems with Disk Full
------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/916071

There are rare cases where Persistit will generate exceptions other than java.io.IOException: No space left on device when a disk volume containing the journal or volume file fills up. The database will be intact upon recovery, but the application may receive unexpected exceptions.

Out of Memory Error, Direct Memory Buffer
------------------------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/985117

Out of Memory Error, Direct Memory Buffer.  Can cause failed transactions under extreme load conditions as a result of threads getting backed up writing to the journal file. However, this error is transient and recoverable by by retrying the failed transaction.

* Workaround: Ensure your application has the ability to retry failed transactions

Tree#getChangeCount may return inaccurate result
-------------------------------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/986465

The getChangeCount method may return inaccurate results as its not currently transactional.  The primary consumer is the PersistitMap. As a result of this bug Persistit may not generate java.util.ConcurrentModiciationException when it is supposed to.

Multi-Version-Values sometimes not fully pruned
-------------------------------------------------------------

https://bugs.launchpad.net/akiban-persistit/+bug/1000331

Multi-version values are not always pruned properly causing volume growth.  The number of MVV records and their overhead size can be obtaining by running the IntegrityCheck task. 
* Workaround 1: Run the IntegrityCheck task (CLI command icheck) with the -P option which will prune the MVVs. This will remove obsolete MVV instances and in many cases free up pages in which new data can be stored.  However, it will not reduce the actual size of the volume file.

* Workaround 2: To reduce the size of the volume you can use the CLI commands save  and load to offload and then reload the data into a newly created volume file. See http://www.akiban.com/ak-docs/admin/persistit/Management.html#management for more information about these operations.

