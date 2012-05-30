************************************
Akiban-Persistit 3.1.1 Release Notes
************************************

Release Date
============
May 25, 2012

Overview
========
This is the first open source release of the Persistit project (https://launchpad.net/akiban-persistit).  

See http://www.akiban.com/akiban-persistit for a summary of features and benefits, licensing information and how to get support.

Documentation
=============
Users Guide (http://www.akiban.com/ak-docs/admin/persistit)
JavaDoc  (http://www.akiban.com/ak-docs/admin/persistit/apidocs)

Building Akiban-Persistit
=========================
Use Maven (maven.apache.org/download.html) to build Persistit.

To build::

mvn install

Resulting jar files are in the *target* directory.

To build Javadoc::

mvn javadoc:javadoc

Resulting Javadoc HTML files are in *target/site/apidocs*.

Building and Running the Examples
---------------------------------

Small examples are found in the *examples* directory. Each has a short README file describing the example, and an Ant (http://ant.apache.org/bindownload.cgi) build script. After building the main akiban-persisit jar file using Maven, you may run::

ant run

in each of the examples subdirectories to build and run the example.

Known Issues
============

* All operations within Trees such as store, fetch, remove and traverse are correctly supported within transactions. However, the operations to create and delete Tree instances currently do not respect transaction boundaries. For example, if a transaction creates a new Tree, it is immediately visible within other Transactions and will continue to exist even if the original transaction aborts.  (However, records inserted or modified by the original transaction will not be visible until the transaction commits.) Prior to creating/removing trees, transaction processing should be quiesced and allowed to complete.

* Bug 916071 - There are rare cases where Persistit will generate exceptions other than java.io.IOException: No space left on device when a disk volume containing the journal or volume file fills up. The database will be intact upon recovery, but the application may receive unexpected exceptions.

* Bug 985117 - Out of Memory Error, Direct Memory Buffer.  Can cause failed transactions under extreme load conditions as a result of threads getting backed up writing to the journal file.  
Workaround: Ensure your application has the ability to retry failed transactions

* Bug 986465 - getChangeCount may return inaccurate results as its not currently transactional.  The primary consumer is the PersistitMap. As a result of this bug Persistit may not generate java.util.ConcurrentModiciationException when it is supposed to.
Bug 1000331 - Multi-version values are not always pruned properly causing volume growth.  The number of MVV records and their overhead size can be obtaining by running the IntegrityCheck task.
** Workaround 1:  Run the IntegrityCheck task (CLI command icheck) with the -P option which will prune the MVVs. This will remove obsolete MVV instances and in many cases free up pages in which new data can be stored.  However, it will not reduce the actual size of the volume file.
** Workaround 2:  To reduce the size of the volume you can use the CLI commands save  and load to offload and then reload the data into a newly created volume file.
See http://www.akiban.com/ak-docs/admin/persistit/Management.html#management for more information about these operations.

* For optimal performance, proper configuration of the Persistit buffer pool is required.  See section "Configuring the Buffer Pool" in the configuration document http://www.akiban.com/ak-docs/admin/persistit/Configuration.html

.. note:: Especially when used with multi-gigabyte heaps, the default Hotspot JVM server heuristics are be suboptimal for Persistit applications. Persistit is usually configured to allocate a large fraction of the heap to Buffer instances that are allocated at startup and held for the duration of the Persistit instance. For efficient operation, all of the Buffer instances must fit in the tenured (old) generation of the heap to avoid very significant garbage collector overhead.  Use either -XX:NewSize or -Xmn to adjust the relative sizes of the new and old generations.

