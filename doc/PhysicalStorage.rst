.. _PhysicalStorage:

Physical B+Tree Representation
==============================

This chapter describes the physical structures used to represent Akiban Persistit records on disk and in memory.

Files
-----

Following is a directory listing illustrating a working Persistit database::

  -rw-r--r--. 1 demo demo  24G Feb  8 13:18 akiban_data
  -rw-r--r--. 1 demo demo  48K Feb  8 13:19 akiban_system
  -rw-r--r--. 1 demo demo 954M Feb  8 13:18 akiban_journal.000000000225
  -rw-r--r--. 1 demo demo 954M Feb  8 13:19 akiban_journal.000000000226
  -rw-r--r--. 1 demo demo 954M Feb  8 13:19 akiban_journal.000000000227
  -rw-r--r--. 1 demo demo 662M Feb  8 13:19 akiban_journal.000000000228

This database contains two *volume* files, ``akiban_data`` and ``akiban_system`` and four files that constitute part of the *journal*. As explained below, Persistit records are usually stored in a combination of volume and journal files.

.. _Journal:

The Journal
-----------

The *journal* is a set of files containing variable length records. The journal is append-only. New records are written only at the end; existing records are never overwritten. The journal consists of a numbered series of files having a configurable maximum size. When a journal file becomes full Persistit closes it and begins a new file with the next counter value. The maximum size of a journal file is determined by a configuration property called its block size.  The default block size value is 1,000,000,000 bytes which works well with today’s standard server hardware.

Every record in the journal has a 64-bit integer *journal address*. The journal address denotes which file contains the record and the record’s offset within that file. Journal addresses start at zero in a new database instance and grow perpetually [#f1]_.

Persistit writes two major types of records to journal files.

- For each committed update transaction, Persistit writes a record containing sufficient information to replay the transaction during recovery. For example, when Persistit stores a key/value pair during a transaction, it writes a record to the journal containing the key and value.
- Persistit also writes all updated page images to the journal. Some of these are eventually copied to volume files, as described below. This write/copy mechanism is critical to Persistit’s crash-recovery mechanism (see :ref:`Recovery`).

As updates are applied, Persistit constantly appends new information- both transaction records and modified page images - to the end of the highest-numbered file. To prevent the aggregation of a large number of journal files Persistit also works to copy or remove information from older journal files so that they can be deleted. The background thread responsible for this activity is called the ``JOURNAL_COPIER`` thread. The JOURNAL_COPIER copies pages from the journal back into their home volume files, allowing old files to be deleted. Normally a Persistit system at rest gradually copies all update page images and perform checkpoints so that only one small journal file remains. Applications can accelerate that process by calling the ``com.persistit.Persistit#copyBackPages`` method.

The journal is critical to ensuring Persistit can recover structurally intact B+Trees and apply all committed transactions after a system failure. For this reason, unless the JOURNAL_COPIER is entirely caught up, any attempt to save the state of a Persistit database must include both the volume and journal files.

The journal also plays a critical role during concurrent backup. To back up a running Persistit database, the ``com.persistit.BackupTask`` does the following:

- Enables ``appendOnly`` mode to suspend the copying of updated page images.
- Copies the appropriate volume and journal files
- Disables ``appendOnly`` mode to allow JOURNAL_COPIER to continue.

For more details on the journal, checkpoints and transactions, see :ref:`Recovery`. For more information on concurrent backup and other management tasks, see :ref:`Management`.

Pages and Volumes
-----------------

Persistit ultimately stores its data in one or more Volume files. Persistit manages volume files internally in sections called pages. Every page within one volume has the same size. The page size is configurable and may be 1,024, 2,048, 4,096, 8,192, or 16,384 (recommended) bytes long. Once the page size for a volume has been established, it cannot be changed. See :ref:`Configuration` for details of how to assign the page size for a new volume.

Directory Tree
^^^^^^^^^^^^^^

Within a volume there can be an unlimited number of B+Trees. (B+Trees are also called simply “trees” in this document.) A tree consists of a set of pages including a *root page*, *index pages* and *data pages*. The root page can be data page if the tree is trivial and contains only small number of records. Usually the root page is an index page which contains references to other index pages which in turn may refer to data pages.

Persistit manages a potentially large number of trees by maintaining a tree of trees called ``_directory``.  The ``_directory`` tree contains the name, root page address, ``com.persistit.Accumulator`` data and ``com.persistit.TreeStatistics`` data for all the other trees in the volume. The tree name ``_directory`` is reserved and may not be used when creating an Exchange.

Data Pages
^^^^^^^^^^

A data page contains a representation of one or more variable-length key/value pairs. The number of key/value pairs depends on the page size, and the sizes of the serialized keys and values. The first key in each data page is stored in its entirety, while subsequent keys are stored with *prefix compression* to reduce storage footprint and accelerate searches. Therefore the storage sizes of the second and subsequent keys in a data page depend on how many of the leading bytes of their serialized form match their predecessors. (See :ref:`Key` and :ref:`Value` for information on how Persistit encodes logical Java values into the byte arrays stored in a data page.)

Index Pages
^^^^^^^^^^^

An index page has a structure similar to a data page except that instead of holding serialized value data, it instead contains page addresses of subordinate pages within the tree.

.. TODO - diagram of B+Tree, page layouts, etc

.. _Recovery:

Recovery
--------

Akiban Persistit is designed, implemented and tested to ensure that whether the application shuts down gracefully or crashes without cleanly closing the database, the database remains structurally intact and internally consistent after restart.

To do this, Persistit performs a process called *recovery* every time it starts up.  The recovery process is generally very fast after a normal shutdown. However, it can take a considerable amount of time after a crash because many committed transactions may need to be executed.

Recovery performs three major activities:

- Restores all B+Trees to an internally consistent state with a known timestamp.
- Replays all transaction that committed after that timestamp.
- Prunes multi-version values belonging to certain aborted transactions (see :ref:`Pruning`).

To accomplish this, Persistit writes all updates first to the journal. Persistit also periodically writes *checkpoint* records to the journal. During recovery, Persistit finds the last valid checkpoint written before shutdown or crash, restores B+Trees to state consistent with that checkpoint, and then replays transactions that committed after the checkpoint.

Recovery depends on the availability of the volume and journal files as they existed prior to abrupt termination. If these are modified or destroyed outside of Persistit, successful recovery is unlikely.

Timestamps and Checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^

Persistit maintains a universal counter called the *timestamp* counter. Every update operation assigns a new, larger timestamp, and every record in the journal includes the timestamp assigned to the operation writing the record. The timestamp counter is unrelated to clock time.  It is merely a counter.

A *checkpoint* is simply a timestamp for which a valid recovery is possible. Periodically Persistit chooses a timestamp to be a new checkpoint. Over time it then ensures that all pages updated before the checkpoint have been written to the journal, and then writes a checkpoint marker. By default checkpoints occur once every two minutes. Normal shutdown through ``com.persistit.Persistit#close`` writes a final checkpoint to the journal regardless of when the last checkpoint cycle occurred. That final checkpoint is what allows recovery after a normal shutdown to be very fast.

Upon start-up Persistit starts by finding the last valid checkpoint timestamp, and then recovers only those page images from the journal that were written prior to it. The result is that all B+Trees are internally consistent and contain all the updates that were issued and committed to disk before the checkpoint timestamp and none the occurred after the checkpoint timestamp.

Then Persistit locates and reapplies all transaction records in the journal for transactions that committed after the last valid checkpoint timestamp. These transactions are reapplied to the database, with the result that:

- The B+Tree index and data structures are intact. All store, fetch, remove and traverse operations will complete successfully.
- All committed transactions are present in the recovered database.  (See :ref:`Transactions` for durability determined by ``CommitPolicy``.)

.. note::

   Persistit provides the utility class ``com.persistit.IntegrityCheck`` to verify the integrity of individual Trees or entire Volumes.

For updates occurring outside of a transaction the resulting state is identical to some consistent, reasonably recent state prior to the termination. (“Reasonably recent” depends on the checkpoint interval, which by default is set to two minutes.)

Flush/Force/Checkpoint
^^^^^^^^^^^^^^^^^^^^^^

An application may require certainty at various points that all pending updates have been fully written to disk. The ``com.persistit.Persistit`` class provides three methods to ensure that updates have been written:

  ``com.persistit.Persistit#flush``
      causes Persistit to write all pending updates to the journal. Upon successful completion of flush any pages that needed writing prior to the call to flush are 
      guaranteed to have been written to the journal or their respective volume files.
  ``com.persistit.Persistit#force``
      forces the underlying operating system to write pending updates from the operating system’s write-behind cache to the actual disk. (This operation relies on 
      the underlying ``java.io.Filechannel#force(boolean)`` method.)
  ``com.persistit.Persistit#checkpoint``
      causes Persistit to allocate a new checkpoint timestamp and then wait for all updates that happened before that timestamp to be committed to disk.

However, typical applications, especially those using :ref:`Transactions`, do not need to invoke these methods. Once a Transaction is durable, so are all other transactions that occurred at timestamps earlier than the transaction’s commit timestamp and no other method calls are required.


The Buffer Pool
---------------

Persistit maintains a cache of page copies in memory called the *buffer pool*. The buffer pool is a critical resource in reducing disk I/O and providing good run-time performance. After performing a relatively expensive disk operation to read a copy of a page into the buffer pool, Persistit retains that copy to allow potentially many fetch and update operations to be performed against keys and values stored in that page.

Persistit optimizes update operations by writing updated database pages lazily, generally a few seconds to minutes after the update has been performed on the in-memory copy of the page cached in the buffer pool. By writing lazily, Persistit allows many update operations to be completed on each page before incurring a relatively expensive disk I/O operation to write the updated version of the page to the Volume.

In Persistit the buffer pool is a collection of buffers allocated from the heap for the duration of Persistit’s operation. The buffers are allocated by the ``com.persistit.Persistit#initialize`` method and are released when the application invokes close. Because buffers are allocated for the life of the Persistit instance, they impose no garbage collection overhead. (However, especially when using large buffer pool allocation in a JVM with a large heap, there are some special memory configuration issues to consider.  See :ref:`Configuration` for details.)

Persistit allocates buffers from the buffer pool in approximately  least-recently-used (LRU) order. Most applications exhibit behavior in which data, having been accessed once, is read or updated several more times before the application moves to a different area of the database (locality of reference). LRU is an allocation strategy the yields reasonably good overall throughput by maintaining pages that are likely to be used again in the buffer pool in preference to pages that have not been used for a relatively long time.

Generally, allocating more buffers in the buffer pool increases the likelihood that a page will be found in the pool rather than having to be reloaded from disk. Since disk I/O is relatively expensive, this means that enlarging the buffer pool is a good strategy for reducing disk I/O and thereby increasing throughput. Persistit is designed to manage extremely large buffer pools very efficiently, so if memory is available, it is generally a good strategy to maximum buffer pool size.

Tools
-----

The command-line interface (see :ref:`CLI`) includes tools you can use to examine pages in volumes and records in the journal. Two of these include the ``jview`` and ``pview`` tasks. The ``jview`` command displays journal records selected within an address range, by type, by page address, and using other selection criteria in a readable form.  The ``pview`` command displays the contents of pages selected by page address or key from a volume, or by journal address from the journal.

.. rubric:: Footnotes

.. [#f1] Even on a system executing 1 million transactions per second the address space is large enough to last for hundreds of years.
