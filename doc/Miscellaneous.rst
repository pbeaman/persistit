.. _Miscellaneous:

Miscellaneous Topics
====================

Following are some short items you may find useful as you explore Akiban Persistit. Follow links to the API documentation for more details.

TreeBuilder
-----------

The class ``com.persistit.TreeBuilder`` improves performance of inserting large sets of data when the keys being inserted are non-sequential. TreeBuilder is effective if and only if the size of the data being loaded is significantly larger than the amount of memory available in the buffer pool and keys are inserted in essentially random order.

Histograms
----------

The method ``com.persistit.Exchange#computeHistogram`` class provides a way to sample and summarize a set of keys in a ``Tree``.  It works by traversing keys in index pages near the root of the tree.  Because only a small fraction of all the keys in the tree are represented in the index, this can result in relatively small sample set of keys relatively quickly. The result can be used to estimate the actual number of keys.

Temporary Volumes
-----------------

A Persistit temporary volume is a special kind of Volume that is deleted when Persistit is closed. The update mechanism for temporary volumes avoids writing to disk whenever possible, and its contents are not recoverable Persistit shuts down. Therefore in some cases database operations on temporary volumes are faster.

The primary use case for a temporary volume is an application that needs the unlimited size, but not the persistence of normal Persistit volumes.

See the ``com.persistit.Persistit#createTemporaryVolume`` method for additional details.

Logging
-------

By default Persistit emits log messages to a file called persistit.log  and also writes high level log messages to System.out.  You can change this behavior by plugging in a different logging implementation. In particular, Persistit provides pluggable adapters for various other logging implementations, including Log4J, SLF4J, and the Java logging API introduced in JDK 1.4.
An adapter must implement the interface ``com.persistit.logging.PersistitLogger``. For example, see source code for ``com.persistit.logging.Slf4jAdapter``.

Using one of these logging frameworks is simple. For example, the following code connects Persistit to an application-supplied SLF4J logger:

.. code-block:: java

  db.setPersistitLogger(new Slf4jAdapter(LOG))

where ``db`` is the Persistit instance and ``LOG`` is a Logger supplied by SLF4J. This method should be called before the ``initialize`` method.



