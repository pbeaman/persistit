
Getting Started with Akiban Persistit
=====================================

Welcome!

We have worked hard to make Akiban Persistit™ exceptionally fast, reliable, simple and lightweight. We hope you will enjoy learning more about it and using it.

Akiban Persistit is a key/value data storage library written in Java™. Key features include:

- support for highly concurrent transaction processing with multi-version concurrency control
- optimized serialization and deserialization mechanism for Java primitives and objects
- multi-segment (compound) keys to enable a natural logical key hierarchy
- support for long records (megabytes)
- implementation of a persistent SortedMap
- extensive management capability including command-line and GUI tools

This chapter briefly and informally introduces and demonstrates various Persistit features through examples. Subsequent chapters and the Javadoc API documentation provides a detailed reference guide to the product.

Download and Install
--------------------

Download |zip_file_name| from the `Launchpad project <https://launchpad.net/akiban-persistit/+download>`_ or directly from `akiban.com <http://www.akiban.com/akiban-persistit>`_.

Unpack the distribution kit into a convenient directory using any unzip utility. For example, use |unpack_zip_cmd| to unpack the distribution kit into the current working directory.

Review the ``LICENSE.txt`` file located in the root of the installation directory. Persistit is licensed under the Eclipse Public License or a free-use community license, see our `licensing options <http://www.akiban.com/akiban-licensing-options>`_ for more details. By installing, copying or otherwise using the Software contained in the distribution kit, you agree to be bound by the terms of the license agreement. If you do not agree to these terms, remove and destroy all copies of the software in your possession immediately.

Working with Persistit
----------------------

Add the |jar_file_name|, is found in the root directory of the distribution kit, to your project's classpath. For example, copy it to ``jre/lib/ext`` in your Java Runtime Environment, or add it to your classpath environment variable. 

That's it. You are ready to work with Persistit.

Examples
^^^^^^^^

Review the ``examples`` directory. Here you will find functional examples of varying complexity.

  ``examples/HelloWorld``
      source code for the example illustrating this chapter
  ``examples/SimpleDemo``
      short, simple program similar to HelloWorld
  ``examples/SimpleBench``
      a small micro-benchmark measuring the speed of insert, traversal, random updates, etc.
  ``examples/SimpleTransaction``
      example demonstrating use of Persisit’s multi-version currency control (MVCC) transactions
  ``examples/FindFile``
      a somewhat larger example that uses Persistit as the backing store for file finder utility
  ``examples/PersistitMapDemo``
      demonstration of the PersistitMap interface
  ``examples/SpringFrameworkExample``
      configures and initializes a Persistit instance through Spring Framework

HelloWorld
----------

Before going further let's honor tradition with a small program that stores, fetches and displays the phrase “Hello World.” In this program we will create a record with the key “Hello” and the value “World”.

**HelloWorld.java**

.. code-block:: java

  import com.persistit.Exchange;
  import com.persistit.Key;
  import com.persistit.Persistit;

  public class HelloWorld {
      public static void main(String[] args) throws Exception {
          Persistit db = new Persistit();
          try {
              // Read configuration from persistit.properties, allocate
              // buffers, open Volume, and perform recovery processing
              // if necessary.
              //
              db.initialize();
              //
              // Create an Exchange, which is a thread-private facade for
              // accessing data in a Persistit Tree. This Exchange will
              // access a Tree called "greetings" in a Volume called
              // "hwdemo". It will create a new Tree by that name
              // if one does not already exist.
              //
              Exchange dbex = db.getExchange("hwdemo", "greetings", true);
              //
              // Set up the Value field of the Exchange.
              //
              dbex.getValue().put("World");
              //
              // Set up the Key field of the Exchange.
              //
              dbex.getKey().append("Hello");
              //
              // Ask Persistit to put this key/value pair into the Tree.
              // Until this point, the changes to the Exchange are local
              // to this thread.
              //
              dbex.store();
              //
              // Prepare to traverse all the keys in the Tree (of which there
              // is currently only one!) and for each key display its value.
              //
              dbex.getKey().to(Key.BEFORE);
              while (dbex.next()) {
                  System.out.println(dbex.getKey().indexTo(0).decode() + " "
                          + dbex.getValue().get());
              }
              db.releaseExchange(dbex);
          } finally {
              // Always close Persistit. If the application does not do
              // this, Persistit's background threads will keep the JVM from
              // terminating.
              //
              db.close();
          }
      }
  }

Concepts
--------

Although ``HelloWorld.java`` is not very useful, it demonstrates several of the basic building blocks of the Persistit API.

Initialization and Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before accessing any data, ``HelloWorld.java`` calls the ``com.persistit.Persistit#initialize()`` method. This sets up the memory configuration for buffers and the path names of Persistit volume and journal files. Alternative methods accept configuration information from a ``com.persistit.Configuration`` object, a ``java.util.Properties`` object, a specified properties file, or by default from the file named ``persistit.properties`` in the current working directory.

In this example, ``persistit.properties`` looks like this:: 

  datapath=.
  buffer.count.8192=32
  volume.1=${datapath}/hwdemo,create,pageSize:8192,initialPages:5,extensionPages:5,maximumPages:100000
  journalpath=${datapath}/hwdemo_journal

See :ref:`Configuration` for additional information about Persistit configuration properties.

Volumes and Trees
^^^^^^^^^^^^^^^^^

A configuration defines one or more volume files that will contain stored Persistit data. Usually you will specify the ``create`` flag, which allows Persistit to create a new volume if the file does not already exist. Creating a new file also establishes the initial size and growth parameters for that volume.

Each volume may contain an unlimited number of named trees. Each tree within a volume embodies a logically distinct B+Tree index structure. Think of a tree as simply a named key space within a volume.

``HelloWorld.java`` stores its key/value pair in a tree called “greetings” in a volume named “hwdemo”. This is specified by constructing an Exchange.

Exchanges
---------

The ``com.persistit.Exchange`` class is the primary facade for interacting with Persistit data. It is so-named because it allows an application to exchange information with the database. An Exchange provides methods for storing, deleting, fetching and traversing key/value pairs.

The method

.. code-block:: java

  Exchange dbex = db.getExchange("hwdemo", "greetings", true);

in ``HelloWorld.java`` finds a volume named "hwdemo" and attempts to find a tree in it named "greetings". If there is no such tree, ``getExchange`` creates it.

Methods ``com.persistit.Persistit#getExchange`` and ``com.persistit.Persistit#releaseExchange`` maintain a pool of reusable Exchange objects designed for use by multi-threaded applications such as web applications. If a suitable exchange already exists, ``getExchange`` returns it; otherwise it constructs a new one.

The Exchange looks up the volume name “hwdemo” by matching it against the volumes specified in the configuration. The match is based on the simple file name of the volume after removing its final dotted suffix.  For example, the volume name “hwdemo” matches the volume specification ``${datapath}/hwdemo.v00``.

Each Exchange is implicitly associated with a ``com.persistit.Key`` and a ``com.persistit.Value``. Typically you work with an Exchange in one of the following patterns:

- Modify the Key, modify the Value and then perform a ``com.persistit.Exchange#store`` operation.
- Modify the Key, perform a ``com.persistit.Exchange#fetch`` operation and then read the Value.
- Modify the Key and then perform a ``com.persistit.Exchange#remove`` operation.
- Optionally modify the Key, perform a ``com.persistit.Exchange#next``, ``com.persistit.Exchange#previous`` or ``com.persistit.Exchange#traverse`` operation, then read the resulting Key and/or Value.

These methods and their variants provide the foundation for using Persistit.

Records
^^^^^^^

In Persistit, a database record consists of a Key and a Value. The terms “record” and “key/value pair” are used interchangeably.

When you store a record, Persistit searches for a previously stored record having the same key. If there is such a record, Persistit replaces its value.  If there is no such record, Persistit inserts a new one.  Like a Java Map, Persistit stores at most one value per key, and every record in a Tree has a unique key value.

Keys
^^^^

A Key contains a unique identifier for key/value pair - or record - in a tree. The identifier consists of a sequence of one or more Java values encoded into an array of bytes stored in the volume file.

Key instances are mutable. Your application typically changes an Exchange's Key in preparation for fetching or retrieving data. In particular, you can append, remove or replace one or more values in a Key. Each value you append is called a *key segment*. You append multiple key segments to implement concatenated keys. See ``com.persistit.Key`` for additional information on constructing keys and the ordering of key traversal within a tree.

The ``HelloWorld.java`` example appends “Hello” to the Exchange’s Key object in this line:

.. code-block:: java

            dbex.getKey().append("Hello");

The result is a key with a single key segment.

Values
^^^^^^

A Value object represents the serialized state of a Java object or a primitive value. It is a staging area for data being transferred from or to the database by ``fetch``, ``traverse`` and ``store`` operations.

Value instances are mutable. The ``fetch`` and ``traverse`` operations modify the state of an Exchange's Value instance to represent the value associated with some Key. Your application executes methods to modify the state of the Value instance in preparation for storing new data values into the database.

Numerous methods allow you to serialize and deserialize primitives values and objects into and from a Value object. For example, in ``HelloWorld.java``, the statement

.. code-block:: java

            dbex.getValue().put("World");

serializes the string “World” into the backing byte array of the Exchange’s Value object and

.. code-block:: java

            	System.out.println(
                	dbex.getKey().indexTo(0).decode() + " " +
                	dbex.getValue().get());

deserializes and prints an object value from the Key and another object value from the Value. Value also has methods such as ``getInt``, ``getLong``, ``getByteArray`` to decode primitive and array values directly.

Storing and Fetching Data
^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, it is these two methods in ``HelloWorld.java`` that cause the Exchange object to share data with the B+Tree, making it persistent and potentially available to other threads:

.. code-block:: java

            dbex.store();
            ...
            while (dbex.next()) { ... }

Closing Persistit
^^^^^^^^^^^^^^^^^

Persistit creates one or more background threads that lazily write data to the Volume files and perform other maintenance activities. Be sure to invoke the ``com.persistit.Persistit#close`` method to allow these threads to finish their work and exit properly. The pattern illustrated in ``HelloWorld.java``, using a *try/finally* block to invoke ``close``, is strongly recommended.

The ``com.persistit.Persistit#close(boolean)`` method optionally flushes all data to disk from the buffer pool before shutting down. Specifying the ``false`` option will close Persistit more quickly will lose recent updates if they were not performed inside of transactions, or will potentially require a longer recovery process during the next startup to reapply committed transactions.

Additional Topics
-----------------

PersistitMap
^^^^^^^^^^^^
A particularly easy way to get started with Persistit is to use its built-in ``com.persistit.PersistitMap`` implementation. PersistitMap implements the ``java.util.SortedMap`` interface, so it can directly replace ``java.util.TreeMap`` or other kinds of Map in existing Java code.

See :ref:`PersistitMap`.

KeyFilters
^^^^^^^^^^

A ``com.persistit.KeyFilter`` can be supplied to restrict the results returned by the ``com.persistit.Exchange#traverse`` methods. You can specify discrete values or ranges for values of individual key segments and apply other simple predicates.

See :ref:`Basic-API`.   

Transactions
^^^^^^^^^^^^

Persistit provides ACID Transaction support with multi-version concurrency control (MVCC) and adjustable durability policy.

See :ref:`Transactions`.

Managing Persistit
^^^^^^^^^^^^^^^^^^

Persistit provides several mechanisms for managing Persistit operation within an application. These include

- JMX MXBeans
- The ``com.persistit.Management`` object which provides programmatic access to many management operations
- The ``com.persistit.CLI`` object which provides a command-line interface for various management operations
- The AdminUI tool which provides a graphical client interface for examining records and other resources
- Logging interface design for easy embedding in host applications

See :ref:`Management`.
