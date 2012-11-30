.. _Basic-API:

Basic API
=========

Akiban Persistit stores data as key-value pairs in highly optimized B+Tree. (Actually, Akiban Persistit implements `B-Link Tree <http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf>`_ architecture for greater concurrency). Like a Java Map implementation, Persistit associates at most one value with each unique instance of a Key value.

Persistit provides classes and methods to access and modify keys and their associated values. Application code calls Persistit API methods to store, fetch, traverse and remove keys and records to and from the database.

Persistit permits efficient multi-threaded concurrent access to database volumes. It is designed to minimize contention for critical resources and to maximize throughput on multi-processor machines. Concurrent ACID transactions are supported with multi-value concurrency control (MVCC).

The Persistit Instance
----------------------

To access Persistit, the application first constructs an instance of the ``com.persistit.Persistit`` class and initializes it. This instance is the keystone of all subsequent operations.  It holds references to the buffers, maps, transaction contexts and other structures needed to access B+Trees. The life cycle of a Persistit instance should be managed as follows:

.. code-block:: java

  final Persistit db = new Persistit();
  //
  // register any Coder and Renderer instances before initialization
  //
  db.setConfiguration(config);
  db.initialize();
  try {
      // do application work
  } finally {
      db.close();
  }

where ``config`` is a ``com.persistit.Configuration`` that describes the memory allocation, initial set of volumes, journal file name, and other settings needed to get Persistit started. The configuration can also be specified as a ``java.util.Properties`` collection, or by name of a properties file. See :ref:`Configuration` for details.

The ``com.persistit.Persistit#close`` method gracefully flushes all modified data to disk, stops background threads and unregisters JMX MBeans. 

.. note:: 

  The Persistit background threads are not daemon threads, so if your application returns 
  from its static main method without calling ``close``, the JVM will not automatically exit.

Although normal shutdown should always invoke ``close``, Persistit is designed to recover a consistent database state in the event of an abrupt shutdown or crash. See :ref:`Recovery`.

.. _Key:

Key
---

The content of a ``com.persistit.Key`` is the unique identifier for a key/value pair within a tree. Internally a ``Key`` contains an array of bytes that constitute the physical identity of the key/value pair within a tree. Logically, a key consists of a sequence of zero or more Java values, each of which is called a *key segment*. 

The following value types are implicitly supported in keys::

  null
  boolean (and Boolean)
  byte (and Byte)
  short (and Short)
  char (and Character)
  int (and Integer)
  long (and Long)
  float (and Float)
  double (and Double)
  java.lang.String
  java.math.BigInteger
  java.math.BigDecimal
  java.util.Date
  byte[]

In addition, you may register custom implementations of the ``com.persistit.encoding.KeyCoder`` interface to support encoding of other object classes. By default, String values are encoded in UTF-8 format.

Appending and Decoding Key Segments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``Key`` class provides methods to encode and decode each of these types to and from a key segment. For each type listed above, there is an ``append`` method, a ``to`` method and a ``decode`` method. For example, for the long type, there are methods

.. code-block:: java

  public void append(long v)
  public void to(long v)
  public long decodeLong()

The ``to`` methods replaces the final key segment with a different value (unless the key is empty, in which case it works the same as ``append``).

For example:

.. code-block:: java

  key.clear();         	// clear any previous key segments
  key.append("Atlantic");  // append segment "Atlantic"
  key.to("Pacific");   	// replace "Atlantic" with "Pacific"
  key.reset();         	// reset index to beginning
  String s = key.decode(); // s contains "Pacific"

The Key class also provides methods to encode and decode Object values to and from a key. Strings, Dates, objects of the corresponding wrapper classes for the primitive types listed above, and objects supported by registered instances of ``com.persistit.encoding.KeyCoder`` are permitted. Primitive values are automatically boxed and unboxed as needed. The following code fragment demonstrates key manipulation with automatic conversion of primitive types and their wrappers.

.. code-block:: java

  key.clear();              	// clear any previous key segments
  key.append(new Integer(1234));
  key.append("Atlantic");
  key.append(1.23d);
  key.reset();              	// reset index to beginning for decoding
  int v = key.decodeInt();  	// v will be 1234
  String s = (String)key.decode(); // s will be "Atlantic"
  Double d = (Double)decode();    // d will be 1.23d as a Double

In this code segment, an object of type Integer is appended to the key’s value sequence, and then the same value is later decoded as a primitive int value. A String is appended and then decoded into a String. Finally, a primitive double value is appended and then decoded as an object of class Double.

The maximum size of a serialized ``Key`` is 2,047 bytes.

For further information, see ``com.persistit.Key``.


.. _Value:

Value
-----

A ``com.persistit.Value`` object holds a value. Unlike keys, Value objects have no restriction on the types of data they can represent, and they can hold much larger objects. In particular, a Value may contain null, any of the primitive types, or an object of any class.

The backing store of a ``Value`` is a byte array that is written to a B+Tree data page, or in the case of a long record, multiple pages. The ``com.persistit.Value#put`` method variants encode (serialize) a Java primitive or Object value into the backing store, and the ``com.persistit.Value#get`` method variants decode (deserialize) the value.

For example, in ``HelloWorld.java``, the line

.. code-block:: java

  dbex.getValue().put("World");

serializes the String “World”, and the expression

.. code-block:: java

  dbex.getValue().get()

decodes it. Persistit does not intrinsically cache decoded object values, nor does it track an object's state changes.  Each call to the ``get()`` method returns a new instance of the object. However, you can use a ``com.persistit.encoding.ObjectCache`` to cache object values. ``ObjectCache`` is designed specifically to cache objects fetched from Persistit.

Value Types
^^^^^^^^^^^

``Value`` provides optimized predefined representations for the following types::

  null
  all primitive types
  all arrays
  java.math.BigInteger
  java.math.BigDecimal
  java.lang.String
  java.util.Date

In general, Persistit uses one of four mechanisms to encode a Java value into a Value object:

- If the value is one of the predefined types listed above, Persistit uses its own internal serialization logic.
- If there is a registered ``com.persistit.encoding.ValueCoder`` for the object's class, Persistit delegates to it.
- If enabled, Persistit uses an accelerated serialization/deserialization mechanism to encode and decode objects.
- Otherwise, for classes that implement java.io.Serializable, Persistit attempts to perform default Java serialization and deserialization.

A Value may also be in the undefined state, which results from performing a fetch operation on a key for which no value is present in the database. The undefined state is distinct from the value ``null`` and can be tested with the ``isDefined()`` method.

See :ref:`Serialization` for additional information.

Large Values
^^^^^^^^^^^^

Persistit stores large values, in the current version up to 64MB in size. For example, it is possible to store an image’s backing bytes as a single value in the database. The size of the value to be stored is constrained by available heap memory; the entire value must be able to be serialized into an in-memory byte array in order for Persistit to store or retrieve it. Use ``com.persistit.Value#setMaximumSize`` to specify a the size constraint. Large values are broken up across multiple data pages and are not necessarily stored in contiguous file areas.

The definition of “large” depends on the configuration properties. for example, for a volume with a page size of 16K bytes the threshold occurs at 6,108 bytes. A value having a serialized size smaller than this is stored in a single data page while a larger value is broken up and stored in multiple pages. For a smaller pages size the threshold is lower.

On occasion it may be desirable to fetch only part of a large value. For example, it may be useful to extract summary information from the beginning of a the backing byte array for an Image. Variants versions of the ``fetch`` and ``traverse`` accept a minimum byte count parameter. When these methods are used only the specified minimum number bytes of the backing store are retrieved from the database. This technique can prevent Persistit from reading large numbers of pages from the disk in order to examine only a small portion of the record.

.. _Exchange:

Exchange
--------

The primary low-level interface for interacting with Persistit is ``com.persistit.Exchange``. The Exchange class provides all methods for storing, deleting, fetching and traversing key/value pairs. These methods are summarized here and described in detail in the Javadoc API documentation.

An Exchange instance contains references to a ``Key`` and a ``Value``. The methods ``com.persistit.Exchange#getKey()`` and ``com.persistit.Exchange#getValue()`` access these instances.

To construct an Exchange you specify a Volume (or alias) and a tree name in its constructor. The constructor will optionally create a new tree in that Volume if a tree having the specified name has not already been created. An application may construct an arbitrary number of Exchange objects. Creating a new Exchange has no effect on the database if the specified tree already exists. Tree creation is thread-safe: multiple threads concurrently constructing Exchanges using the same Tree name will safely result in the creation of only one new tree.

An Exchange is a moderately complex object that can consume tens of kilobytes to megabytes (depending on the sizes of the Key and Value) of heap space. Memory-constrained applications should construct Exchanges in moderatation.

Persistit offers Exchange pooling to avoid rapidly creating and destroying Exchange objects in multi-threaded applications.  An application may use the ``com.persistit.Persistit#getExchange`` and ``com.persistit.Persistit#releaseExchange`` methods to take and return an Exchange from and to a thread-local pool.

An Exchange internally maintains some optimization information such that references to nearby Keys within a tree are accelerated. Performance may benefit from using a different Exchange for each area of the Tree being accessed.

Concurrent Operations on Exchanges
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Although the underlying Persistit database is designed for highly concurrent multi-threaded operation, the ``Exchange`` class and its associated ``Key`` and ``Value`` instances are *not* thread-safe. Each thread should acquire and use its own Exchange object when accessing the database. Nonetheless, multiple threads can execute database operations on overlapping data concurrently using their thread-private ``Exchange`` instances.

Because Persistit permits concurrent operations by multiple threads, there is no guarantee that the underlying database will remain unchanged after an Exchange fetches or modifies its data. However, each operation on an Exchange is atomic, meaning that the inputs and outputs of each method are consistent with some valid state of the underlying Persistit backing store at some instant in time. The Exchange’s Value and Key objects represent that consistent state even if another thread subsequently modifies the database. Transactions, described below, allow multiple database operations to be performed atomically and consistently.

Exchange API
^^^^^^^^^^^^

An Exchange has permanent references to a ``com.persistit.Key`` and a ``com.persistit.Value``. Typically you work with an Exchange in one of the following patterns:

- Modify the Key, perform a ``fetch`` operation, and extract the Value.
- Modify the Key, modify the Value, and then perform a ``store`` operation.
- Modify the Key, and then perform a ``remove`` operation.
- Optionally modify the Key, perform a ``traverse`` operation, then read the resulting Key and/or Value.

These four methods, plus a few other methods listed here, are the primary low-level interface to the database. Semantics are as follows:

``fetch``
    Reads the stored value associated with this Exchange's Key and modifies the Exchange’s Value to reflect that value.
``store``
    Inserts or replaces the key/value pair for the specified key in the Tree either by replacing the former value, if there was one, or inserting a new value.
``fetchAndStore``
    Reads and then replaces the stored value. Upon completion, Value reflects the formerly stored value for the current Key. This operation is atomic.
``remove``, ``removeAll``, ``removeKeyRange``
    Removes key/value pairs from the Tree. Versions of this method specify either a single key or a range of keys to be removed.
``fetchAndRemove``
    Fetches and then removes the stored value. Upon completion, Value reflects the formerly stored value for the current Key. This operation is atomic.
``traverse``, ``next``, ``previous``
    Modifies the Exchange’s Key and Value to reflect a successor or predecessor key within the tree. See ``com.persistit.Key`` for detailed information on the order of traversal.
``hasNext``, ``hasPrevious``
    Indicates, without modifying the Exchange’s Value or Key objects, whether there is a successor or predecessor key in the Tree.
``hasChildren``
    Indicates whether there are records having keys that are logical children. A *logical child* of some key *P* is any key that can be constructed by appending one or more key segments to *P*.

For convenience, Exchange delegates ``append`` and ``to`` methods to ``com.persistit.Key``. For example, Exchange provides the following methods that delegate to the identically named methods of Key :

.. code-block:: java

  public Exchange append(long v)
  public Exchange append(String v)
  ...

To allow code call-chaining these methods of Exchange return the same Exchange. For example, it is valid to write code such as

.. code-block:: java

  exchange.clear().append(" Pacific").append("Ocean").append(123).fetch();

This example fetches the value associated with the concatenated key
``{“Pacific”, ”Ocean”, 123}``.

Exchange also delegates other key manipulation methods. (See ``com.persistit.Exchange`` for detailed API documentation.)

Traversing and Querying Collections of Data
-------------------------------------------

An Exchange provides a number of methods for traversing a collection of records in the Persistit database. These include variations of the ``com.persistit.Exchange#traverse``, ``com.persistit.Exchange#next`` and ``com.persistit.Exchange#previous``. For all of these methods, Persistit does two things: it modifies the Exchange's ``Key`` to reflect a new key that is before or after the current key, and it modifies the ``Value`` associated with the Exchange to reflect the database value associated with that key.

For example, this code from ``HelloWorld.java`` prints out the key and value of each record in a tree:

.. code-block:: java

       	dbex.getKey().to(Key.BEFORE);
       	while (dbex.next())
       	{
           	System.out.println(
               	dbex.getKey().indexTo(0).decode() + " " +
               	dbex.getValue().get());
       	}

In general, the traversal methods let you find a key in a tree related to the key you supply. In Persistit programs you frequently prime a key value by appending either ``Key#BEFORE`` or ``Key#AFTER``. A key containing either of these special values can never be stored in a tree; these are reserved to represent positions in key traversal order before the first valid key and after the last valid key, respectively. You then invoke next or previous, or any of the other traverse family variants, to enumerate keys within the tree.

You can specify whether traversal is *deep* or *shallow*.  Deep traversal traverses the logical children (see com.persistit.Key) of a key. Shallow traversal traverses only the logical siblings.

.. _KeyFilter:

Selecting key values with a KeyFilter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A ``com.persistit.KeyFilter`` defines a subset of all possible key values. For example, a KeyFilter can select keys with certain fixed segment values, sets of values or ranges of values.  Calling ``traverse``, ``next`` or ``previous`` with a KeyFilter efficiently traverses the subset of all keys in a Tree that match the filter.

You construct a KeyFilter either by adding selection terms to it, or by calling the ``com.persistit.KeyParser#parseKeyFilter`` method of the ``com.persistit.KeyParser`` class to construct one from a string representation.

Use of a KeyFilter is illustrated by the following code fragment:

.. code-block:: java

  Exchange ex = new Exchange("myVolume", "myTree", true);
  KeyFilter kf = new KeyFilter("{\"Beethoven\":\"Britten\"}");
  ex.append(Key.BEFORE);
  while (ex.next(kf)){
      System.out.println(ex.getKey().reset().decodeString());
  }

This simple example emits the string-valued keys within Tree “myTree” whose values fall alphabetically between “Beethoven” and “Britten”, inclusive.


You will find an example with a KeyFilter in the examples/FindFileDemo directory.

.. _PersistitMap:

PersistitMap
------------

In addition to low-level access methods on keys and values, Persistit provides ``com.persistit.PersistitMap``, which implements the ``java.util.SortedMap`` interface. PersistitMap uses the Persistit database as a backing store so that key/value pairs are persistent, potentially shared with all threads, and limited in number only by disk storage.

Keys and Values for PersistitMap must conform to the constraints described above under :ref:`Key` and :ref:`Value`.

The constructor for PersistitMap takes an Exchange as its sole parameter. All key/value pairs of the Map are stored within the tree identified by this Exchange. The Key supplied by the Exchange becomes the root of a logical tree. For example:

.. code-block:: java

  Exchange ex = new Exchange("myVolume", "myTree", true);
  ex.append("USA").append("MA");
  PersistitMap<String, String> map = new PersistitMap<String, String>(ex);
  map.put("Boston", "Hub");

places a key/value pair into Tree “myTree” with the concatenated key ``{"USA ","MA","Boston"}`` and a value ``"Hub"``.

Generally the expected behavior for an Iterator on a Map collection view is to throw a ``ConcurrentModificationException`` if the underlying collection changes. This is known as “fail-fast” behavior. PersistitMap implements this behavior by throwing a ``ConcurrentModificationException`` in the event the Tree containing the map changes after the Iterator is constructed.

However, sometimes it may be desirable to use PersistitMap and its collections view interfaces to iterate across changing data, especially for large databases. PersistitMap provides the method ``com.persistit.PersistitMap#setAllowConcurrentModification`` to control whether changes made by other threads are permitted. By default, concurrent modifications are not allowed.

.. note:: When ``PersistitMap`` is used within a transaction updates generated by other concurrent transactions are not visible and   
   therefore cannot cause a ConcurrentModificationException.  However, to avoid unpredictable results an Iterator created within the scope 
   of a transaction must be used only within that transaction.


Exceptions in PersistitMap
^^^^^^^^^^^^^^^^^^^^^^^^^^

Persistit operations throw a variety of exceptions that are subclasses of ``com.persistit.exception.PersistitException``. However, the methods of the SortedMap interface do not permit arbitrary checked exceptions to be thrown. Therefore, PersistitMap wraps any PersistitException generated by the underlying database methods within a ``com.persistit.PersistitMap.PersistitMapException``. This exception is unchecked and can therefore be thrown by methods of the Map interface. Applications using PersistitMap should catch and handle PersistitMap.PersistitMapException.

Applying a KeyFilter to a PersistitMap Iterator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can specify a ``com.persistit.KeyFilter`` for the Iterator returned by the ``keySet()``, ``entrySet()`` and ``values()`` methods of ``com.persistit.PersistitMap``.  The KeyFilter restricts the range of keys traversed by the Iterator. To set the KeyFilter, you must cast the Iterator to the inner class PersistitMap.ExchangeIterator, as shown here:

.. code-block:: java

	PersistitMap map = new PersistitMap(exchange);
	PersistitMap.ExchangeIterator iterator =
   	(PersistitMap.ExchangeIterator)map.entrySet().iterator();
	iterator.setFilterTerm(KeyFilter.rangeTerm("A", "M"));

In this example, the iterator will only access String-valued keys between “A” and “M”.


