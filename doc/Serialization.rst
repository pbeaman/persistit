.. _Serialization:

Serializing Object Values
=========================

Akiban Persistit uses one of several mechanisms to serialize a Java Object into a ``com.persistit.Value``.

* For the following classes, Persistit provides built-in optimized serialization logic that cannot be overridden:

  * ``java.lang.String``
  * ``java.util.Date``
  * ``java.math.BigInteger``
  * ``java.math.BigDecimal``
  * Wrapper classes for primitive values (``Boolean``, ``Byte``, ``Short``, etc.)
  * All arrays (however, the mechanisms described here apply to array elements).

* An application can register a custom ``com.persistit.encoding.ValueCoder`` to handle serialization of a particular class
* Default serialization using Persistit's built-in serialization mechanism described below, or
* Standard Java serialization as described in `Java Object Serialization Specification <http://docs.oracle.com/javase/7/docs/platform/serialization/spec/serialTOC.html>`_.

Persistit's default serialization method serializes objects into approximately 33% fewer bytes, and depending on the structure of objects being serialized, is about 40% faster than Java serialization.

Storing Objects in Persistit
----------------------------

To store an object value into a Persistit database, you put the object into the Value field of an Exchange, and then invoke the Exchange's store method as shown in this code fragment:

.. code-block:: java

    exchange.getValue().put(myObject);
    exchange.store();

Of course, Persistit cannot actually store a live object on disk.  Instead it creates and stores a byte array containing state information about the object. Subsequently you fetch an object from Persistit as follows:

.. code-block:: java

    exchange.fetch();
    MyClass myObject = (MyClass)exchange.getValue().get();

The resulting MyClass instance is a newly constructed object instance that is equivalent - subject to the accuracy of the serialization code - to the original object. This process is equivalent to the serialization and deserialization capabilities provided by java.io.ObjectOutputStream and java.io.ObjectInputStream.

Persistit makes use of helper classes called “coders” to marshal data between live objects and their stored byte-array representations. Value coders, which implement ``com.persistit.encoding.ValueCoder``, marshal data to and from Value objects; ``com.persistit.encoding.KeyCoder`` implementations do the same for instances of ``com.persistit.Key``. A ``ValueCoder`` provides capability somewhat like the custom serialization logic implemented through ``readObject``, ``writeObject``, ``readExternal`` and ``writeExternal``. However, a ``ValueCoder`` can provide this logic for any class without modifying the class itself, which may be important if the class is part of a closed library.

You may create and register a value coder for almost any class, including classes that are not marked Serializable. The exceptions are those listed which have built-in, non-overridable serialization logic.

DefaultValueCoder and SerialValueCoder
--------------------------------------

When required to serialize or deserialize class with no explicitly defined ``ValueCoder``, Persistit automatically creates and registers one of the following two default ``ValueCoder`` implementations:

``com.persistit.DefaultValueCoder``::  uses introspection to determine which fields to serialize, and reflection to access and update the fields
``com.persistit.encoding.SerialValueCoder``:: creates instances of ObjectInputStream and ObjectOutputStream to serialize and deserialize the object.

DefaultValueCoder uses a more compact storage format and is significantly faster than standard Java serialization; however, it imposes certain limitations and trade-offs described below. By default, Persistit will use a DefaultValueCoder. However, you can identify classes that should instead be serialized and deserialized by ``SerialValueCoder`` by specifying the ``serialOverride`` configuration property, which is described below.

DefaultValueCoder
-----------------

A DefaultValueCoder uses Java reflection to access and update the fields of an arbitrary object. The set of fields is defined by the Java Object Serialization Specification. By default, these include all non-static, non-transient fields of the current class and its Serializable superclasses.  A class may override this default set by specifying an array of ``java.io.ObjectStreamField`` objects in a private final static field named ``serialPersistentFields``, as described in the specification.

``DefaultValueCoder`` invokes the special methods ``readResolve``, ``writeReplace``, ``readObject`` and ``writeObject``,  (or for Externalizable classes,  ``writeExternal`` and ``readExternal``) to provide the compatible custom serialization support. To support the ``readObject``/``readExternal`` and ``writeObject``/``writeExternal`` methods, Persistit creates extended implementations of ``java.io.ObjectOutputStream`` and ``java.io.ObjectInputStream``. These use a custom serialization format optimized for writing to a Value's backing byte array. For example, they do not organize data into 1,024-byte blocks, and they factor meta data about classes into a separate class information database so that this information is not repeated in multiple records containing instances of the same class.

Currently, ``DefaultValueCoder`` does not support the following elements of the serialization API:

- the ``readObjectNoData`` custom serialization method
- the ``PutFields``/``GetFields`` API of ``ObjectOutputStream`` and ``ObjectInputStream``
- the ``readLine`` method of ``ObjectInputStream``.

Constructing Objects upon Deserialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When deserializing a value, ``DefaultValueCoder`` combines information about the original object's class and the stored field data to reconstruct an object equivalent to the original. To do so it must first construct a new instance of class and then decode and set its serialized fields.

For compatibility with standard Java serialization, ``DefaultValueCoder`` constructs new object instances of Serializable classes using the same logic as ``ObjectInputStream``, namely:

If the class is Externalizable, ``DefaultValueCoder`` invokes its public no-argument constructor. (The specification for Externalizable requires the class to have such a constructor.)

Otherwise, if the class is Serializable, ``DefaultValueCoder`` invokes the no-argument constructor of its nearest non-serializable superclass.

``DefaultValueCoder`` must use platform-specific logic when constructing instances of Serializable classes: specifically, it invokes the same internal, non-public method as ``ObjectInputStream``. We have verified correct behavior on a wide range of Java runtime environments, but because the implementation uses private methods within various JRE versions, it is possible (though unlikely) that a future JRE will not provide a comparable capability.

To avoid using platform-specific API calls, you can specify the configuration property::

    constructorOverride=true

When this property is ``true``, ``DefaultValueCoder`` requires each object being serialized or deserialized to have a no-argument constructor through which instances will be constructed during deserialization. Unless the class implements Externalizable, that constructor may be private, package-private, protected or public.

Extending DefaultValueCoder
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can register an extended ``DefaultValueCoder`` to provide custom behavior, including custom logic for constructing instances of a class, as shown here:

.. code-block:: java

	Persistit.getInstance().getCoderManager().registerValueCoder(MyClass.class, new DefaultValueCoder(MyClass.class) {
  public Object get(Value value, Class clazz, CoderContext context) throws ConversionException {

    // Construct the object being deserialized.
    Object instance = new MyClass(...custom arguments...);

    // See "registering objects while deserializing" below
    value.registerEncodedObject(instance);
    
    // Load the non-transient, non-static fields
    render(value, instance, clazz, context);
    
    return instance;
        	}
 });



Security Policy Requirements for DefaultValueCoder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

DefaultValueCoder performs security-sensitive operations: (a) it reads and writes data from and to private fields using reflection, and (b) it overrides the default implementations of java.io.ObjectInputStream and java.io.ObjectOutputStream. If a SecurityManager is installed then three permissions must be granted to enable the new mechanism::

  java.lang.RuntimePermission "accessDeclaredMembers";
  java.lang.reflect.ReflectPermission("suppressAccessChecks")
  java.io.SerializablePermission("enableSubclassImplementation")

See :ref:`Security` for an extended discussion on security policy issues for Persistit.

SerialValueCoder
----------------

``SerialValueCoder`` uses standard Java serialization to store and retrieve object values. Typically this results in slower performance and a more verbose storage format than ``DefaultValueCoder``, but there are a number of reasons why a particular application might require standard Java serialization, including:

- the security context into which the application will be deployed does not grant the permissions noted above that are required for ``DefaultValueCoder``,
- to avoid Persistit's use of private API calls to construct object instances during deserialization,
- a preference for the use of a standard format defined within the Java platform rather than Persistit's custom format,
- limitations documented above on the API elements available during custom deserialization within DefaultValueCoder, for example non-support of GetField and PutField.

Your application can specify ``SerialValueCoders`` for specific classes either by explicitly creating and registering them, or by naming them in the com.persistit.serialOverride property.

To explicitly register a ``SerialValueCoder`` for the class ``MyClass``, do this:

.. code-block:: java

	...
	Persistit.getInstance().getCoderManager().registerValueCoder(
    	MyClass.class,
    	new SerialValueCoder(MyClass.class));
	...


The Serial Override Configuration Property
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``com.persistit.serialOverride`` property specifies classes that are to be serialized by ``SerialValueCoder`` rather than ``DefaultValueCoder``. This property affects how Persistit assigns a value coder when none has previously been registered. It does not override or affect explicitly registered coders.

Names are separated by commas and may contain wild cards.

The following are valid patterns:

  ``java.io.File``
      Just the File class.
  ``java.io.*``
      All classes in the java.io package.
  ``java.awt.**``
      All classes in the java.awt package and its sub-packages
  ``java.util.*Map``
      All of the Map classes in the java.util.
  ``**``
      All classes in all packages

More precisely, ``serialOverride`` specifies a comma-delimited list of zero or more patterns, each of which is either a fully-qualified class name or pattern that has within it exactly one wild card. The wild card “\*” replaces any sequence of characters other than a period (“.”), while “\*\*” replaces any sequence of characters including periods.  For example::

  serialOverride=org.apache.**,com.mypkg.serialstuff.*,com.mypkg.MyClass

Like all configuration properties, you may specify this in the persistit.properties file or as a system property through a Java command-line argument in the form::

  -Dcom.persistit.serialOverride=...

Registering Objects in a Custom ``ValueCoder``
----------------------------------------------

In a custom ``ValueCoder`` implementation, the ``get`` method is responsible for constructing and populating an instance of an object. The following pattern should be used when implementing the get method:

.. code-block:: java

  public void get(Value value, Class clazz, CoderContext context) throws ConversionException {
    	// Construct the object being deserialized.
    	//
    	Object instance = ...constructor for the object...

    	// Associate a handle with the newly
    	// created instance.
    	//
    	value.registerEncodedObject(instance);

    	// Populate the object's internal state
    	//
    	... load the fields – for example, by calling render...

    	return instance;
  }

The purpose of the ``registerEncodedObject`` method is to record the association between the newly created object and an internal integer-valued handle that may be used subsequently in the serialization stream to refer to that object. This mechanism supports objects that may have fields that refer either indirectly or indirectly back to the same object – i.e., that participate in a cyclical reference graph.

As a concrete example, consider a Person class with a spouse field such that for married couple p and q,  p.spouse is q and q.spouse is p. When Persistit serializes p it also serializes q, but when it serializes q's spouse field, it records a reference handle associated with the already-serialized instance of p rather than writing a new copy of p in the serialization stream. Upon deserializing q, Persistit looks up the object for the recorded handle to correctly associate the already-deserialized p instance with q.

Whenever you implement a custom ``get()`` method in any ``ValueCoder``, you must notify the underlying Value object about the newly created object by calling registerEncodedObject before deserializing its fields so that any back-references made within serialized fields of that object can find the object correctly.

``Value.toString()`` and ``decodeDisplayable``
----------------------------------------------

In many cases it is not very useful simply to display the result of evaluating ``toString()`` on an object. The default toString method inherited from Object conveys just a class name and a memory handle. In addition, for remote operations of AdminUI, it may not even be feasible to construct a deserialized object for each record. Therefore, ``com.persistit.Value`` provides a specialized ``toString()`` method to render an arbitrary object value into a legible string. The AdminUI utility uses this facility to summarize the data contained in a Tree.

Persistit creates a String value loading the object's class, using the following algorithm:

- If the state represented by this Value is undefined, then return "undefined".
- If the state is null or a boolean, return "null" "false", or "true".
- If the value represents a primitive type, return the string representation of the value, prefixed by "(byte)", "(short)", "(char)", "(long)", or "(float)" for the corresponding types. Values of type int and double are presented without prefix to reduce clutter.
- If the value represents a String, return a modified form of the string enclosed in double quotes. For each character of the string, if it is a double quote replace it by "\"", otherwise if it is outside of the printable ASCII character set replace the character in the modified string by "\b", "\t", "\n", "\r" or "\uNNNN" such that the modified string would be a valid Java string constant.
- If the value represents a wrapper for a primitive value (i.e., a java.lang.Boolean, java.lang.Byte, etc.) return the string representation of the value prefixed by "(Boolean)", "(Byte)", "(Short)", "(Character)", "(Integer)", "(Long)", "(Float)" or "(Double)".  The package name java.lang is removed to reduce clutter.
- If the value represents a java.util.Date, return a formatted representation of the date using the format specified by Key.SDF. This is a readable format that displays the date with full precision, including milliseconds.
- If the value represents an array, return a list of comma-separated element values surrounded by square brackets.
- If the value represents one of the standard Collection implementations in the java.util package, then return a comma-separated list of values surrounded by square brackets.
- If the value represents one of the standard Map implementations in the java.util package, then return a comma-separated list of key/value pairs surrounded by square brackets. Each key/value pair is represented by a string in the form key->value.
- If the value represents an object of a class for which there is a registered com.persistit.encoding.ValueDisplayer, invoke the displayer's display method to format a displayable representation of the object.
- If the value represents an object that has been stored using the version default serialization mechanism described above, return the class name of the object followed by a comma-separated tuple, enclosed within curly brace characters, representing the value of each field of the object.
- If the value represents an object encoded through standard Java serialization, return the string "(Serialized-object)" followed by a sequence of hex digits representing the serialized bytes. Note that this process does not attempt to deserialize the object.
- If the value represents an object that has already been represented within the formatted result - for example, if a Collection contains two references to the same object - then instead of creating an additional string representing the second or subsequent instance, emit a back reference pointer in the form @NNN where NNN is the character offset within the displayable string where the first instance was found. (This does not apply to strings and the primitive wrapper classes.)

For example, consider a Person having for date of birth, first name, last name, salary and friends, an array of other Person objects. The result returned by toString() on a Value representing Mary Smith who has a friend John Smith, might appear as follows::

 (Person){(Date)19490826000000.000-0400,"Mary","Jones",(long)75000,[
	(Person){(Date)19550522000000.000-0400,"John","Smith",(long)68000,[@0]}]}

In this example, John Smith's friends array contains a back reference to Mary Jones in the form "@0" because Mary's displayable reference starts at the beginning of the string.


PersistitReference
------------------

In general, serializing an object that contains references to other objects requires all the referenced objects also to be serialized. For an object connected to a large reference graph, it may be impractical or even semantically incorrect to serialize the entire graph.

One way to control the serialization graph for such an object is to write a custom ValueCoder; the custom ValueCoder can store key values for looking up the referenced object, rather than the object itself.  The ValueCoderDemo.java program demonstrates how this can be done.

The ``com.persistit.ref.PersistitReference`` interface, and its abstract subclasses, provide an alternative mechanism for breaking up an object reference graph.  It requires no custom ValueCoder, but does impact the design of application classes.  In addition, you will need to write a concrete implementation of either com.persistit.ref.AbstractReference or com.persistit.ref.AbstractWeakReference based on the actual storage structure of your object graph.

ObjectCache
-----------

A ``com.persistit.Value`` object holds the serialized, encoded state of a primitive value of an object.  Each time you invoke the get method on a Value, Persistit generates a new copy of the object deserialized from this Value.  Persistit does not implicitly cache deserialized objects. However, the ``com.persistit.encoding.ObjectCache`` class provides a simple mechanism for applications that need to maintain an in-memory cache of of objects from Persistit. ``ObjectCache`` works somewhat like a specialized version of java.util.WeakHashMap.

``ObjectCache`` has ``put``, ``get`` and ``remove`` methods much like a normal Map implementation.  However, when storing an object value with the supplied ``com.persistit.Key``, ``ObjectCache`` constructs a new, immutable ``com.persistit.KeyState`` object to hold as an internal key. This is necessary because ``Key`` objects change value as they are used.

Each ``ObjectCache`` entry holds its object value as a ``SoftReference``, making it available for garbage collection when space is needed.
