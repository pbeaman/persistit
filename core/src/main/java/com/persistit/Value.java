/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import java.io.IOException;
import java.io.InputStream;
import java.io.NotActiveException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.HashMap;

import com.persistit.encoding.CoderContext;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.ValueCoder;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;
import com.persistit.exception.InvalidKeyException;
import com.persistit.exception.MalformedValueException;

/**
 * <p>
 * Encapsulates the serialized form of an <tt>Object</tt> or a primitive value.
 * To store data, the application modifies the <tt>Exchange</tt>'s
 * <tt>Value</tt> object and then invokes the {@link Exchange#store()} operation
 * to write the encoded value into the database. To fetch data, the application
 * modifies the <tt>Exchange</tt>'s <tt>Key</tt> object, invokes the
 * {@link Exchange#fetch()} operation and then reads the resulting state from
 * the <tt>Value</tt>.
 * </p>
 * <p>
 * A <tt>Value</tt>'s state is represented internally by an array of bytes.
 * Methods of this class encode primitive or object values into the byte array,
 * and decode bytes in the array into values equivalent to their original
 * values. For primitive-valued values, the decoded value is identical to the
 * original value. That is, after: <blockquote>
 * 
 * <pre>
 * int a = 123;
 * value.put(a);
 * int b = value.get();
 * </pre>
 * 
 * </blockquote> <tt>a == b</tt> is true. For object-valued items, the result
 * will be an object that, subject to the accuracy of serialization code, is
 * equal to the original object, but generally with different identity. That is,
 * after: <blockquote>
 * 
 * <pre>
 * Object a = new Fricostat();
 * value.put(a);
 * int b = value.get();
 * </pre>
 * 
 * </blockquote> usually <tt>a == b</tt> is false, but <tt>a.equals(b)</tt> is
 * true.
 * </p>
 * <p>
 * <tt>Value</tt> uses three strategies for these conversions:
 * <ul>
 * <li>For primitive types, their wrapper classes and certain other classes,
 * <tt>Value</tt> uses built-in logic to perform these conversions. Objects of
 * class <tt>java.lang.String</tt>, <tt>java.util.Date</tt>,
 * <tt>java.math.BigInteger</tt>, <tt>java.math.BigDecimal</tt> and all arrays
 * are encoded and decoded by built-in methods.</li>
 * <li>For an object of any other class, the encoding and decoding methods of
 * <tt>Value</tt> attempt to find an associated
 * {@link com.persistit.encoding.ValueCoder} to perform custom encoding and
 * decoding of the object.</li>
 * <li>If there is no <tt>ValueCoder</tt> then if the class implements
 * <tt>java.io.Serializable</tt> or <tt>java.io.Externalizable</tt>, encoding
 * and decoding is performed through serialization logic using extended
 * <tt>java.io.ObjectOutputStream</tt> and <tt>java.io.ObjectInputStream</tt>
 * classes implemented by <tt>Value</tt>.</li>
 * </ul>
 * Note that <tt>Value</tt> can only encode an object if it has a
 * <tt>ValueCoder</tt> or implements either <tt>java.io.Serializable</tt> or
 * <tt>java.io.Externalizable</tt>.
 * </p>
 * <p>
 * Persistit JSA 1.1 introduces a faster, more compact internal storage format
 * for serialization. Objects encoded in 1.1 without the assistance of a
 * registered <tt>ValueCoder</tt> are normally stored in this new format and
 * cannot be decoded by earlier versions of Persistit. However, the converse is
 * not true: objects serialized by earlier versions of Persistit can be
 * deserialized properly by 1.1. Thus you may upgrade an installed application
 * from version 1.0 to 1.1 without running any type of database conversion
 * process.
 * </p>
 * <p>
 * In certain cases it may be preferable to store values using the default Java
 * serialization format defined by the <a href=
 * "http://java.sun.com/j2se/1.4.2/docs/guide/serialization/spec/serialTOC.html"
 * > Java Object Serialization Specification</a>. You may set the
 * <tt>serialOverride</tt> configuration property to specify classes that are to
 * be serialized in standard form.
 * </p>
 * <p>
 * See <a href="../../../Object_Serialization_Notes.html"> Persistit JSA 1.1
 * Object Serialization</a> for more detailed information on these these
 * subjects.
 * </p>
 * <h3>Value as the key of a HashMap or WeakHashMap</h3>
 * <p>
 * It may be useful to build a WeakHashMap associating the serialized content of
 * a <tt>Value</tt> with an associated deserialized object to avoid object
 * deserialization overhead or to implement correct identity semantics. Since
 * <tt>Value</tt> is mutable it is a poor choice for use as a map key. Instead,
 * an immutable {@link ValueState} should be used to hold an immutable copy of
 * this state. <tt>Value</tt> and <tt>ValueState</tt> implement
 * <tt>hashCode</tt> and <tt>equals</tt> in a compatible fashion so that code
 * similar to the following works as expected: <blockquote>
 * 
 * <pre>
 *      ...
 *      Value value = &lt;some value&gt;;
 *      if (!map.contains(value))   // uses the transient current state
 *      {
 *          ValueState vs = new ValueState(value);
 *          map.put(vs, object);    // uses an immutable copy as the key
 *      }
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <a name="_displayableFormat" /> <h3>Displayable Format</h3>
 * <p>
 * The {@link #toString()} method of this class attempts to construct a
 * human-readable representation of the serialized value. The Tree display panel
 * of the AdminUI utility uses this capability to summarize the contents of
 * values stored in a tree. The string representation is constructed as follows:
 * <ol>
 * <li>If the state represented by this <tt>Value</tt> is undefined, then return
 * "undefined".</li>
 * <li>If the state is <tt>null</tt> or a <tt>boolean</tt>, return "null"
 * "false", or "true".</li>
 * <li>If the value represents a primitive type, return the string
 * representation of the value, prefixed by "(byte)", "(short)", "(char)",
 * "(long)", or "(float)" for the corresponding types. Values of type
 * <tt>int</tt> and <tt>double</tt> are presented without prefix to reduce
 * clutter.</li>
 * <li>If the value represents a String, return a modified form of the string
 * enclosed in double quotes. For each character of the string, if it is a
 * double quote replace it by "\"", otherwise if it is outside of the printable
 * ASCII character set replace the character in the modified string by "\b",
 * "\t", "\n", "\r" or "\u0000" such that the modified string would be a valid
 * Java string constant.</li>
 * <li>If the value represents a <tt>java.util.Date</tt>, return a formatted
 * representation of the date using the format specified by {@link Key#SDF}.
 * This is a readable format the displays the date with full precision,
 * including milliseconds.</li>
 * <li>If the value represents an array, return a list of comma-separated
 * element values surrounded by square brackets.</li>
 * <li>If the value represents one of the standard <tt>Collection</tt>
 * implementations in the <tt>java.util</tt> package, then return a
 * comma-separated list of values surrounded by square brackets.</li>
 * <li>If the value represents one of the standard <tt>Map</tt> implementations
 * in the <tt>java.util</tt> package, then return a comma-separated list of
 * key/value pairs surrounded by square brackets. Each key/value pair is
 * represented by a string in the form <i>key</i>-&gt;<i>value</i>.</li>
 * <li>If the value represents an object of a class for which there is a
 * registered {@link com.persistit.encoding.ValueDisplayer}, invoke the
 * displayer's {@link com.persistit.encoding.ValueDisplayer#display display}
 * method to format a displayable representation of the object.</li>
 * <li>If the value represents an object that has been stored using the version
 * 1.1 storage mechanism described object, return the class name of the object
 * followed by a comma-separated tuple, enclosed within curly brace characters,
 * representing the value of each field of the object.</li>
 * <li>If the value represents an object encoded through standard Java
 * serialization, return the string "(Serialized-object)" followed by a sequence
 * of hex digits representing the serialized bytes. Note that this process does
 * not attempt to deserialize the object, which might have unintended
 * consequences.</li>
 * <li>If the value represents an object that has already been represented
 * within the formatted result - for example, if a <tt>Collection</tt> contain
 * two references to the same object - then instead of creating an additional
 * string representing the second or subsequent instance, emit a back reference
 * pointer in the form @NNN where NNN is the character offset within the
 * displayable string where the first instance was found. (Note: does not apply
 * to strings and the primitive wrapper classes.)</li>
 * </ol>
 * <p>
 * For example, consider a Person class with fields for date of birth, first
 * name, last name, salary and friends, an array of other Person objects. The
 * result returned by {@link #toString} on a <tt>Value</tt> representing two
 * Person instances, each with just the other as a friend, might appear as
 * follows: (Note, space added for legibility.)
 * 
 * <pre>
 * <code>
 * (Person){(Date)19490826000000.000-0400,"Mary","Jones",(long)75000,[
 *    (Person){(Date)19550522000000.000-0400,"John","Smith",(long)68000,[@0]}]}
 * </code>
 * </pre>
 * 
 * In this example, John Smith's <tt>friends</tt> array contains a back
 * reference to Mary Jones in the form "@0" because Mary's displayable reference
 * starts at the beginning of the string.
 * </p>
 * <a name="_streamMode" /> <h3>Stream mode</h3>
 * <p>
 * A <tt>Value</tt> normally contains just one object or primitive value. In its
 * normal mode of operation, the <tt>put</tt> operation overwrites any
 * previously held state, and the <tt>get</tt> operation retrieves the one
 * object or primitive value represented by the current state of the
 * <tt>Value</tt>. A subsequent invocation of <tt>get</tt> returns the same
 * value.
 * </p>
 * <p>
 * However, at certain times it is useful to store multiple items (fields)
 * together in one <tt>Value</tt> object. To allow this, <tt>Value</tt>
 * implements an alternative mode of operation called <i>stream</i> mode in
 * which each <tt>put</tt> invocation appends a new field to the state rather
 * than replacing the previous state. Similarly, <tt>get</tt> operations
 * retrieve sequentially written fields rather than rereading the same field.
 * Stream allows {@link com.persistit.encoding.ValueCoder ValueCoder}
 * implementations to aggregate the multiple fields encapsulated within an
 * encoded value.
 * </p>
 * <a name="_lowLevelAPI" /> <h3>Low-Level API</h3>
 * <p>
 * The low-level API allows an application to bypass the encoding and decoding
 * operations described above and instead to operate directly on the byte array
 * stored in the database. This might be appropriate for an existing application
 * that has already implemented its own serialization mechanisms. Applications
 * should use these methods only if there is a compelling design requirement to
 * do so.
 * </p>
 * <p>
 * The low-level API methods are: <br>
 * <blockquote>
 * 
 * <pre>
 *      byte[] {@link #getEncodedBytes}
 *      int {@link #getEncodedSize}
 *      void {@link #setEncodedSize(int)}
 *      void {@link #putEncodedBytes(byte[], int, int)}
 *      void {@link #copyFromEncodedBytes(byte[], int, int, int)}
 *      boolean {@link #ensureFit(int)}
 * </blockquote>
 * </pre>
 * 
 * </p>
 * 
 * 
 * @version 1.1
 */
public final class Value {
    /**
     * A Value that is always EMPTY - i.e., for which <tt>isDefined()</tt> is
     * always false.
     */
    public final static Value EMPTY_VALUE = new Value(null, 0, 0);
    /**
     * Default initial size of the byte array that backs this <tt>Value</tt>.
     */
    public final static int INITIAL_SIZE = 256;
    /**
     * Default maximum size to which the backing buffer can grow. The default
     * value is 4Mb.
     */
    public final static int DEFAULT_MAXIMUM_SIZE = 1024 * 1024 * 4;

    /**
     * Absolute maximum size limit.
     */
    public final static int MAXIMUM_SIZE = 64 * 1024 * 1024;

    private final static int SIZE_GRANULARITY = 256;
    private final static int SIZE_GROWTH_DENOMINATOR = 8;

    private final static char TRUE_CHAR = 'T';
    private final static char FALSE_CHAR = 'F';
    //
    // Primitive values first. Codes allocated for .net types as well as
    // Java and mutually available types.
    //
    private final static int TYPE_NULL = 1;
    private final static int TYPE_BOOLEAN = 2;
    private final static int TYPE_BYTE = 3;
    // private final static int TYPE_UBYTE = 4;
    private final static int TYPE_SHORT = 5;
    // private final static int TYPE_USHORT = 6;
    private final static int TYPE_CHAR = 7;
    private final static int TYPE_INT = 8;
    // private final static int TYPE_UINT = 9;
    private final static int TYPE_LONG = 10;
    // private final static int TYPE_ULONG = 11;
    // private final static int TYPE_DECIMAL = 12;
    private final static int TYPE_FLOAT = 13;
    private final static int TYPE_DOUBLE = 14;
    //
    // Wrapper classes for primitive types.
    // Note: we need to encode these differently than primitive
    // types, even though we automatically convert ("autobox") because
    // we need to know the component type of an array. Byte[] is
    // different than byte[], and so we need to differentiate.
    //
    private final static int CLASS_BOOLEAN = 18;
    private final static int CLASS_BYTE = 19;
    // private final static int CLASS_UBYTE = 20;
    private final static int CLASS_SHORT = 21;
    // private final static int CLASS_USHORT = 22;
    private final static int CLASS_CHAR = 23;
    private final static int CLASS_INT = 24;
    // private final static int CLASS_UINT = 25;
    private final static int CLASS_LONG = 26;
    // private final static int CLASS_ULONG = 27;
    // private final static int CLASS_DECIMAL = 28;
    private final static int CLASS_FLOAT = 29;
    private final static int CLASS_DOUBLE = 30;
    //
    // Used when recording the component type of an array
    //
    private final static int CLASS_OBJECT = 31;
    //
    // Standard classes encoded with built-in encoding scheme.
    //
    private final static int CLASS_STRING = 32;
    private final static int CLASS_DATE = 33;
    private final static int CLASS_BIG_INTEGER = 34;
    private final static int CLASS_BIG_DECIMAL = 35;

    //
    // Indicates a key range to be removed. Used only in representing
    // pending remove operations in the Transaction tree.
    //
    private final static int CLASS_ANTIVALUE = 49;
    //
    // Indicates a reference to an object that was encoded earlier in this
    // Value. Followed by the identityHashCode and a unique handle for the
    // object.
    //
    private final static int CLASS_REREF = 50;
    //
    // Serialized type introducer. Followed by the Persistit handle for the
    // type (even though serialization also represents that type - we need to
    // be able to decode the class without deserializing the object).
    //
    private final static int CLASS_SERIALIZED = 61;
    //
    // Array class introducer. Followed by component type and
    // length.
    //
    private final static int CLASS_ARRAY = 62;
    //
    // Array of arrays. Is followed by the number of dimensions and then the
    // component type.
    //
    private final static int CLASS_MULTI_ARRAY = 63;
    //
    // The following introduce integer-valued class IDs, sizes and counts.
    // For each of these, the bottom four bits hold the most significant 4
    // bits of the integer value being represented.
    //
    // A CLASS handle is by the current ClassIndex. There is a
    // one-to-one mapping between handle values and ClassInfo objects.
    // Each ClassInfo identifies a Class.
    //
    // A SIZE is a count of bytes. A size is encoded as a prefix for a
    // variable-length inner item. The outermost item is never prefixed
    // the size is given by the raw byte count.
    //
    // A COUNT is a count of items within a list.
    //
    // CLASS1 / SIZE2 / COUNT2
    // is followed by no additional bytes, and therefore
    // can encode values between 0 and 15
    //
    // CLASS2 / SIZE2 / COUNT2
    // is followed by one byte, and therefore can encode
    // values between 0 and 2**12 - 1.
    //
    // CLASS3 / SIZE4 /COUNT3
    // is followed by two bytes, and therefore can encode
    // values between 0 and 2**20 - 1.
    //
    // CLASS5 / SIZE5 / COUNT5
    // is followed by a 4-byte integer. The low bytes of
    // the introducer byte are ignored. This representation
    // can encode values up to Integer.MAX_VALUE. (With
    // 5 bits left available if needed for longer values.
    //
    // Note that a LONGREC is introduced by 0xFF (255) as the first byte. This
    // scheme will not collide because CLASS5 always has zeros in its low 4
    // bits, meaning that the highest byte in a standard class encoding will
    // be 0xF0 (240).

    private final static int BASE1 = 0x00;
    private final static int BASE2 = 0x10;
    private final static int BASE3 = 0x20;
    private final static int BASE5 = 0x30;

    private final static int CLASS1 = 0x40;
    // private final static int CLASS2 = 0x50;
    // private final static int CLASS3 = 0x60;
    private final static int CLASS5 = 0x70;
    //
    private final static int COUNT1 = 0x80;
    // private final static int COUNT2 = 0x90;
    // private final static int COUNT3 = 0xA0;
    private final static int COUNT5 = 0xB0;
    //
    private final static int SIZE1 = 0xC0;
    // private final static int SIZE2 = 0xD0;
    // private final static int SIZE3 = 0xE0;
    private final static int SIZE5 = 0xF0;

    private final static int[] ENCODED_SIZE_BITS = { -1, 0x00, 0x10, 0x20, -1,
            0x30 };

    private final static Class[] CLASSES = {
            null, // 0
            Void.TYPE, Boolean.TYPE,
            Byte.TYPE,
            null, // reserved for .net unsigned byte
            Short.TYPE,
            null, // reserved for .net unsigned short
            Character.TYPE,
            Integer.TYPE,
            null, // reserved for .net unsigned int
            Long.TYPE,
            null, // reserved for .net unsigned long
            null, // reserved for .net decimal
            Float.TYPE, Double.TYPE,
            null,

            null, // 16
            Void.class, Boolean.class,
            Byte.class,
            null, // reserved for .net unsigned byte
            Short.class,
            null, // reserved for .net unsigned short
            Character.class,
            Integer.class,
            null, // reserved for .net unsigned int
            Long.class,
            null, // reserved for .net unsigned long
            null, // reserved for .net decimal
            Float.class, Double.class,
            Object.class,

            String.class, // 32
            Date.class, BigInteger.class, BigDecimal.class, null, null, null,
            null, null, null, null, null, null, null, null, null,

            null, // 48
            null,
            Object.class, // 50 Reference to previously encoded Object,
            null, null, null, null, null, null, null, null, null,
            Serializable.class, // 60
            Serializable.class, // 61

    };

    //
    // A non-negative element of this array denotes the fixed number of bytes
    // required to represent the corresponding array element.
    // Element value -1 means that the corresponding item is variable-length.
    //
    private final static int[] FIXED_ENCODING_SIZES = { 0, // 0
            0, // null
            1, // boolean
            1, // byte
            1, // unsigned byte
            2, // short
            2, // unsigned short
            2, // char
            4, // int
            4, // unsigned int
            8, // long
            8, // unsigned long
            16, // decimal
            4, // float
            8, // double
            -1,

            0, // 16
            0, // null
            1, // boolean
            1, // byte
            1, // unsigned byte
            2, // short
            2, // unsigned short
            2, // char
            4, // int
            4, // unsigned int
            8, // long
            8, // unsigned long
            16, // decimal
            4, // float
            8, // double
            -1,

            -1, // 32 String
            8, // Date
            -1, // BigInteger
            -1 // BigDecimal

    };

    private final static int TOO_MANY_LEVELS_THRESHOLD = 100;
    private final static HashMap _arrayTypeCache = new HashMap();
    private final static int SAB_INCREMENT = 1024;

    private int _maximumSize = DEFAULT_MAXIMUM_SIZE;

    private int _size = 0;
    private int _end = 0;
    private int _next = 0;
    private int _depth = 0;
    private int[] _endArray;
    private int _level;

    private byte[] _bytes;
    private byte[] _longBytes;
    private int _longSize;
    private boolean _longMode;

    private long _pointer = -1;
    private int _pointerPageType = -1;
    private boolean _atomicIncrementArmed = false;
    private long _atomicIncrementValue;

    private ValueObjectInputStream _vis;
    private ValueObjectOutputStream _vos;

    private int _serializedItemCount;
    private WeakReference _valueCacheWeakRef;
    private ValueCache _valueCache;

    private boolean _shared = true;
    private DefaultValueCoder _currentCoder;
    private Object _currentObject;

    private final Persistit _persistit;

    private WeakReference _stringAssemblyBufferWeakRef;

    /**
     * Construct a <tt>Value</tt> object with default initial and maximum
     * encoded sizes.
     */
    public Value(final Persistit persistit) {
        this(persistit, INITIAL_SIZE, DEFAULT_MAXIMUM_SIZE);
    }

    /**
     * Construct a </tt>Value</tt> object with specified initial encoded size
     * and default maximum size.
     * 
     * @param initialSize
     *            Initial size of the encoded value buffer.
     */
    public Value(final Persistit persistit, final int initialSize) {
        this(persistit, initialSize, DEFAULT_MAXIMUM_SIZE);
    }

    /**
     * Construct a <tt>Value</tt> object with specific initial encoded size and
     * specified maximum size.
     * 
     * @param initialSize
     *            Initial size of the encoded value buffer.
     * @param maximumSize
     *            Maximum size of the encoded value buffer.
     */
    public Value(final Persistit persistit, final int initialSize,
            final int maximumSize) {
        _persistit = persistit;
        _bytes = new byte[initialSize];
        _maximumSize = maximumSize;
    }

    /**
     * Construct a new <tt>Value</tt> that represents the same data as the
     * source.
     * 
     * @param source
     *            A <tt>Value</tt> whose state should be copied as the initial
     *            state of this <tt>Value</tt>.
     */
    public Value(Value source) {
        this(source._persistit, source._bytes.length, source._maximumSize);
        source.copyTo(this);
    }

    /**
     * Remove all content from this <tt>Value</tt>. This method also disables
     * stream mode.
     * 
     */
    public void clear() {
        _size = 0;
        reset();
    }

    void clear(boolean secure) {
        if (secure) {
            Util.clearBytes(_bytes, 0, _bytes.length);
            if (_longBytes != null) {
                Util.clearBytes(_longBytes, 0, _longBytes.length);
            }
            _longSize = 0;

            char[] sab = null;
            if (_stringAssemblyBufferWeakRef != null) {
                sab = (char[]) _stringAssemblyBufferWeakRef.get();
                if (sab != null) {
                    int length = sab.length;
                    for (int index = 0; index < length; index++) {
                        sab[index] = (char) 0;
                    }
                }
            }
        }
        clear();
    }

    private char[] getStringAssemblyBuffer(int size) {
        char[] sab = null;
        if (_stringAssemblyBufferWeakRef != null) {
            sab = (char[]) _stringAssemblyBufferWeakRef.get();
        }
        if (sab == null || sab.length < size) {
            sab = new char[size + SAB_INCREMENT];
            _stringAssemblyBufferWeakRef = new WeakReference(sab);
        }
        return sab;
    }

    /**
     * Copy the state of this <tt>Value</tt> to another <tt>Value</tt>.
     * 
     * @param target
     *            The <tt>Value</tt> to which state should be copied.
     */
    public void copyTo(Value target) {
        if (target == this)
            return;
        target.changeLongRecordMode(isLongRecordMode());
        target.ensureFit(_size);
        System.arraycopy(_bytes, 0, target._bytes, 0, _size);
        target._size = _size;
        target._maximumSize = _maximumSize;
        target._pointer = _pointer;
        target._longMode = _longMode;
        target.reset();
    }

    /**
     * Hash code for the current state of this <tt>Value</tt>. Note that if the
     * underlying state changes, <tt>hashCode</tt> will produce a different
     * result. Construct a {@link ValueState} instance to hold an immutable copy
     * of the current state of a <tt>Value</tt>.
     */
    public int hashCode() {
        int hashCode = 0;
        for (int index = 0; index < _size; index++) {
            hashCode = (hashCode * 17) ^ (_bytes[index] & 0xFF);
        }
        return hashCode & 0x7FFFFFFF;
    }

    /**
     * Implements the <tt>equals</tt> method such that <tt>Value</tt> and
     * {@link ValueState} objects may be used interchangeably as map keys.
     */
    public boolean equals(Object obj) {
        if (obj instanceof Value) {
            Value value = (Value) obj;
            if (value._size != _size)
                return false;
            for (int i = 0; i < _size; i++) {
                if (value._bytes[i] != _bytes[i])
                    return false;
            }
            return true;
        } else if (obj instanceof ValueState) {
            return ((ValueState) obj).equals(this);
        } else
            return false;
    }

    /**
     * Reduce the backing byte buffer to the minimal length needed to represent
     * the current state.
     * 
     * @return <tt>true</tt> if the size was actually reduced.
     */
    public boolean trim() {
        return trim(0);
    }

    /**
     * Reduce the backing byte buffer to the greater of the minimal length
     * needed to represent the current state and the specified lower bound.
     * 
     * @param newSize
     *            the minimum size of the backing buffer.
     * @return <tt>true</tt> if the size was actually reduced.
     */
    public boolean trim(int newSize) {
        if (_bytes.length > _size && _bytes.length > newSize) {
            byte[] bytes = new byte[Math.max(_size, newSize)];
            System.arraycopy(_bytes, 0, bytes, 0, _size);
            _bytes = bytes;
            return true;
        } else {
            return false;
        }
    }

    /**
     * <p>
     * Ensures that the specified number of bytes can be appended to the backing
     * byte array. If the available space is too small, this method replaces the
     * array with a new one having at least <tt>length</tt> available bytes.
     * Applications using the low-level API must call {@link #getEncodedBytes}
     * to get a reference to the new array after this replacement has occurred.
     * </p>
     * <p>
     * This method is part of the <a href="#_lowLevelAPI">Low-Level API</a>.
     * </p>
     * 
     * @param length
     * @return <tt>true</tt> if the backing byte array was replaced by a larger
     *         array.
     */
    public boolean ensureFit(int length) {
        if (length > 0 && length * SIZE_GROWTH_DENOMINATOR < _size) {
            length = _size / SIZE_GROWTH_DENOMINATOR;
        }
        int newSize = _size + length;
        if (_size + length <= _bytes.length)
            return false;
        newSize = ((newSize + SIZE_GRANULARITY - 1) / SIZE_GRANULARITY)
                * SIZE_GRANULARITY;
        if (newSize > _maximumSize)
            newSize = _maximumSize;
        if (newSize < length + _size) {
            throw new ConversionException("Requested size=" + length
                    + " exceeds maximum size=" + _maximumSize);
        }
        byte[] bytes = new byte[newSize];
        System.arraycopy(_bytes, 0, bytes, 0, _size);
        _bytes = bytes;

        return true;
    }

    /**
     * Copy a subarray from the encoded byte array to a target. This method is
     * part of the <a href="#_lowLevelAPI">Low-Level API</a>. This method
     * 
     * @param dest
     *            The target byte array
     * @param from
     *            Offset from which to start the copy
     * @param to
     *            Offset into the target at which the subarray should be copied
     * @param length
     *            Number of bytes to copy
     * @throws ArrayIndexOutOfBoundsException
     */
    public void copyFromEncodedBytes(byte[] dest, int from, int to, int length) {
        Util.arraycopy(_bytes, from, dest, to, length);
    }

    /**
     * Returns the number of bytes used to encode the current value. This method
     * is part of the <a href="#_lowLevelAPI">Low-Level API</a>.
     * 
     * @return The size
     */
    public int getEncodedSize() {
        return _size;
    }

    /**
     * Replace the encoded value with bytes from a supplied array. This method
     * is part of the <a href="#_lowLevelAPI">Low-Level API</a>.
     * 
     * @param from
     *            Byte array from which to copy the encoded value
     * @param offset
     *            Offset to first byte in the supplied array from which to copy
     * @param length
     *            Number of bytes to copy
     * @throws ArrayIndexOutOfBoundsException
     *             if the supplied offset or size exceed the bounds of the
     *             supplied array
     * @throws ConversionException
     *             if the resulting value size exceeds the maximum size
     */
    public void putEncodedBytes(byte[] from, int offset, int length) {
        ensureFit(length);
        if (length > 0) {
            Util.arraycopy(from, offset, _bytes, 0, length);
        }
        setEncodedSize(length);
    }

    /**
     * Returns the backing byte array used to hold the state of this
     * <tt>Value</tt>. This method is part of the <a
     * href="#_lowLevelAPI">Low-Level API</a>.
     * 
     * @return The byte array
     */
    public byte[] getEncodedBytes() {
        return _bytes;
    }

    /**
     * Sets the length of the encoded data in the backing byte array. This
     * length governs the number of bytes from the backing byte array that will
     * be stored in the database during the next <tt>store</tt> operation. This
     * method is part of the <a href="#_lowLevelAPI">Low-Level API</a>.
     */
    public void setEncodedSize(int size) {
        if (size < 0 || size > _bytes.length) {
            throw new IllegalArgumentException("Size " + size
                    + " exceeds capacity");
        }
        _size = size;
        _depth = 0;
    }

    /**
     * Returns the maximum size to which the backing buffer can grow.
     * 
     * @return The maximum size
     */
    public int getMaximumSize() {
        return _maximumSize;
    }

    /**
     * Modifies the maximum size to which the backing buffer can grow and trims
     * the current backing buffer to be no larger than the new maximum.
     * 
     * @param size
     *            The maximum size
     * 
     * @throws IllegalArgumentException
     *             If the backing buffer is already larger than <tt>size</tt>,
     *             this method
     * 
     */
    public void setMaximumSize(int size) {
        if (size < _size) {
            throw new IllegalArgumentException(
                    "Value is larger than new maximum size");
        }
        if (size > MAXIMUM_SIZE) {
            throw new IllegalArgumentException(
                    "Value is larger than absolute limit " + MAXIMUM_SIZE);
        }
        trim(size);
        _maximumSize = size;
    }

    /**
     * Enables or disables stream mode. See <a href="#_streamMode">Stream
     * Mode</a> for further information.
     * 
     * @param b
     *            <tt>true</tt> to enable stream mode, <tt>false</tt> to disable
     *            it.
     */
    public void setStreamMode(boolean b) {
        reset();
        _depth = b ? 1 : 0;
    }

    /**
     * Indicates whether stream mode is enabled. See <a
     * href="#_streamMode">Stream Mode</a> for further information.
     * 
     * @return <tt>true</tt> if stream mode is enabled.
     */
    public boolean isStreamMode() {
        return _depth > 0;
    }

    /**
     * Indicates whether there is data associated with this <tt>Value</tt>. The
     * result of fetching a <tt>Key</tt> that has no associated record in the
     * database leaves the corresponding <tt>Value</tt> in an undefined state.
     * Note that a Value containing <tt>null</tt> is defined. Persistit
     * distinguishes between null and undefined states.
     * 
     * @return <tt>true</tt> if there is data represented by this <tt>Value</tt>
     *         .
     */
    public boolean isDefined() {
        return _size != 0;
    }

    /**
     * Tests whether the data held by this <tt>Value</tt> is null.
     * 
     * @return <tt>true</tt> if the current state of this <tt>Value</tt>
     *         represents <i>null</i>.
     */
    public boolean isNull() {
        return getTypeHandle() == TYPE_NULL;
    }

    boolean isAntiValue() {
        return getTypeHandle() == CLASS_ANTIVALUE;
    }

    /**
     * Provides a String representation of the state of this <tt>Value</tt>.
     * 
     * @see #decodeDisplayable(boolean, StringBuilder)
     * 
     * @return A String value. If this Value is undefined, returns the word
     *         "undefined". Note that this value is indistinguishable from the
     *         result of <tt>toString</tt> on a <tt>Value</tt> whose state
     *         represents the string "undefined". Invoke the
     *         {@link #isDefined()} method to determine reliably whether the
     *         <tt>Value</tt> is defined.
     */
    public String toString() {
        if (_size == 0) {
            return "undefined";
        }
        
        if (_longMode && (_bytes[0] & 0xFF) == Buffer.LONGREC_TYPE
                && (_size >= Buffer.LONGREC_SIZE)) {
            return toStringLongMode();
        }

        int saveDepth = _depth;
        int saveLevel = _level;
        int saveNext = _next;
        int saveEnd = _end;
        boolean saveAtomic = _atomicIncrementArmed;
        StringBuilder sb = new StringBuilder();
        setStreamMode(true);
        try {
            boolean first = true;
            while (_next < _size) {
                if (!first)
                    sb.append(",");
                first = false;
                decodeDisplayable(true, sb, null);
            }
        } catch (ConversionException e) {
            int truncatedSize = Math.min(_size - _next, 256);
            sb.append("ConversionException " + e.getCause() + " index=" + _next
                    + " size=" + (_size - _next) + ": "
                    + Util.hexDump(_bytes, 0, truncatedSize));
        } catch (Exception e) {
            sb.append("Exception " + e + " while decoding value at index="
                    + _next + ": " + e);
        } finally {
            _atomicIncrementArmed = saveAtomic;
            _end = saveEnd;
            _next = saveNext;
            _level = saveLevel;
            _depth = saveDepth;
        }
        return sb.toString();
    }

    private String toStringLongMode() {
        final StringBuilder sb = new StringBuilder();
        sb.append("LongRec size=");
        sb.append(Util.getLong(_bytes, Buffer.LONGREC_SIZE_OFFSET));
        sb.append(" page=");
        sb.append(Util.getLong(_bytes, Buffer.LONGREC_PAGE_OFFSET));
        return sb.toString();
    }

    /**
     * Appends a displayable, printable String version of a value into the
     * supplied StringBuilder. If <tt>quoted</tt> is <tt>true</tt>, then the all
     * String values in the result will be enclosed and converted to a printable
     * format.
     * 
     * @see #decodeDisplayable(boolean, StringBuilder, CoderContext)
     * 
     * @param quoted
     *            <tt>true</tt> to quote and convert all strings to printable
     *            form.
     * @param sb
     *            A <tt>StringBuilder</tt> to which the displayable value will
     *            be appended.
     */
    public void decodeDisplayable(boolean quoted, StringBuilder sb) {
        decodeDisplayable(quoted, sb, null);
    }

    /**
     * Appends a displayable String version of a value into the supplied
     * StringBuilder. If <tt>quoted</tt> is <tt>true</tt>, then the all String
     * values in the result will be enclosed and converted to a printable
     * format.
     * 
     * @param quoted
     *            <tt>true</tt> to quote and convert all strings to printable
     *            form.
     * @param sb
     *            A <tt>StringBuilder</tt> to which the displayable value will
     *            be appended.
     * @param context
     *            A <tt>CoderContext</tt> to be passed to any underlying
     *            {@link ValueDisplayer}.
     */
    public void decodeDisplayable(boolean quoted, StringBuilder sb,
            CoderContext context) {
        checkSize(1);

        int start = _next;
        int level = _level;

        int classHandle = nextType();
        int currentItemCount = _serializedItemCount;
        boolean isVariableLength = (_next - start) > 1;
        switch (classHandle) {
        case TYPE_NULL: {
            _next = start;
            sb.append(getNull());
            break;
        }

        case TYPE_BYTE: {
            _next = start;
            appendParenthesizedFriendlyClassName(sb, byte.class);
            sb.append(getByte());
            break;
        }

        case TYPE_CHAR: {
            _next = start;
            appendParenthesizedFriendlyClassName(sb, char.class);
            if (quoted)
                Util.appendQuotedChar(sb, getChar());
            else
                sb.append((int) getChar());
            break;
        }

        case TYPE_INT: {
            _next = start;
            sb.append(getInt());
            break;
        }

        case TYPE_LONG: {
            _next = start;
            appendParenthesizedFriendlyClassName(sb, long.class);
            sb.append(getLong());
            break;
        }

        case TYPE_FLOAT: {
            _next = start;
            appendParenthesizedFriendlyClassName(sb, float.class);
            sb.append(getFloat());
            break;
        }

        case TYPE_DOUBLE: {
            _next = start;
            sb.append(getDouble());
            break;
        }

        case TYPE_BOOLEAN: {
            _next = start;
            sb.append(getBoolean());
            break;
        }

        case CLASS_STRING:
        case CLASS_BOOLEAN:
        case CLASS_BYTE:
        case CLASS_SHORT:
        case CLASS_CHAR:
        case CLASS_INT:
        case CLASS_LONG:
        case CLASS_FLOAT:
        case CLASS_DOUBLE: {
            // For these built-in types we don't display the back
            // reference.
            _next = start;
            if (_level != level)
                _end = popEnd();
            Object value = get(null, context);
            appendDisplayable(sb, value, quoted, false);
            break;
        }

        case CLASS_REREF: {
            _next = start;
            Object value = get(null, context);
            appendDisplayable(sb, value, quoted, true);
            break;
        }

        case CLASS_ARRAY: {
            try {
                _depth++;
                _serializedItemCount++;
                registerEncodedObject(sb.length());
                int componentClassHandle = nextType();
                switch (componentClassHandle) {
                case TYPE_BOOLEAN: {
                    sb.append("boolean[]{");
                    int length = _end - _next;
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        sb.append(toBoolean(_next) ? "true" : "false");
                        _next++;
                    }
                    break;
                }

                case TYPE_BYTE: {
                    sb.append("byte[]{");
                    int length = _end - _next;
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        sb.append(Util.getByte(_bytes, _next));
                        _next++;
                    }
                    break;
                }

                case TYPE_SHORT: {
                    sb.append("short[]{");
                    int length = arraySize(_end, _next, 2);
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        sb.append(Util.getShort(_bytes, _next));
                        _next += 2;
                    }
                    break;
                }

                case TYPE_CHAR: {
                    sb.append("char[]{");
                    int length = arraySize(_end, _next, 2);
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        int c = Util.getChar(_bytes, _next);
                        if (quoted)
                            Util.appendQuotedChar(sb, c);
                        else
                            sb.append((int) c);
                        _next += 2;
                    }
                    break;
                }

                case TYPE_INT: {
                    sb.append("int[]{");
                    int length = arraySize(_end, _next, 4);
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        sb.append(Util.getInt(_bytes, _next));
                        _next += 4;
                    }
                    break;
                }

                case TYPE_LONG: {
                    sb.append("long[]{");
                    int length = arraySize(_end, _next, 8);
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        sb.append(Util.getLong(_bytes, _next));
                        _next += 8;
                    }
                    break;
                }

                case TYPE_FLOAT: {
                    sb.append("float[]{");
                    int length = arraySize(_end, _next, 4);
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        float f = Float.intBitsToFloat(Util.getInt(_bytes,
                                _next));
                        sb.append(f);
                        _next += 4;
                    }
                    break;
                }

                case TYPE_DOUBLE: {
                    sb.append("double[]{");
                    int length = arraySize(_end, _next, 8);
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        double d = Double.longBitsToDouble(Util.getLong(_bytes,
                                _next));
                        sb.append(d);
                        _next += 8;
                    }
                    break;
                }

                default: {
                    Class cl = classForHandle(componentClassHandle);
                    if (cl != null)
                        appendFriendlyClassName(sb, cl);
                    sb.append("[]{");
                    int length = decodeElementCount();
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(",");
                        decodeDisplayable(quoted, sb, context);
                    }
                    break;
                }
                }
                sb.append('}');
            } finally {
                _depth--;
            }
            if (isVariableLength)
                closeVariableLengthItem();
            break;
        }
        case CLASS_MULTI_ARRAY: {
            _next = start;
            decodeDisplayableMultiArray(quoted, sb, context, null);
            break;
        }
        case CLASS_SERIALIZED: {
            _next = start;
            int length = sb.length();
            _depth++;
            try {
                Object object = get(null, context);
                getValueCache().store(currentItemCount,
                        new DisplayMarker(sb.length()));
                appendDisplayable(sb, object, quoted, false);
            } catch (Exception e) {
                sb.setLength(length);
                sb.append("(Serialized-Object)");
                Util.bytesToHex(sb, _bytes, start, _end - start);
            } finally {
                _depth--;
                if (isVariableLength)
                    closeVariableLengthItem();
            }
            break;
        }

        default: {
            if (classHandle >= CLASS1) {
                try {
                    Class clazz = _persistit.classForHandle(classHandle);
                    ValueCoder coder = null;
                    _depth++;
                    getValueCache().store(currentItemCount,
                            new DisplayMarker(sb.length()));

                    _serializedItemCount++;

                    if (clazz != null) {
                        coder = getValueCoder(clazz);
                    }
                    if (coder instanceof ValueDisplayer) {
                        appendParenthesizedFriendlyClassName(sb, clazz);
                        ((ValueDisplayer) coder).display(this, sb, clazz,
                                context);
                    } else if (coder instanceof SerialValueCoder) {
                        int length = sb.length();
                        try {
                            _next = start;
                            Object object = get(null, context);
                            getValueCache().store(currentItemCount,
                                    new DisplayMarker(sb.length()));
                            appendDisplayable(sb, object, quoted, false);
                        } catch (Exception e) {
                            sb.setLength(length);
                            sb.append("(Serialized-Object)");
                            Util.bytesToHex(sb, _bytes, start, _end - start);
                        }
                    } else {
                        appendParenthesizedFriendlyClassName(sb, clazz);
                        sb.append("{");
                        boolean first = true;
                        while (hasMoreItems()) {
                            if (!first)
                                sb.append(",");
                            first = false;
                            decodeDisplayable(true, sb, null);
                        }
                        sb.append("}");
                    }
                    break;
                } catch (Throwable t) {
                    sb.append("<<" + t + ">>");
                } finally {
                    _depth--;
                    if (isVariableLength)
                        closeVariableLengthItem();
                }
            } else {
                try {
                    _next = start;
                    Object value = get(null, context);
                    getValueCache().store(currentItemCount,
                            new DisplayMarker(sb.length()));
                    appendDisplayable(sb, value, quoted, false);
                } catch (Throwable t) {
                    sb.append("<<" + t + ">>");
                } finally {
                    if (isVariableLength)
                        closeVariableLengthItem();
                }
                break;
            }
        }
        }
    }

    private void decodeDisplayableMultiArray(boolean quoted, StringBuilder sb,
            CoderContext context, Class prototype) {
        int start = _next;
        int type = nextType(CLASS_MULTI_ARRAY, CLASS_REREF);
        if (type == CLASS_REREF) {
            _next = start;
            Object array = get(null, null);
            if (array == null || array instanceof DisplayMarker
                    || array.getClass().isArray()) {
                appendDisplayable(sb, array, quoted, true);
            } else {
                throw new ConversionException(
                        "Referenced object is not an array");
            }
        } else {
            try {
                _depth++;
                int componentClassHandle = nextType();
                checkSize(1);
                int dimensions = _bytes[_next++] & 0xFF;
                if (prototype == null) {
                    prototype = Array.newInstance(
                            classForHandle(componentClassHandle),
                            new int[dimensions]).getClass();
                }
                int length = decodeElementCount();

                _serializedItemCount++;
                registerEncodedObject(sb.length());
                sb.append('[');
                Class componentType = prototype.getComponentType();
                if (componentType.getComponentType().isArray()) {
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(',');
                        decodeDisplayableMultiArray(quoted, sb, context,
                                componentType);
                    }
                } else {
                    for (int index = 0; index < length; index++) {
                        if (index > 0)
                            sb.append(',');
                        decodeDisplayable(quoted, sb, context);
                    }
                }
                sb.append(']');
            } finally {
                _depth--;
            }
            closeVariableLengthItem();
        }
    }

    private void appendParenthesizedFriendlyClassName(StringBuilder sb, Class cl) {
        sb.append('(');
        appendFriendlyClassName(sb, cl);
        sb.append(')');
    }

    private void appendFriendlyClassName(StringBuilder sb, Class cl) {
        if (cl == null) {
            sb.append("null");
            return;
        }
        if (cl.isPrimitive()) {
            sb.append(cl.getName());
        } else if (cl.isArray()) {
            appendFriendlyClassName(sb, cl.getComponentType());
            sb.append("[]");
        } else if (cl == String.class) {
            sb.append("String");
        } else if (cl == Date.class) {
            sb.append("Date");
        } else if (Number.class.isAssignableFrom(cl)
                && cl.getName().startsWith("java.lang.")
                || cl.getName().startsWith("java.math.")) {
            sb.append(cl.getName().substring(10));
        } else {
            sb.append(cl.getName());
        }
    }

    private void appendDisplayable(StringBuilder sb, Object value,
            boolean quoted, boolean reference) {
        if (value == null) {
            sb.append("null");
        } else {
            Class cl = value.getClass();
            String className = cl.getName();

            if (cl == String.class) {
                String s = (String) value;
                int length = s.length();
                if (length > 24 && reference)
                    length = 21;
                if (quoted) {
                    sb.append("\"");
                    for (int index = 0; index < s.length(); index++) {
                        Util.appendQuotedChar(sb, s.charAt(index));
                    }
                    sb.append("\"");
                } else {
                    sb.append(s.substring(0, length));
                }
                if (length < s.length())
                    sb.append("...");
            } else if (cl == Date.class) {
                appendParenthesizedFriendlyClassName(sb, cl);
                sb.append(Key.SDF.format((Date) value));
            } else if (value instanceof Number) {
                sb.append('(');
                sb.append(className.startsWith("java.lang.") ? className
                        .substring(10) : className);
                sb.append(')');
                sb.append(value);
            } else if (value instanceof DisplayMarker) {
                sb.append(value);
            } else {
                appendParenthesizedFriendlyClassName(sb, cl);
                try {
                    String s = value.toString();
                    appendDisplayable(sb, s, false, reference);
                } catch (Throwable t) {
                    sb.append("<<" + t + ">>");
                }
                // sb.append(value);
            }
        }
    }

    int getTypeHandle() {
        int saveDepth = _depth;
        int saveLevel = _level;
        int saveNext = _next;
        int saveEnd = _end;
        int result = nextType();
        _end = saveEnd;
        _next = saveNext;
        _level = saveLevel;
        _depth = saveDepth;
        return result;
    }

    /**
     * Returns the type of the object represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The type
     */
    public Class getType() {
        int saveDepth = _depth;
        int saveLevel = _level;
        int saveNext = _next;
        int saveEnd = _end;
        try {
            int classHandle = nextType();
            if (classHandle > 0 && classHandle < CLASSES.length
                    && CLASSES[classHandle] != null) {
                return CLASSES[classHandle];
            } else if (classHandle == CLASS_ARRAY) {
                _depth++;
                int componentClassHandle = nextType();
                return arrayClass(classForHandle(componentClassHandle), 1);
            } else if (classHandle == CLASS_MULTI_ARRAY) {
                _depth++;
                int componentClassHandle = nextType();
                checkSize(1);
                int dimensions = _bytes[_next++] & 0xFF;
                return arrayClass(classForHandle(componentClassHandle),
                        dimensions);
            }

            else
                return classForHandle(classHandle);
        } finally {
            _end = saveEnd;
            _next = saveNext;
            _level = saveLevel;
            _depth = saveDepth;
        }
    }

    private Class arrayClass(Class componentClass, int dimensions) {
        Class[] arraysByDimension = (Class[]) _arrayTypeCache
                .get(componentClass);
        Class result = null;
        if (arraysByDimension != null && arraysByDimension.length > dimensions)
            result = arraysByDimension[dimensions];
        if (result != null)
            return result;

        if (dimensions == 1)
            result = Array.newInstance(componentClass, 0).getClass();

        else
            result = Array.newInstance(componentClass, new int[dimensions])
                    .getClass();
        if (arraysByDimension != null) {
            if (arraysByDimension.length <= dimensions) {
                Class[] temp = new Class[dimensions + 2];
                System.arraycopy(arraysByDimension, 0, temp, 0,
                        arraysByDimension.length);
                arraysByDimension = temp;
                _arrayTypeCache.put(componentClass, arraysByDimension);
            }
        } else
            arraysByDimension = new Class[dimensions + 2];

        arraysByDimension[dimensions] = result;
        return result;
    }

    /**
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt> and verifies that it is <tt>null</tt>.
     * 
     * @return <tt>null</tt>
     * @throws ConverisonException
     *             if this <tt>Value</tt> does not currently represent
     *             <tt>null</tt>.
     */
    public Object getNull() {
        int start = _next;
        int type = nextType();
        if (type == TYPE_NULL) {
            _serializedItemCount++;
            return null;
        }
        _next = start;
        Object object = get(null, null);
        if (object == null)
            return null;
        throw new ConversionException("Expected null");
    }

    /**
     * Decodes the boolean value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a boolean.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public boolean getBoolean() {
        int start = _next;
        if (nextType() == TYPE_BOOLEAN) {
            _serializedItemCount++;
            return getBooleanInternal();
        }
        _next = start;
        return ((Boolean) getExpectedType(Boolean.class)).booleanValue();
    }

    private boolean getBooleanInternal() {
        checkSize(1);
        boolean result = toBoolean(_next);
        _next++;
        return result;
    }

    /**
     * Decodes the byte value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a byte.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public byte getByte() {
        int start = _next;
        if (nextType() == TYPE_BYTE) {
            _serializedItemCount++;
            return getByteInternal();
        }
        _next = start;
        return ((Byte) getExpectedType(Byte.class)).byteValue();
    }

    private byte getByteInternal() {
        checkSize(1);
        byte result = _bytes[_next++];
        return result;
    }

    /**
     * Decodes the short value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a short.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public short getShort() {
        int start = _next;
        if (nextType() == TYPE_SHORT) {
            _serializedItemCount++;
            return getShortInternal();
        }
        _next = start;
        return ((Short) getExpectedType(Short.class)).shortValue();
    }

    private short getShortInternal() {
        checkSize(2);
        short result = (short) Util.getShort(_bytes, _next);
        _next += 2;
        return result;
    }

    /**
     * Decodes the char value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a char.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public char getChar() {
        int start = _next;
        if (nextType() == TYPE_CHAR) {
            _serializedItemCount++;
            return getCharInternal();
        }
        _next = start;
        return ((Character) getExpectedType(Character.class)).charValue();
    }

    private char getCharInternal() {
        checkSize(2);
        char result = (char) Util.getChar(_bytes, _next);
        _next += 2;
        return result;
    }

    /**
     * Decodes the int value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a int.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public int getInt() {
        int start = _next;
        if (nextType() == TYPE_INT) {
            _serializedItemCount++;
            return getIntInternal();
        }
        _next = start;
        return ((Integer) getExpectedType(Integer.class)).intValue();
    }

    private int getIntInternal() {
        checkSize(4);
        int result = Util.getInt(_bytes, _next);
        _next += 4;
        return result;
    }

    /**
     * Decodes the long value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a long.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public long getLong() {
        int start = _next;
        if (nextType() == TYPE_LONG) {
            _serializedItemCount++;
            return getLongInternal();
        }
        _next = start;
        return ((Long) getExpectedType(Long.class)).longValue();
    }

    private long getLongInternal() {
        checkSize(8);
        long result = Util.getLong(_bytes, _next);
        _next += 8;
        return result;
    }

    /**
     * Decodes the float value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a float.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public float getFloat() {
        int start = _next;
        if (nextType() == TYPE_FLOAT) {
            _serializedItemCount++;
            return getFloatInternal();
        }
        _next = start;
        return ((Float) getExpectedType(Float.class)).floatValue();
    }

    private float getFloatInternal() {
        checkSize(4);
        float result = Float.intBitsToFloat(Util.getInt(_bytes, _next));
        _next += 4;
        return result;
    }

    /**
     * Decodes the double value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as a double.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of
     *             this type.
     */
    public double getDouble() {
        int start = _next;
        if (nextType() == TYPE_DOUBLE) {
            _serializedItemCount++;
            return getDoubleInternal();
        }
        _next = start;
        return ((Double) getExpectedType(Double.class)).doubleValue();
    }

    private double getDoubleInternal() {
        checkSize(8);
        double result = Double.longBitsToDouble(Util.getLong(_bytes, _next));
        _next += 8;
        return result;
    }

    /**
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt>. This method is identical to {@link #get()} except that in
     * <a href="#_streamMode">Stream Mode</a> the pointer to the next retrieved
     * value is not advanced.
     * 
     * @return The value as a Object.
     * 
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of a
     *             recognizable class.
     * 
     * @throws MalformedValueException
     *             if this <tt>Value</tt> is structurally corrupt.
     */
    public Object peek() {
        return peek(null, null);
    }

    /**
     * <p>
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt>. This method is identical to {@link #get(Object)} except
     * that in <a href="#_streamMode">Stream Mode</a> the pointer to the next
     * retrieved value is not advanced.
     * </p>
     * <p>
     * This variant of <tt>get</tt> <i>may</i> modify and return the target
     * object supplied as a parameter, rather than creating a new object. This
     * behavior will occur only if the encoded value has a registered
     * {@link ValueRenderer}. See the documentation for <tt>ValueRenderer</tt>
     * for more information.
     * </p>
     * 
     * @param target
     *            A mutable object into which a {@link ValueRenderer} <i>may</i>
     *            decode this <tt>Value</tt>.
     * 
     * @return The value as a Object.
     * 
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of a
     *             recognizable class.
     * 
     * @throws MalformedValueException
     *             if this <tt>Value</tt> is structurally corrupt.
     */
    public Object peek(Object target) {
        return peek(target, null);
    }

    /**
     * <p>
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt>. This method is identical to
     * {@link #get(Object, CoderContext)} except that in <a
     * href="#_streamMode">Stream Mode</a> the pointer to the next retrieved
     * value is not advanced.
     * </p>
     * 
     * @param target
     *            A mutable object into which a {@link ValueRenderer} <i>may</i>
     *            decode this <tt>Value</tt>.
     * 
     * @return The value as a Object.
     * 
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of a
     *             recognizable class.
     * 
     * @throws MalformedValueException
     *             if this <tt>Value</tt> is structurally corrupt.
     */
    public Object peek(Object target, CoderContext context) {
        final Object object;
        final int saveDepth = _depth;
        final int saveLevel = _level;
        final int saveNext = _next;
        final int saveEnd = _end;
        final boolean saveAtomic = _atomicIncrementArmed;
        try {
            object = get(target, context);
        } finally {
            _atomicIncrementArmed = saveAtomic;
            _end = saveEnd;
            _next = saveNext;
            _level = saveLevel;
            _depth = saveDepth;
        }
        return object;
    }

    /**
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt>. If the represented value is primitive, this method
     * returns the wrapped object of the corresponding class. For example, if
     * the value represents an <tt>int</tt>, this method returns a
     * <tt>java.lang.Integer</tt>.
     * 
     * @return The value as a Object.
     * 
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of a
     *             recognizable class.
     * 
     * @throws MalformedValueException
     *             if this <tt>Value</tt> is structurally corrupt.
     */
    public Object get() {
        return get(null, null);
    }

    /**
     * <p>
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt>. If the represented value is primitive, this method
     * returns the wrapped object of the corresponding class. For example, if
     * the value represents an <tt>int</tt>, this method returns a
     * <tt>java.lang.Integer</tt>.
     * </p>
     * <p>
     * This variant of <tt>get</tt> <i>may</i> modify and return the target
     * object supplied as a parameter, rather than creating a new object. This
     * behavior will occur only if the encoded value has a registered
     * {@link ValueRenderer}. See the documentation for <tt>ValueRenderer</tt>
     * for more information.
     * </p>
     * 
     * @param target
     *            A mutable object into which a {@link ValueRenderer} <i>may</i>
     *            decode this <tt>Value</tt>.
     * 
     * @return The value as a Object.
     * 
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of a
     *             recognizable class.
     * 
     * @throws MalformedValueException
     *             if this <tt>Value</tt> is structurally corrupt.
     */
    public Object get(Object target) {
        return get(target, null);
    }

    /**
     * <p>
     * Decodes the object value represented by the current state of this
     * <tt>Value</tt>. If the represented value is primitive, this method
     * returns the wrapped object of the corresponding class. For example, if
     * the value represents an <tt>int</tt>, this method returns a
     * <tt>java.lang.Integer</tt>.
     * </p>
     * <p>
     * This variant of <tt>get</tt> <i>may</i> modify and return the target
     * object supplied as a parameter, rather than creating a new object. This
     * behavior will occur only if the encoded value has an associated
     * {@link ValueRenderer} registered by {@link CoderManager}. See the
     * documentation for those classes for a detailed explanation of value
     * rendering.
     * </p>
     * 
     * @param target
     *            A mutable object into which a {@link ValueRenderer} <i>may</i>
     *            decode this <tt>Value</tt>.
     * 
     * @param context
     *            An application-specified value that may assist a
     *            {@link ValueCoder}. The context is passed to the
     *            {@link ValueCoder#get} method.
     * 
     * @return The value as a Object.
     * 
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent data of a
     *             recognizable class.
     * 
     * @throws MalformedValueException
     *             if this <tt>Value</tt> is structurally corrupt.
     */
    public Object get(Object target, CoderContext context) {
        Object object = null;
        int start = _next;
        int classHandle = nextType();
        int currentItemCount = _serializedItemCount++;

        switch (classHandle) {
        case TYPE_NULL:
            break;

        case TYPE_BOOLEAN:
        case CLASS_BOOLEAN:
            object = getBooleanInternal() ? Boolean.TRUE : Boolean.FALSE;
            break;

        case TYPE_BYTE:
        case CLASS_BYTE:
            object = Byte.valueOf(getByteInternal());
            break;

        case TYPE_SHORT:
        case CLASS_SHORT:
            object = Short.valueOf(getShortInternal());
            break;

        case TYPE_CHAR:
        case CLASS_CHAR:
            object = Character.valueOf(getCharInternal());
            break;

        case TYPE_INT:
        case CLASS_INT:
            object = Integer.valueOf(getIntInternal());
            break;

        case TYPE_FLOAT:
        case CLASS_FLOAT:
            object = Float.valueOf(getFloatInternal());
            break;

        case TYPE_LONG:
        case CLASS_LONG:
            object = Long.valueOf(getLongInternal());
            break;

        case TYPE_DOUBLE:
        case CLASS_DOUBLE:
            object = Double.valueOf(getDoubleInternal());
            break;

        case CLASS_STRING: {
            char[] sab = getStringAssemblyBuffer(_end - _next);
            int length = utfToCharArray(sab, _next, _end);
            object = new String(sab, 0, length);
            closeVariableLengthItem();
            break;
        }

        case CLASS_DATE:
            long time = Util.getLong(_bytes, _next);
            _next += 8;
            object = new Date(time);
            break;

        case CLASS_BIG_INTEGER: {
            int length = _end - _next;
            byte[] bytes = new byte[length];
            System.arraycopy(_bytes, _next, bytes, 0, length);
            _next += length;
            object = new BigInteger(bytes);
            closeVariableLengthItem();
            break;
        }

        case CLASS_BIG_DECIMAL: {
            int length = _end - _next;
            int scale = Util.getInt(_bytes, _next);
            byte[] bytes = new byte[length - 4];
            System.arraycopy(_bytes, _next + 4, bytes, 0, length - 4);
            _next += length;
            object = new BigDecimal(new BigInteger(bytes), scale);
            closeVariableLengthItem();
            break;
        }

        case CLASS_ANTIVALUE: {
            int length = _end - _next;
            int elisionCount = Util.getShort(_bytes, _next);
            byte[] bytes = new byte[length - 2];
            System.arraycopy(_bytes, _next + 2, bytes, 0, length - 2);
            _next += length;
            object = new AntiValue(elisionCount, bytes);
            closeVariableLengthItem();
            break;
        }

        case CLASS_ARRAY: {
            try {
                _depth++;
                int componentClassHandle = nextType();
                switch (componentClassHandle) {
                case TYPE_BOOLEAN: {
                    boolean[] result = new boolean[_end - _next];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = toBoolean(_next + index);
                    }
                    object = result;
                    break;
                }

                case TYPE_BYTE: {
                    byte[] result = new byte[_end - _next];
                    System.arraycopy(_bytes, _next, result, 0, _end - _next);
                    object = result;
                    break;
                }

                case TYPE_SHORT: {
                    short[] result = new short[arraySize(_end, _next, 2)];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = (short) Util.getShort(_bytes, _next
                                + (index * 2));
                    }
                    object = result;
                    break;
                }

                case TYPE_CHAR: {
                    char[] result = new char[arraySize(_end, _next, 2)];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = (char) Util.getChar(_bytes, _next
                                + (index * 2));
                    }
                    object = result;
                    break;
                }

                case TYPE_INT: {
                    int[] result = new int[arraySize(_end, _next, 4)];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = (int) Util.getInt(_bytes, _next
                                + (index * 4));
                    }
                    object = result;
                    break;
                }

                case TYPE_LONG: {
                    long[] result = new long[arraySize(_end, _next, 8)];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = (long) Util.getLong(_bytes, _next
                                + (index * 8));
                    }
                    object = result;
                    break;
                }

                case TYPE_FLOAT: {
                    float[] result = new float[arraySize(_end, _next, 4)];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = Float.intBitsToFloat(Util.getInt(
                                _bytes, _next + (index * 4)));
                    }
                    object = result;
                    break;
                }

                case TYPE_DOUBLE: {
                    double[] result = new double[arraySize(_end, _next, 8)];
                    for (int index = 0; index < result.length; index++) {
                        result[index] = Double.longBitsToDouble(Util.getLong(
                                _bytes, _next + (index * 8)));
                    }
                    object = result;
                    break;
                }

                case CLASS_STRING: {
                    int length = decodeElementCount();
                    String[] result = new String[length];
                    for (int index = 0; index < length; index++) {
                        result[index] = getString();
                    }
                    object = result;
                    break;
                }

                default: {
                    Class componentClass = classForHandle(componentClassHandle);
                    int length = decodeElementCount();
                    Object[] result = (Object[]) Array.newInstance(
                            componentClass, length);
                    getValueCache().store(currentItemCount, result);
                    for (int index = 0; index < length; index++) {
                        Array.set(result, index, get(null, null));
                    }
                    object = result;
                    break;
                }
                }
            } finally {
                _depth--;
            }
            closeVariableLengthItem();
            break;

        }

        case CLASS_MULTI_ARRAY:
            _next--;
            _serializedItemCount--;
            object = getMultiArray(null);
            break;

        case CLASS_SERIALIZED:
            _depth++;
            try {
                ObjectInputStream ois = new OldValueInputStream(this);
                object = ois.readObject();
                if (_next != _end) {
                    throw new ConversionException(
                            "Invalid serialized Object at index=" + _next);
                }
                closeVariableLengthItem();
            } catch (IOException ioe) {
                throw new ConversionException("@" + start, ioe);
            } catch (ClassNotFoundException cnfe) {
                throw new ConversionException("@" + start, cnfe);
            } finally {
                _depth--;
            }

            break;

        case CLASS_REREF: {
            int base = _bytes[_next++] & 0xFF;
            int handle = decodeVariableLengthInt(base);
            object = getValueCache().get(handle);
            break;
        }

        default: {
            int saveDepth = _depth;
            try {
                _depth++;
                Class cl = _persistit.classForHandle(classHandle);
                ValueCoder coder = getValueCoder(cl);

                if (coder != null) {
                    if (target == null) {
                        object = coder.get(this, cl, context);
                    }

                    else if (coder instanceof ValueRenderer) {
                        ((ValueRenderer) coder).render(this, target, cl,
                                context);
                        object = target;
                    } else {
                        throw new ConversionException(
                                "No ValueRenderer for class " + cl.getName());
                    }
                } else {
                    throw new ConversionException("No ValueCoder for class "
                            + cl.getName());
                }
            } finally {
                _depth = saveDepth;
            }
            closeVariableLengthItem();
            break;
        }
        }
        if (_depth > 0) {
            getValueCache().store(currentItemCount, object);
        } else {
            releaseValueCache();
        }
        return object;
    }

    private int arraySize(int end, int next, int blockSize) {
        int size = end - next;
        if ((size % blockSize) != 0) {
            throw new ConversionException("Invalid array size");
        }
        return size / blockSize;
    }

    private int utfToCharArray(char[] sab, int offset, int end) {
        int counter = 0;

        for (int i = offset; i < end; i++) {
            int b = _bytes[i] & 0xFF;
            int b2;
            int b3;
            switch (b >> 4) {

            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                /* 0xxxxxxx */
                sab[counter++] = ((char) b);
                break;

            case 12:
            case 13:
                /* 110x xxxx 10xx xxxx */
                i++;
                if (i >= _end) {
                    throw new ConversionException();
                }
                b2 = _bytes[i];
                if ((b2 & 0xC0) != 0x80) {
                    throw new ConversionException();
                }
                sab[counter++] = (char) (((b & 0x1F) << 6) | (b2 & 0x3F));
                break;

            case 14:
                /* 1110 xxxx 10xx xxxx 10xx xxxx */
                i += 2;
                if (i >= _end) {
                    throw new ConversionException();
                }
                b2 = _bytes[i - 1];
                b3 = _bytes[i];
                if (((b2 & 0xC0) != 0x80) || ((b3 & 0xC0) != 0x80)) {
                    throw new ConversionException();
                }
                sab[counter++] = (char) (((b & 0x0F) << 12)
                        | ((b2 & 0x3F) << 6) | ((b3 & 0x3F) << 0));
                break;

            default:
                /* 10xx xxxx, 1111 xxxx */
                throw new ConversionException();
            }
        }
        return counter;
    }

    /**
     * Registers an object with an internal handle used to represent back
     * references. For example, suppose objects x and y have fields that refer
     * to each other. Then serializing x will serialize y, and the the reference
     * in y to x will be represented in the serialization stream by a back
     * reference handle. This method should be called from the
     * {@link ValueCoder#get(Value, Class, CoderContext)} method of custom
     * <tt>ValueCoder</tt>s. See <a
     * href="../../../Object_Serialization_Notes.html>Persistit 1.1 Object
     * Serialization</a> for further details.
     * 
     * @param object
     *            A newly created objected whose fields are about to be
     *            deserialized
     */
    public void registerEncodedObject(Object object) {
        if (_depth > 0) {
            getValueCache().store(_serializedItemCount - 1, object);
        }
    }

    /**
     * Registers a display marker with the current object handle.
     * 
     * @param index
     *            Current position in the output string
     */
    private void registerEncodedObject(int index) {
        if (_depth > 0) {
            getValueCache().store(_serializedItemCount - 1,
                    new DisplayMarker(index));
        }
    }

    /**
     * Decodes the <tt>java.lang.String</tt> value represented by the current
     * state of this <tt>Value</tt>.
     * 
     * @return The value as a String.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent a String.
     */
    public String getString() {
        return (String) getExpectedType(String.class);
    }

    /**
     * Decodes the <tt>java.lang.String</tt> value represented by the current
     * state of this <tt>Value</tt> into a supplied
     * <tt>java.lang.StringBuilder</tt>.
     * 
     * @return The supplied StringBuilder, modified to contain the decoded
     *         String
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent a String.
     */
    public StringBuilder getString(StringBuilder sb) {
        _serializedItemCount++;
        if (nextType(CLASS_STRING) == TYPE_NULL)
            return null;
        sb.setLength(0);
        char[] sab = getStringAssemblyBuffer(_end - _next);
        int length = utfToCharArray(sab, _next, _end);
        sb.append(sab, 0, length);
        closeVariableLengthItem();
        return sb;
    }

    /**
     * Decodes the <tt>java.util.Date</tt> value represented by the current
     * state of this <tt>Value</tt>.
     * 
     * @return The value as a Date.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent a Date.
     */
    public Date getDate() {
        return (Date) getExpectedType(Date.class);
    }

    /**
     * Decodes the <tt>java.math.BigInteger</tt> value represented by the
     * current state of this <tt>Value</tt>.
     * 
     * @return The value as a BigInteger.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent a
     *             BigInteger.
     */
    public BigInteger getBigInteger() {
        return (BigInteger) getExpectedType(BigInteger.class);
    }

    /**
     * Decodes the <tt>java.math.BigDecimal</tt> value represented by the
     * current state of this <tt>Value</tt>.
     * 
     * @return The value as a BigDecimal.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent a
     *             BigDecimal.
     */
    public BigDecimal getBigDecimal() {
        return (BigDecimal) getExpectedType(BigDecimal.class);
    }

    /**
     * Returns the element count for the array represented by the current state
     * of this <tt>Value</tt>.
     * 
     * @return The element count, or -1 if the Value represents <tt>null</tt>
     *         rather than an array.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent an array.
     */
    public int getArrayLength() {
        int start = _next;
        int type = nextType(CLASS_ARRAY, CLASS_MULTI_ARRAY);
        if (type == TYPE_NULL)
            return -1;
        int componentClassHandle = -1;
        int result = -1;
        try {
            _depth++;
            componentClassHandle = nextType();
            if (type == CLASS_MULTI_ARRAY)
                _next++; // skip the dimension count.
            int length = _end - _next;
            if (type == CLASS_ARRAY && componentClassHandle > 0
                    && componentClassHandle <= TYPE_DOUBLE
                    && FIXED_ENCODING_SIZES[componentClassHandle] > 0) {
                result = length / FIXED_ENCODING_SIZES[componentClassHandle];
            } else {
                result = decodeElementCount();
            }
        } finally {
            closeVariableLengthItem();
            _depth--;
            _next = start;
        }
        return result;
    }

    /**
     * Decodes the array value represented by the current state of this
     * <tt>Value</tt>.
     * 
     * @return The value as an Object that can be cast to an array.
     * @throws ConversionException
     *             if this <tt>Value</tt> does not currently represent an array.
     */
    public Object getArray() {
        Object object = get(null, null);
        if (object == null || object.getClass().isArray()) {
            return object;
        }
        throw new ConversionException("Expected an array but value is a "
                + object.getClass().getName());
    }

    /**
     * Returns a <tt>boolean</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(boolean)[])get()</tt>.
     * 
     * @return The array.
     */
    public boolean[] getBooleanArray() {
        return (boolean[]) getExpectedType(boolean[].class);
    }

    /**
     * Copies a subarray of the <tt>boolean</tt> array represented by the state
     * of this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getBooleanArray(boolean[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_BOOLEAN);
            int sourceLength = _end - _next;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = toBoolean(index + fromOffset);
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>byte</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(byte[])get()</tt>.
     * 
     * @return The array.
     */
    public byte[] getByteArray() {
        return (byte[]) getExpectedType(byte[].class);
    }

    /**
     * Copies a subarray of the <tt>byte</tt> array represented by the state of
     * this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getByteArray(byte[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (array == _bytes) {
            throw new IllegalArgumentException("Can't overwrite encoded bytes");
        }
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_BYTE);
            int sourceLength = _end - _next;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            if (length > 0)
                System.arraycopy(_bytes, _next + fromOffset, array, toOffset,
                        length);
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>short</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(short[])get()</tt>.
     * 
     * @return The array.
     */
    public short[] getShortArray() {
        return (short[]) getExpectedType(short[].class);
    }

    /**
     * Copies a subarray of the <tt>short</tt> array represented by the state of
     * this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getShortArray(short[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_SHORT);
            int sourceLength = (_end - _next) / 2;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = (short) Util.getShort(_bytes, _next
                        + (index + fromOffset) * 2);
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>char</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(char[])get()</tt>.
     * 
     * @return The array.
     */
    public char[] getCharArray() {
        return (char[]) getExpectedType(char[].class);
    }

    /**
     * Copies a subarray of the <tt>char</tt> array represented by the state of
     * this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getCharArray(char[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_CHAR);
            int sourceLength = (_end - _next) / 2;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = (char) Util.getChar(_bytes, _next
                        + (index + fromOffset) * 2);
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>int</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(int[])get()</tt>.
     * 
     * @return The array.
     */
    public int[] getIntArray() {
        return (int[]) getExpectedType(int[].class);
    }

    /**
     * Copies a subarray of the <tt>int</tt> array represented by the state of
     * this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getIntArray(int[] array, int fromOffset, int toOffset, int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_INT);
            int sourceLength = (_end - _next) / 4;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = (int) Util.getInt(_bytes, _next
                        + (index + fromOffset) * 4);
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>long</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(long[])get()</tt>.
     * 
     * @return The array.
     */
    public long[] getLongArray() {
        return (long[]) getExpectedType(long[].class);
    }

    /**
     * Copies a subarray of the <tt>long</tt> array represented by the state of
     * this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getLongArray(long[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        ;
        try {
            _depth++;
            nextType(TYPE_LONG);
            int sourceLength = (_end - _next) / 8;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = (long) Util.getLong(_bytes, _next
                        + (index + fromOffset) * 8);
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>float</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(float[])get()</tt>.
     * 
     * @return The array.
     */
    public float[] getFloatArray() {
        return (float[]) getExpectedType(float[].class);
    }

    /**
     * Copies a subarray of the <tt>float</tt> array represented by the state of
     * this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getFloatArray(float[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_FLOAT);
            int sourceLength = (_end - _next) / 4;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = Float.intBitsToFloat(Util.getInt(
                        _bytes, _next + (index + fromOffset) * 4));
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>double</tt> array representing the state of this
     * <tt>Value</tt>. Equivalent to </tt>(double[])get()</tt>.
     * 
     * @return The array.
     */
    public double[] getDoubleArray() {
        return (double[]) getExpectedType(double[].class);
    }

    /**
     * Copies a subarray of the <tt>double</tt> array represented by the state
     * of this <tt>Value</tt> into the supplied target array. The subarray is
     * bounded by <tt>fromOffset</tt> and <tt>length</tt>, and truncated to fit
     * within the target array.
     * 
     * @param array
     *            The target array
     * @param fromOffset
     *            Offset of the first element within the source array to copy
     *            from
     * @param toOffset
     *            Offset of the first element within the target array to copy to
     * @param length
     *            The maximum number of elements to copy.
     * 
     * @return The number of elements actually copied, or -1 if the
     *         <tt>Value</tt> object represents <tt>null</tt>.
     */
    public int getDoubleArray(double[] array, int fromOffset, int toOffset,
            int length) {
        _serializedItemCount++;
        if (nextType(CLASS_ARRAY) == TYPE_NULL)
            return -1;
        try {
            _depth++;
            nextType(TYPE_DOUBLE);
            int sourceLength = (_end - _next) / 8;
            if (length > sourceLength - fromOffset)
                length = sourceLength - fromOffset;
            if (length > array.length - toOffset)
                length = array.length - toOffset;
            for (int index = 0; index < length; index++) {
                array[toOffset + index] = Double.longBitsToDouble(Util.getLong(
                        _bytes, _next + (index + fromOffset) * 8));
            }
            closeVariableLengthItem();
            return length;
        } finally {
            _depth--;
        }
    }

    /**
     * Returns a <tt>Object</tt> array representing the state of this
     * <tt>Value</tt>. This is equivalent to </tt>(Object[])getArray()</tt>.
     * 
     * @return The array.
     */
    public Object[] getObjectArray() {
        return (Object[]) getExpectedType(Object[].class);
    }

    /**
     * Returns a <tt>Object</tt> array representing the state of this
     * <tt>Value</tt>. This is equivalent to </tt>(Object[])getArray()</tt>.
     * 
     * @return The array.
     */
    public String[] getStringArray() {
        return (String[]) getExpectedType(String[].class);
    }

    /**
     * Indicates whether there is at least one more item in this <tt>Value</tt>.
     * This method is valid only if the Value is in <a
     * href="#_streamMode">Stream Mode</a>. (See {@link #isStreamMode}.)
     * 
     * @return <tt>true</tt> if another item can be decoded from this
     *         <tt>Value</tt>.
     */
    public boolean hasMoreItems() {
        return _next < _end;
    }

    /**
     * Replaces the current state with the supplied <tt>boolean</tt> value (or
     * in <a href="#_streamMode">stream mode</a>, appends a new field containing
     * this value to the state).
     * 
     * @param booleanValue
     *            The new value
     */
    public void put(boolean booleanValue) {
        preparePut();
        ensureFit(2);
        _bytes[_size++] = (byte) TYPE_BOOLEAN;
        _bytes[_size++] = (byte) (booleanValue ? TRUE_CHAR : FALSE_CHAR);
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>byte</tt> value (or in
     * <a href="#_streamMode">stream mode</a>, appends a new field containing
     * this value to the state).
     * 
     * @param byteValue
     *            The new value
     */
    public void put(byte byteValue) {
        preparePut();
        ensureFit(2);
        _bytes[_size++] = (byte) TYPE_BYTE;
        _bytes[_size++] = byteValue;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>short</tt> value (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param shortValue
     *            The new value
     */
    public void put(short shortValue) {
        preparePut();
        ensureFit(3);
        _bytes[_size++] = (byte) TYPE_SHORT;
        Util.putShort(_bytes, _size, shortValue);
        _size += 2;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>char</tt> value (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param charValue
     *            The new value
     */
    public void put(char charValue) {
        preparePut();
        ensureFit(3);
        _bytes[_size++] = (byte) TYPE_CHAR;
        Util.putChar(_bytes, _size, charValue);
        _size += 2;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>int</tt> value (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param intValue
     *            The new value
     */
    public void put(int intValue) {
        preparePut();
        ensureFit(5);
        _bytes[_size++] = (byte) TYPE_INT;
        Util.putInt(_bytes, _size, intValue);
        _size += 4;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>long</tt> value (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param longValue
     *            The new value
     */
    public void put(long longValue) {
        preparePut();
        ensureFit(9);
        _bytes[_size++] = (byte) TYPE_LONG;
        Util.putLong(_bytes, _size, longValue);
        _size += 8;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>float</tt> value (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param floatValue
     *            The new value
     */
    public void put(float floatValue) {
        preparePut();
        ensureFit(5);
        _bytes[_size++] = (byte) TYPE_FLOAT;
        Util.putInt(_bytes, _size, Float.floatToIntBits(floatValue));
        _size += 4;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>double</tt> value (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param doubleValue
     *            The new value
     */
    public void put(double doubleValue) {
        preparePut();
        ensureFit(9);
        _bytes[_size++] = (byte) TYPE_DOUBLE;
        Util.putLong(_bytes, _size, Double.doubleToLongBits(doubleValue));
        _size += 8;
        _serializedItemCount++;
    }

    /**
     * Replaces the current state with the supplied <tt>Object</tt> (or in <i><a
     * href="#_streamMode">stream mode</a></i>, appends a new field containing
     * this value to the state).
     * 
     * @param object
     *            The new value. The supplied Object must be <i>null</i>, or it
     *            must implement <tt>java.io.Serializable</tt> or
     *            <tt>java.io.Externalizable</tt>, or it must be handled by a
     *            registered {@link com.persistit.encoding.ValueCoder}.
     * 
     * @throws ConversionException
     *             if the Object cannot be encoded as a sequence of bytes.
     */
    public void put(Object object) {
        put(object, null);
    }

    /**
     * Replaces the current state with the supplied <tt>Object</tt> (or in <i><a
     * href="#_streamMode">stream mode</a></i>, appends a new field containing
     * this value to the state).
     * 
     * @param object
     *            The new value. The supplied Object must be <i>null</i>, or it
     *            must implement <tt>java.io.Serializable</tt> or
     *            <tt>java.io.Externalizable</tt>, or it must be handled by a
     *            registered {@link com.persistit.encoding.ValueCoder}.
     * 
     * @param context
     *            An application-specified value that may assist a
     *            {@link ValueCoder}. The context is passed to the
     *            {@link ValueCoder#put} method.
     * 
     * @throws ConversionException
     *             if the Object cannot be encoded as a sequence of bytes.
     */
    public void put(Object object, CoderContext context) {
        preparePut();
        int currentItemCount = _serializedItemCount++;

        if (object == null) {
            ensureFit(1);
            _bytes[_size++] = TYPE_NULL;
            return;
        }
        if (_shared) {
            int serializationHandle = getValueCache().put(currentItemCount,
                    object);
            if (serializationHandle != -1) {
                ensureFit(5);
                _bytes[_size++] = (byte) CLASS_REREF;
                _size += encodeVariableLengthInt(0, _size, serializationHandle);
                if (_depth == 0)
                    releaseValueCache();
                return;
            }
        }
        Class cl = object.getClass();
        if (cl == String.class) {
            String string = (String) object;
            putUTF(string);
        } else if (cl == Date.class) {
            ensureFit(9);
            _bytes[_size++] = CLASS_DATE;
            Util.putLong(_bytes, _size, ((Date) object).getTime());
            _size += 8;
        } else if (cl == BigInteger.class) {
            byte[] bytes = ((BigInteger) object).toByteArray();
            int length = bytes.length;
            ensureFit(length + 2);
            int index = _size;
            _bytes[index++] = CLASS_BIG_INTEGER;
            System.arraycopy(bytes, 0, _bytes, index, length);
            _size = index + length;
            endVariableSizeItem(length + 1);

        } else if (cl == BigDecimal.class) {
            BigDecimal bigDecimalValue = (BigDecimal) object;
            BigInteger unscaled = bigDecimalValue.unscaledValue();
            byte[] bytes = unscaled.toByteArray();
            int length = bytes.length;
            ensureFit(length + 8);
            int index = _size;
            _bytes[index++] = CLASS_BIG_DECIMAL;
            Util.putInt(_bytes, index, bigDecimalValue.scale());
            index += 4;
            System.arraycopy(bytes, 0, _bytes, index, length);
            _size = index + length;
            endVariableSizeItem(length + 5);
        }
        //
        // All Primitive wrapper classes go here.
        //
        else if (cl == Boolean.class) {
            ensureFit(2);
            _bytes[_size++] = (byte) CLASS_BOOLEAN;
            _bytes[_size++] = (byte) (((Boolean) object).booleanValue() ? TRUE_CHAR
                    : FALSE_CHAR);
        } else if (cl == Byte.class) {
            ensureFit(2);
            _bytes[_size++] = (byte) CLASS_BYTE;
            _bytes[_size++] = ((Byte) object).byteValue();
        } else if (cl == Short.class) {
            ensureFit(3);
            _bytes[_size++] = (byte) CLASS_SHORT;
            Util.putShort(_bytes, _size, ((Short) object).shortValue());
            _size += 2;
        } else if (cl == Character.class) {
            ensureFit(3);
            _bytes[_size++] = (byte) CLASS_CHAR;
            Util.putChar(_bytes, _size, ((Character) object).charValue());
            _size += 2;
        } else if (cl == Integer.class) {
            ensureFit(5);
            _bytes[_size++] = (byte) CLASS_INT;
            Util.putInt(_bytes, _size, ((Integer) object).intValue());
            _size += 4;
        } else if (cl == Long.class) {
            ensureFit(9);
            _bytes[_size++] = (byte) CLASS_LONG;
            Util.putLong(_bytes, _size, ((Long) object).longValue());
            _size += 8;
        } else if (cl == Float.class) {
            ensureFit(5);
            _bytes[_size++] = (byte) CLASS_FLOAT;
            Util.putInt(_bytes, _size,
                    Float.floatToRawIntBits(((Float) object).floatValue()));
            _size += 4;
        } else if (cl == Double.class) {
            ensureFit(9);
            _bytes[_size++] = (byte) CLASS_DOUBLE;
            Util.putLong(_bytes, _size,
                    Double.doubleToRawLongBits(((Double) object).doubleValue()));
            _size += 8;
        } else if (cl.isArray()) {
            Class componentClass = cl.getComponentType();
            int length = Array.getLength(object);
            if (componentClass.isPrimitive()) {
                if (componentClass == Boolean.TYPE) {
                    putBooleanArray1((boolean[]) object, 0, length);
                } else if (componentClass == Byte.TYPE) {
                    putByteArray1((byte[]) object, 0, length);
                } else if (componentClass == Short.TYPE) {
                    putShortArray1((short[]) object, 0, length);
                } else if (componentClass == Character.TYPE) {
                    putCharArray1((char[]) object, 0, length);
                } else if (componentClass == Integer.TYPE) {
                    putIntArray1((int[]) object, 0, length);
                } else if (componentClass == Long.TYPE) {
                    putLongArray1((long[]) object, 0, length);
                } else if (componentClass == Float.TYPE) {
                    putFloatArray1((float[]) object, 0, length);
                } else if (componentClass == Double.TYPE) {
                    putDoubleArray1((double[]) object, 0, length);
                }
            } else {
                putObjectArray1((Object[]) object, 0, length);
            }
        }

        else {
            ensureFit(6);
            int start = _size;
            int end = start;
            boolean replaced = false;

            try {
                _depth++;
                ValueCoder coder = getValueCoder(cl);

                while (coder instanceof DefaultObjectCoder) {
                    Object replacement = ((DefaultObjectCoder) coder)
                            .writeReplace(this, object);

                    if (replacement == object)
                        break;

                    replaced = true;

                    if (replacement != null) {
                        object = replacement;
                        cl = replacement.getClass();
                        coder = getValueCoder(cl);
                    } else {
                        break;
                    }
                }

                if (replaced) {
                    put(object, context);
                    end = _size;
                } else {
                    int handle;
                    if (cl == Object.class) {
                        handle = CLASS_OBJECT;
                    } else
                        handle = _persistit.getClassIndex().lookupByClass(cl)
                                .getHandle();

                    if (coder != null) {
                        _size += encodeVariableLengthInt(CLASS1, _size, handle
                                - CLASS1);

                        coder.put(this, object, context);
                        end = _size;
                    } else {
                        _bytes[_size++] = CLASS_SERIALIZED;
                        ObjectOutputStream oos = new OldValueOutputStream(this);
                        oos.writeObject(object);
                        oos.close();
                        end = _size;
                    }
                }
            } catch (IOException ioe) {
                throw new ConversionException(ioe);
            } finally {
                _depth--;
                //
                // Restores _size to original value in the event of an
                // exception.
                // No-op on successful completion, because end will be equal to
                // the updated value of _size. This is kludgey, but it gets
                // the semantics right.
                //
                _size = end;
            }
            if (!replaced)
                endVariableSizeItem(_size - start);
        }
        if (_depth == 0) {
            releaseValueCache();
        }
    }

    /**
     * Replaces the current state with <i>null</i> (or in <i><a
     * href="#_streamMode">stream mode</a></i>, appends a null to the state).
     */
    public void putNull() {
        put(null, null);
    }

    /**
     * Replaces the current state with the supplied <tt>java.lang.String</tt>
     * (or in <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param string
     *            The new value
     */
    public void putString(String string) {
        put(string, null);
    }

    /**
     * Replaces the current state with the supplied <tt>java.lang.String</tt>
     * (or in <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state). Unlike <tt>putString</tt>, this
     * method always writes a new copy of the String rather than a reference to
     * a previously written value. Thus on deserialization, two copies of the
     * same string written by this method will result in two unique String
     * objects.
     * 
     * @param string
     *            The new value
     */
    public void putUTF(String string) {
        int length = string.length();
        ensureFit(length + 1);
        int index = _size;
        _bytes[index++] = (byte) CLASS_STRING;

        int maxLength = _bytes.length;

        for (int i = 0; i < length; i++) {
            char c = string.charAt(i);

            if (c <= 0x007F) {
                if (index + 1 > maxLength) {
                    _size = index;
                    ensureFit(index + 1 + (length - i) * 2);
                }
                _bytes[index++] = (byte) c;
            } else if (c > 0x07FF) {
                if (index + 3 > maxLength) {
                    _size = index;
                    ensureFit(index + 3 + (length - i) * 2);
                }
                _bytes[index++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                _bytes[index++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                _bytes[index++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            } else {
                if (index + 2 > maxLength) {
                    _size = index;
                    ensureFit(index + 2 + (length - i) * 2);
                }
                _bytes[index++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                _bytes[index++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            }
        }
        length = index - _size;
        _size = index;
        endVariableSizeItem(length);
    }

    /**
     * Replaces the current state with the String represented by the supplied
     * (or in <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state). This is equivalent to
     * <tt>putString(sb.toString())</tt> but its implementation is somewhat more
     * efficient.
     * 
     * @param sb
     *            The new value
     */
    public void putString(StringBuilder sb) {
        if (sb == null) {
            putNull();
        } else {
            _serializedItemCount++;
            preparePut();
            int length = sb.length();
            ensureFit(length + 1);
            int index = _size;
            int maxLength = _bytes.length;
            _bytes[index++] = (byte) CLASS_STRING;

            for (int i = 0; i < length; i++) {
                char c = sb.charAt(i);

                if (c <= 0x007F) {
                    if (index + 1 > maxLength) {
                        _size = index;
                        ensureFit(index + 1 + (length - i) * 2);
                    }
                    _bytes[index++] = (byte) c;
                } else if (c > 0x07FF) {
                    if (index + 3 > maxLength) {
                        _size = index;
                        ensureFit(index + 3 + (length - i) * 2);
                    }
                    _bytes[index++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                    _bytes[index++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                    _bytes[index++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                } else {
                    if (index + 2 > maxLength) {
                        _size = index;
                        ensureFit(index + 2 + (length - i) * 2);
                    }
                    _bytes[index++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                    _bytes[index++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                }
            }
            length = index - _size;
            _size = index;
            endVariableSizeItem(length);
        }
    }

    /**
     * Replaces the current state with the supplied <tt>java.util.Date</tt> (or
     * in <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this value to the state).
     * 
     * @param dateValue
     *            The new value
     */
    public void putDate(Date dateValue) {
        put(dateValue, null);
    }

    /**
     * Replaces the current state with the supplied
     * <tt>java.math.BigInteger</tt> (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this value to the state).
     * 
     * @param bigIntValue
     *            The new value
     */
    public void putBigInteger(BigInteger bigIntValue) {
        put(bigIntValue, null);
    }

    /**
     * Replaces the current state with the supplied
     * <tt>java.math.BigDecimal</tt> (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this value to the state).
     * 
     * @param bigDecimalValue
     *            The new value
     */
    public void putBigDecimal(BigDecimal bigDecimalValue) {
        put(bigDecimalValue, null);
    }

    /**
     * Replaces the current state with the supplied <tt>boolean</tt> (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putBooleanArray(boolean[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>boolean</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putBooleanArray(boolean[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putBooleanArray1(array, offset, length);
    }

    private void putBooleanArray1(boolean[] array, int offset, int length) {
        ensureFit(length + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_BOOLEAN;
        for (int index = 0; index < length; index++) {
            Util.putByte(_bytes, _size, array[index + offset] ? TRUE_CHAR
                    : FALSE_CHAR);
            _size++;
        }
        endVariableSizeItem(length + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>byte</tt> array, (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putByteArray(byte[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>byte</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putByteArray(byte[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putByteArray1(array, offset, length);
    }

    private void putByteArray1(byte[] array, int offset, int length) {
        ensureFit(length + 2);
        int index = _size;
        _bytes[index++] = CLASS_ARRAY;
        _bytes[index++] = TYPE_BYTE;
        Util.arraycopy(array, offset, _bytes, index, length);
        _size = index + length;
        endVariableSizeItem(length + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>short</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putShortArray(short[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>short</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putShortArray(short[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putShortArray1(array, offset, length);
    }

    void putShortArray1(short[] array, int offset, int length) {
        ensureFit(length * 2 + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_SHORT;
        for (int index = 0; index < length; index++) {
            Util.putShort(_bytes, _size, array[index + offset]);
            _size += 2;
        }
        endVariableSizeItem((length * 2) + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>char</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putCharArray(char[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>char</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putCharArray(char[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putCharArray1(array, offset, length);
    }

    private void putCharArray1(char[] array, int offset, int length) {
        ensureFit(length * 2 + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_CHAR;
        for (int index = 0; index < length; index++) {
            Util.putChar(_bytes, _size, array[index + offset]);
            _size += 2;
        }
        endVariableSizeItem((length * 2) + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>int</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putIntArray(int[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>int</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putIntArray(int[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putIntArray1(array, offset, length);
    }

    private void putIntArray1(int[] array, int offset, int length) {
        ensureFit(length * 4 + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_INT;
        for (int index = 0; index < length; index++) {
            Util.putInt(_bytes, _size, array[index + offset]);
            _size += 4;
        }
        endVariableSizeItem((length * 4) + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>long</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putLongArray(long[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>long</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putLongArray(long[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putLongArray1(array, offset, length);
    }

    private void putLongArray1(long[] array, int offset, int length) {
        ensureFit(length * 8 + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_LONG;
        for (int index = 0; index < length; index++) {
            Util.putLong(_bytes, _size, array[index + offset]);
            _size += 8;
        }
        endVariableSizeItem((length * 8) + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>float</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putFloatArray(float[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>float</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putFloatArray(float[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putFloatArray1(array, offset, length);
    }

    private void putFloatArray1(float[] array, int offset, int length) {
        ensureFit(length * 4 + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_FLOAT;
        for (int index = 0; index < length; index++) {
            Util.putInt(_bytes, _size,
                    Float.floatToRawIntBits(array[index + offset]));
            _size += 4;
        }
        endVariableSizeItem((length * 4) + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>double</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putDoubleArray(double[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>double</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putDoubleArray(double[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        putDoubleArray1(array, offset, length);
    }

    private void putDoubleArray1(double[] array, int offset, int length) {
        ensureFit(length * 8 + 2);
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = TYPE_DOUBLE;
        for (int index = 0; index < length; index++) {
            Util.putLong(_bytes, _size,
                    Double.doubleToRawLongBits(array[index + offset]));
            _size += 8;
        }
        endVariableSizeItem((length * 8) + 2);
    }

    /**
     * Replaces the current state with the supplied <tt>String</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putStringArray(String[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>String</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putStringArray(String[] array, int offset, int length) {
        checkArrayLength(length, offset, array.length);
        preparePut();
        ensureFit(7 + length);
        int start = _size;
        _bytes[_size++] = CLASS_ARRAY;
        _bytes[_size++] = CLASS_STRING;
        _size += encodeVariableLengthInt(COUNT1, _size, length);
        for (int index = 0; index < length; index++) {
            try {
                _depth++;
                putString(array[index + offset]);
            } finally {
                _depth--;
            }
        }
        endVariableSizeItem(_size - start);
    }

    /**
     * Replaces the current state with the supplied <tt>Object</tt> array (or in
     * <i><a href="#_streamMode">stream mode</a></i>, appends a new field
     * containing this array to the state).
     * 
     * @param array
     *            The new array value
     */
    public void putObjectArray(Object[] array) {
        put(array, null);
    }

    /**
     * Replaces the current state with a subarray of the supplied array of
     * <tt>Object</tt>-valued elements (or in <i><a href="#_streamMode">stream
     * mode</a></i>, appends a new field containing this subarray to the state).
     * 
     * @param array
     *            The array
     * @param offset
     *            Offset of the subarray in <tt>array</tt>
     * @param length
     *            Length of the subarray
     */
    public void putObjectArray(Object[] array, int offset, int length) {
        preparePut();
        putObjectArray1(array, offset, length);
    }

    /**
     * Does nothing except when this <tt>Value</tt> is in <i>stream mode</i>. In
     * stream mode, this method skips a field. It is generally faster to
     * <tt>skip</tt> a field rather than to <tt>get</tt> and discard it because
     * the value and its interior state do not actually need to be decoded and
     * constructed.
     */
    public void skip() {
        if (_depth == 0)
            return;
        int classHandle = nextType();
        if (classHandle == 0)
            return;
        int size = -1;
        if (classHandle < FIXED_ENCODING_SIZES.length) {
            size = FIXED_ENCODING_SIZES[classHandle];
        } else if (classHandle == CLASS_REREF) {
            int base = _bytes[_next++] & 0xFF;
            decodeVariableLengthInt(base);
            size = 0;
        }

        if (size >= 0) {
            _next += size;
        } else {
            closeVariableLengthItem();
        }
    }

    private void putObjectArray1(Object[] array, int offset, int length) {
        int dimensions = 1;
        Class componentType = array.getClass().getComponentType();

        while (componentType.isArray()) {
            componentType = componentType.getComponentType();
            dimensions++;
        }
        checkArrayLength(length, offset, array.length);
        int start = _size;
        ensureFit(12);
        _bytes[_size++] = (byte) (dimensions == 1 ? CLASS_ARRAY
                : CLASS_MULTI_ARRAY);
        encodeClass(componentType);
        if (dimensions != 1) {
            _bytes[_size++] = (byte) dimensions;
        }
        _size += encodeVariableLengthInt(COUNT1, _size, length);
        for (int index = 0; index < length; index++) {
            try {
                _depth++;
                put(array[index + offset]);
            } finally {
                _depth--;
            }
        }
        endVariableSizeItem(_size - start);
    }

    void putAntiValue(short elisionCount, byte[] bytes) {
        preparePut();
        int start = _size;
        ensureFit(8 + bytes.length);
        _bytes[_size++] = (byte) CLASS_ANTIVALUE;
        Util.putShort(_bytes, _size, elisionCount);
        _size += Util.putBytes(_bytes, _size + 2, bytes) + 2;
        endVariableSizeItem(_size - start);
    }

    void performAtomicIncrement() {

        int type;
        _atomicIncrementArmed = false;
        switch (type = _bytes[0]) {
        case TYPE_BYTE: {
            put((byte) (getByte() + _atomicIncrementValue));
            break;
        }
        case TYPE_SHORT: {
            put((short) (getShort() + _atomicIncrementValue));
            break;
        }
        case TYPE_CHAR: {
            put((char) (getChar() + _atomicIncrementValue));
            break;
        }
        case TYPE_INT: {
            put((int) (getInt() + _atomicIncrementValue));
            break;
        }
        case TYPE_LONG: {
            put((long) (getLong() + _atomicIncrementValue));
            break;
        }
        default: {
            throw new ConversionException(
                    "Existing value is not numeric. Type=" + type);
        }
        }
    }

    long getPointerValue() {
        return _pointer;
    }

    void setPointerValue(long pointer) {
        _pointer = pointer;
    }

    int getPointerPageType() {
        return _pointerPageType;
    }

    void setPointerPageType(int pageType) {
        _pointerPageType = pageType;
    }

    final void armAtomicIncrement(long value) {
        _atomicIncrementArmed = true;
        _atomicIncrementValue = value;
    }

    final void disarmAtomicIncrement() {
        _atomicIncrementArmed = false;
    }

    final boolean isAtomicIncrementArmed() {
        return _atomicIncrementArmed;
    }

    final long getAtomicIncrementValue() {
        return _atomicIncrementValue;
    }

    byte[] getLongBytes() {
        return _longBytes;
    }

    int getLongSize() {
        return _longSize;
    }

    void setLongSize(int size) {
        _longSize = size;
    }

    boolean isLongRecordMode() {
        return _longMode;
    }

    void setLongRecordMode(boolean mode) {
        _longMode = mode;
    }

    private ValueCoder getValueCoder(Class clazz) {
        CoderManager cm = _persistit.getCoderManager();
        if (cm != null) {
            return cm.getValueCoder(clazz);
        } else {
            return null;
        }
    }

    void changeLongRecordMode(boolean mode) {
        if (mode != _longMode) {
            if (_longBytes == null || _longBytes.length < Buffer.LONGREC_SIZE) {
                _longBytes = new byte[Buffer.LONGREC_SIZE];
                _longSize = Buffer.LONGREC_SIZE;
            }

            //
            // Swap the regular and long raw byte arrays
            //
            byte[] tempBytes = _bytes;
            int tempSize = _size;
            _bytes = _longBytes;
            _size = _longSize;
            _longBytes = tempBytes;
            _longSize = tempSize;
            _longMode = mode;

            if (mode) { // TODO - remove
                Debug.$assert(_bytes.length == Buffer.LONGREC_SIZE);
            } else {
                Debug.$assert(_longBytes.length == Buffer.LONGREC_SIZE);
            }
        }
    }

    private void reset() {
        _next = 0;
        _end = _size;
        _depth = 0;
        _level = 0;
        _serializedItemCount = 0;
        if (_endArray != null && _endArray.length > TOO_MANY_LEVELS_THRESHOLD) {
            _endArray = null;
        }
        _atomicIncrementArmed = false;
        releaseValueCache();
    }

    private void preparePut() {
        if (_depth == 0) {
            _size = 0;
            releaseValueCache();
        }
    }

    private void checkSize(int size) {
        if (_next + size != _size) {
            if (_next + size >= _size) {
                throw new MalformedValueException(
                        "Not enough bytes in Value at index=" + (_next - 1));
            }
            if (_depth == 0) {
                throw new MalformedValueException(
                        "Too many bytes in Value at index=" + (_next - 1));
            }
        }
    }

    private void checkArrayLength(int length, int offset, int arrayLength) {
        if (length < 0 || length + offset > arrayLength) {
            throw new IllegalArgumentException("Invalid length " + length);
        }
    }

    private void pushEnd(int end) {
        if (_endArray == null)
            _endArray = new int[10];
        else if (_level >= _endArray.length) {
            int[] temp = new int[_level * 2 + 1];
            System.arraycopy(_endArray, 0, temp, 0, _level);
            _endArray = temp;
        }
        _endArray[_level++] = end;
    }

    private void closeVariableLengthItem() {
        _next = _end;
        if (_level > 0)
            _end = popEnd();
        else
            _end = _size;
    }

    private int popEnd() {
        return _endArray[--_level];
    }

    private int type() {
        if (_next >= _end) {
            throw new ConversionException("No more data at index=" + _next
                    + " end=" + _end);
        }
        return _bytes[_next] & 0xFF;
    }

    private int nextType() {
        int type;
        if (_depth > 0) {
            while (_next >= _end && _level > 0) {
                _end = popEnd();
            }
            type = type();
            _next++;
            if (type >= SIZE1 && type <= SIZE5) {
                int size = decodeVariableLengthInt(type);
                pushEnd(_end);
                _end = _next + size;
                type = _bytes[_next++] & 0xFF;
            }
            if (type >= CLASS1 && type <= CLASS5) {
                type = decodeVariableLengthInt(type) + CLASS1;
            } else if (type == Buffer.LONGREC_TYPE)
                return -1;
        } else {
            _next = 0;
            _end = _size;
            _serializedItemCount = 0;
            releaseValueCache();
            if (_level != 0) {
                _level = 0;
                if (_endArray != null
                        && _endArray.length > TOO_MANY_LEVELS_THRESHOLD) {
                    _endArray = null;
                }
            }
            if (_size == 0) {
                throw new ConversionException("Value is undefined");
            }
            type = _bytes[_next++] & 0xFF;
            if (type >= CLASS1 && type <= CLASS5) {
                type = decodeVariableLengthInt(type) + CLASS1;
            } else if (type == Buffer.LONGREC_TYPE)
                return -1;
        }
        return type;
    }

    private int nextType(int expectedType) {
        int type = nextType();
        if (type == expectedType || type == TYPE_NULL)
            return type;

        throw new ConversionException("Expected a "
                + classForHandle(expectedType) + " but value is a "
                + classForHandle(type));
    }

    private int nextType(int expectedType1, int expectedType2) {
        int type = nextType();
        if (type == expectedType1 || type == expectedType2 || type == TYPE_NULL)
            return type;

        throw new ConversionException("Expected a "
                + classForHandle(expectedType1) + " but value is a "
                + classForHandle(type));
    }

    private Object getExpectedType(Class type) {
        Object object = get(null, null);
        if (object == null || type.isAssignableFrom(object.getClass())) {
            return object;
        } else {
            throw new ConversionException("Expected a " + type.getName()
                    + " but value is a " + object.getClass().getName());
        }
    }

    private void endVariableSizeItem(int itemSize) {
        if (_depth > 0) {
            _size += encodeVariableLengthInt(SIZE1, _size - itemSize, itemSize);
        }
    }

    private int encodeVariableLengthInt(int base, int index, int value) {
        if (Debug.ENABLED)
            Debug.$assert((base & 0x3F) == 0);

        int encodingSize = value < 0x00000010 ? 1 : value < 0x00001000 ? 2
                : value < 0x10000000 ? 3 : 5;

        if (_size > index) {
            ensureFit(encodingSize);
            System.arraycopy(_bytes, index, _bytes, index + encodingSize, _size
                    - index);
        }
        base |= ENCODED_SIZE_BITS[encodingSize];
        switch (encodingSize) {
        case 5:
            _bytes[index + 4] = (byte) (value & 0xFF);
            value >>>= 8;
            _bytes[index + 3] = (byte) (value & 0xFF);
            value >>>= 8;
            // intentionally falls through

        case 3:
            _bytes[index + 2] = (byte) (value & 0xFF);
            value >>>= 8;
            // intentionally falls through

        case 2:
            _bytes[index + 1] = (byte) (value & 0xFF);
            value >>>= 8;
            // intentionally falls through

        case 1:
            _bytes[index] = (byte) (base | (value & 0x0F));
            // intentionally falls through
        }
        return encodingSize;
    }

    private int decodeElementCount() {
        int base = _bytes[_next] & 0xFF;
        if (base < COUNT1 || base > COUNT5) {
            throw new MalformedValueException(
                    "Invalid element count introducer " + base + " at " + _next);
        }
        _next++;
        return decodeVariableLengthInt(base);
    }

    private int decodeVariableLengthInt(int base) {
        int result = base & 0x0F;
        base &= 0x30;
        switch (base) {
        case BASE5:
            result = result << 8 | (_bytes[_next++] & 0xFF);
            result = result << 8 | (_bytes[_next++] & 0xFF);
            // intentionally falls through

        case BASE3:
            result = result << 8 | (_bytes[_next++] & 0xFF);
            // intentionally falls through

        case BASE2:
            result = result << 8 | (_bytes[_next++] & 0xFF);
            // intentionally falls through

        case BASE1:
            break;
        }
        return result;
    }

    private void encodeClass(Class cl) {
        int classHandle = handleForClass(cl);
        if (classHandle < CLASS1) {
            ensureFit(1);
            _bytes[_size++] = (byte) classHandle;
        } else {
            ensureFit(5);
            _size += encodeVariableLengthInt(CLASS1, _size, classHandle
                    - CLASS1);
        }
    }

    private int handleForClass(Class cl) {
        if (cl.isArray()) {
            return CLASS_ARRAY;
        }
        int from, to;
        if (cl.isPrimitive()) {
            from = TYPE_NULL;
            to = TYPE_DOUBLE;
        } else {
            from = CLASS_BOOLEAN;
            to = CLASSES.length;
        }
        for (int index = from; index < to; index++) {
            if (CLASSES[index] == cl)
                return index;
        }
        return handleForIndexedClass(cl);
    }

    private int handleForIndexedClass(Class cl) {
        ClassInfo ci = _persistit.getClassIndex().lookupByClass(cl);
        if (ci != null) {
            return ci.getHandle();
        }
        throw new ConversionException("Class not mapped to handle "
                + cl.getName());
    }

    private Class classForHandle(int classHandle) {
        if (classHandle > 0 && classHandle < CLASSES.length
                && CLASSES[classHandle] != null) {
            return CLASSES[classHandle];
        } else if (classHandle == CLASS_ARRAY) {
            return Object[].class;
        } else if (classHandle == CLASS_MULTI_ARRAY) {
            return Object[][].class;
        }
        ClassInfo ci = classInfoForHandle(classHandle);
        return ci.getDescribedClass();
    }

    private ClassInfo classInfoForHandle(int classHandle) {
        ClassInfo classInfo = _persistit.getClassIndex().lookupByHandle(
                classHandle);
        if (classInfo == null) {
            throw new ConversionException("Unknown class handle " + classHandle);
        }
        return classInfo;
    }

    private boolean toBoolean(int index) {
        char ch = (char) (_bytes[index] & 0xFF);
        if (ch == TRUE_CHAR)
            return true;
        if (ch == FALSE_CHAR)
            return false;
        throw new ConversionException("Expected a Boolean " + " but value "
                + ch + " is neither 'T' nor 'F' at index=" + index);
    }

    private Object getMultiArray(Class prototype) {
        int start = _next;
        int type = nextType(CLASS_MULTI_ARRAY, CLASS_REREF);
        if (type == CLASS_REREF) {
            _next = start;
            Object array = get(null, null);
            if (array == null || array.getClass().isArray()) {
                return array;
            } else {
                throw new ConversionException(
                        "Referenced object is not an array");
            }
        }
        Object result;
        try {
            _depth++;
            int componentClassHandle = nextType();
            checkSize(1);
            int dimensions = _bytes[_next++] & 0xFF;
            if (prototype == null) {
                prototype = Array.newInstance(
                        classForHandle(componentClassHandle),
                        new int[dimensions]).getClass();
            }
            int length = decodeElementCount();
            result = Array.newInstance(prototype.getComponentType(), length);

            _serializedItemCount++;
            registerEncodedObject(result);

            Class componentType = prototype.getComponentType();
            if (componentType.getComponentType().isArray()) {
                for (int index = 0; index < length; index++) {
                    Array.set(result, index, getMultiArray(componentType));
                }
            } else {
                for (int index = 0; index < length; index++) {
                    Array.set(result, index, get(null, null));
                }
            }
        } finally {
            _depth--;
        }
        closeVariableLengthItem();
        return result;
    }

    void decodeAntiValue(Exchange exchange) throws InvalidKeyException {
        nextType(CLASS_ANTIVALUE);
        int length = _end - _next;
        int elisionCount = Util.getShort(_bytes, _next);
        AntiValue.fixupKeys(exchange, elisionCount, _bytes, _next + 2,
                length - 2);
        _next += length;
        closeVariableLengthItem();
    }

    /**
     * Return a <tt>java.io.ObjectOutputStream</tt> that writes bytes directly
     * into this Value. The implementation returned by this method overrides the
     * standard implementation to work correctly within the Persistit context.
     * See <a href="../../../Object_Serialization_Notes.html"> Notes on Object
     * Serialization</a> for details.
     * 
     * @return The <tt>ObjectOutputStream</tt>
     */
    public ObjectOutputStream getObjectOutputStream()
            throws ConversionException {
        if (_vos == null) {
            _vos = (ValueObjectOutputStream) getPrivilegedStream(true);
        }
        return _vos;
    }

    /**
     * Return a <tt>java.io.ObjectInputStream</tt> that reads bytes from this
     * Value. The implementation returned by this method overrides the standard
     * implementation to work correctly within the Persistit context. See <a
     * href="../../../Object_Serialization_Notes.html"> Notes on Object
     * Serialization</a> for details.
     * 
     * @return The <tt>ObjectInputStream</tt>
     */
    public ObjectInputStream getObjectInputStream() throws ConversionException {
        if (_vis == null) {
            _vis = (ValueObjectInputStream) getPrivilegedStream(false);
        }
        return _vis;
    }

    private Object getPrivilegedStream(final boolean output) {
        try {
            final Value value = this;
            return AccessController
                    .doPrivileged(new PrivilegedExceptionAction() {
                        public Object run() throws IOException {
                            if (output)
                                return new ValueObjectOutputStream(value);
                            else
                                return new ValueObjectInputStream(value);
                        }
                    });
        } catch (PrivilegedActionException pae) {
            throw new ConversionException("While creating "
                    + (output ? "ValueObjectOutputStream"
                            : "ValueObjectInputStream"), pae.getException());
        }
    }

    /**
     * An OutputStream that writes bytes into this Value. The resulting stream
     * can be wrapped in an ObjectOutputStream for serialization of Objects.
     */
    private static class ValueObjectOutputStream extends ObjectOutputStream {
        private Value _value;

        private ValueObjectOutputStream(final Value value) throws IOException {
            super();
            _value = value;
        }

        public void writeObjectOverride(Object object) {
            writeObject0(object, true);
        }

        public void write(int b) {
            _value.ensureFit(1);
            _value._bytes[_value._size++] = (byte) b;
        }

        public void write(byte[] bytes) {
            write(bytes, 0, bytes.length);
        }

        public void write(byte[] bytes, int offset, int size) {
            _value.ensureFit(size);
            Util.arraycopy(bytes, offset, _value._bytes, _value._size, size);
            _value._size += size;
        }

        public void writeBoolean(boolean v) {
            _value.put(v);
        }

        public void writeByte(int v) {
            _value.put((byte) v);
        }

        public void writeShort(int v) {
            _value.put((short) v);
        }

        public void writeChar(int v) {
            _value.put((char) v);
        }

        public void writeInt(int v) {
            _value.put(v);
        }

        public void writeLong(long v) {
            _value.put(v);
        }

        public void writeFloat(float v) {
            _value.put(v);
        }

        public void writeDouble(double v) {
            _value.put(v);
        }

        public void writeUnshared(Object object) {
            writeObject0(object, false);
        }

        public void writeUTF(String v) {
            _value.putUTF(v);
        }

        public void writeBytes(String s) {
            throw new UnsupportedOperationException("No writeBytes method");
        }

        public void writeChars(String s) {
            throw new UnsupportedOperationException("No writeChars method");
        }

        private void writeObject0(Object object, boolean shared) {
            boolean saveShared = _value._shared;
            _value._shared = shared;
            try {
                _value.put(object);
            } finally {
                _value._shared = saveShared;
            }
        }

        public void close() {
            // no effect - we can reuse this stream all we want.
        }

        public void defaultWriteObject() throws IOException {
            if (_value._currentCoder == null || _value._currentObject == null) {
                throw new NotActiveException("not in call to writeObject");
            }
            _value._currentCoder
                    .putDefaultFields(_value, _value._currentObject);
        }
    }

    /**
     * An InputStream that reads bytes from this Value. The resulting stream can
     * be wrapped in an ObjectOutputStream for serialization of Objects.
     */
    private static class ValueObjectInputStream extends ObjectInputStream {
        Value _value;

        private ValueObjectInputStream(final Value value) throws IOException {
            super();
            _value = value;
        }

        public Object readObjectOverride() {
            return _value.get();
        }

        public boolean markSupported() {
            return false;
        }

        public boolean readBoolean() {
            return _value.getBoolean();
        }

        public byte readByte() {
            return _value.getByte();
        }

        public int readUnsignedByte() {
            return _value.getByte() & 0xFF;
        }

        public short readShort() {
            return _value.getShort();
        }

        public int readUnsignedShort() {
            return _value.getShort() & 0xFFFF;
        }

        public char readChar() {
            return _value.getChar();
        }

        public int readInt() {
            return _value.getInt();
        }

        public long readLong() {
            return _value.getLong();
        }

        public float readFloat() {
            return _value.getFloat();
        }

        public double readDouble() {
            return _value.getDouble();
        }

        public String readUTF() {
            return _value.getString();
        }

        public void readFully(byte[] b) throws IOException {
            read(b, 0, b.length);
        }

        public void readFully(byte[] b, int offset, int length)
                throws IOException {
            read(b, offset, length);
        }

        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        public int read(byte[] b, int offset, int length) throws IOException {
            if (offset < 0 || offset + length > b.length) {
                throw new IndexOutOfBoundsException();
            }
            int sourceLength = _value._end - _value._next;
            if (length > sourceLength) {
                throw new IOException("Not enough bytes remaining in value");
            }
            System.arraycopy(_value._bytes, _value._next, b, offset, length);
            _value._next += length;
            return length;
        }

        public int skipBytes(int length) throws IOException {
            int sourceLength = _value._end - _value._next;
            if (length > sourceLength) {
                throw new IOException("Not enough bytes remaining in value");
            }
            _value._next += length;
            return length;
        }

        public String readLine() {
            throw new UnsupportedOperationException("No readLine method");
        }

        public void defaultReadObject() {
            if (_value._currentCoder == null || _value._currentObject == null) {
                throw new ConversionException("not in call to readObject");
            }
            _value._currentCoder.renderDefaultFields(_value,
                    _value._currentObject);
        }

    }

    /**
     * An ObjectOutputStream that reads bytes from this Value using standard
     * Java serialization. If constructed with a non-null ObjectStreamClass,
     * then readStreamHeader and readClassDescriptor are overridden to read
     * nothing because the SerialValueCoder will already have access to the
     * necessary information.
     */
    static class OldValueInputStream extends ObjectInputStream {
        Value _value;
        boolean _innerClassDescriptor;
        int _mark = -1;
        ObjectStreamClass _classDescriptor;

        OldValueInputStream(final Value value, ObjectStreamClass classDescriptor)
                throws IOException {
            this(value);
            if (classDescriptor == null) {
                throw new ConversionException("Null class descriptor");
            }
            _value = value;
            _classDescriptor = classDescriptor;
        }

        private OldValueInputStream(final Value value) throws IOException {
            super(new InputStream() {
                public int read() {
                    if (value._next < value._end) {
                        return value._bytes[value._next++] & 0xFF;
                    } else {
                        return -1;
                    }
                }

                public int read(byte[] bytes) {
                    return read(bytes, 0, bytes.length);
                }

                public int read(byte[] bytes, int offset, int size) {
                    if (value._next + size > value._end) {
                        size = value._end - value._next;
                    }
                    if (size <= 0) {
                        size = -1;
                    } else {
                        Util.arraycopy(value._bytes, value._next, bytes,
                                offset, size);

                        value._next += size;
                    }
                    return size;
                }

                public long skip(long lsize) {
                    int size = lsize > Integer.MAX_VALUE ? Integer.MAX_VALUE
                            : (int) lsize;
                    if (size < 0)
                        return 0;
                    if (value._next + size > value._end) {
                        size = value._end - value._next;
                    }
                    if (size < 0)
                        return 0;
                    value._next += size;
                    return size;
                }

                public int available() {
                    int available = value._end - value._next;
                    return available > 0 ? available : 0;
                }

            });
            _value = value;
        }

        protected final ObjectStreamClass readClassDescriptor()
                throws IOException, ClassNotFoundException {
            if (_classDescriptor == null) {
                return super.readClassDescriptor();
            } else if (_innerClassDescriptor) {
                int handle = readInt();
                ClassInfo classInfo = _value.classInfoForHandle(handle);
                ObjectStreamClass classDescriptor = null;
                if (classInfo != null) {
                    classDescriptor = classInfo.getClassDescriptor();
                }
                if (classDescriptor == null) {
                    throw new ConversionException("Unknown class handle "
                            + handle);
                }
                return classDescriptor;
            } else {
                _innerClassDescriptor = true;
                return _classDescriptor;
            }
        }

        protected final void readStreamHeader() throws IOException {
            if (_classDescriptor == null)
                super.readStreamHeader();
        }

        public void mark(int readLimit) {
            _mark = _value._next;
        }

        public void reset() throws IOException {
            if (_mark < 0) {
                throw new IOException("No mark");
            } else {
                _value._next = _mark;
            }
        }

        public boolean markSupported() {
            return true;
        }

        /**
         * Override the default implementation because we want to use the
         * application's ClassLoader, not necessarily the bootstrap loader.
         */
        protected Class resolveClass(ObjectStreamClass desc)
                throws ClassNotFoundException {
            String name = desc.getName();
            return Class.forName(name, false, Thread.currentThread()
                    .getContextClassLoader());
        }
    }

    /**
     * An ObjectOutputStream that writes bytes into this Value using standard
     * Java serialization. If constructed with a non-null ObjectStreamClass,
     * then writeStreamHeader and writeClassDescriptor are overridden to write
     * nothing because the SerialValueCoder will already have access to the
     * necessary information.
     */
    static class OldValueOutputStream extends ObjectOutputStream {
        Value _value;
        ObjectStreamClass _classDescriptor;
        boolean _innerClassDescriptor;

        OldValueOutputStream(final Value value,
                ObjectStreamClass classDescriptor) throws IOException {
            this(value);
            if (classDescriptor == null) {
                throw new ConversionException("Null class descriptor");
            }
            _classDescriptor = classDescriptor;
        }

        private OldValueOutputStream(final Value value) throws IOException {
            super(new OutputStream() {
                public void write(int b) {
                    value.ensureFit(1);
                    value._bytes[value._size++] = (byte) b;
                }

                public void write(byte[] bytes) {
                    write(bytes, 0, bytes.length);
                }

                public void write(byte[] bytes, int offset, int size) {
                    value.ensureFit(size);
                    Util.arraycopy(bytes, offset, value._bytes, value._size,
                            size);
                    value._size += size;
                }
            });
            _value = value;
        }

        protected final void writeClassDescriptor(
                ObjectStreamClass classDescriptor) throws IOException {
            if (_classDescriptor == null) {
                super.writeClassDescriptor(classDescriptor);
            } else if (_innerClassDescriptor) {
                Class clazz = classDescriptor.forClass();
                int handle = _value.handleForIndexedClass(clazz);
                writeInt(handle);
            } else {
                _innerClassDescriptor = true;
            }
        }

        protected final void writeStreamHeader() throws IOException {
            if (_classDescriptor == null)
                super.writeStreamHeader();
        }
    }

    DefaultValueCoder getCurrentCoder() {
        return _currentCoder;
    }

    Object getCurrentObject() {
        return _currentObject;
    }

    void setCurrentCoder(DefaultValueCoder coder) {
        _currentCoder = coder;
    }

    void setCurrentObject(Object object) {
        _currentObject = object;
    }

    private ValueCache getValueCache() {
        if (_valueCache == null && _valueCacheWeakRef != null) {
            _valueCache = (ValueCache) _valueCacheWeakRef.get();
        }
        if (_valueCache == null) {
            _valueCache = new ValueCache();
            _valueCacheWeakRef = new WeakReference(_valueCache);
        }
        return _valueCache;

    }

    private void releaseValueCache() {
        _serializedItemCount = 0;
        if (_valueCache != null) {
            _valueCache.clear();
            // Clear the hard reference.
            _valueCache = null;
        }
    }

    /**
     * Holds a collection of Object values associated with their position within
     * the serialized Value. Used only during get and put operations so that
     * reference graphs with cycles can be encoded and decoded properly. A
     * Persistit instance has a collection of ValueCache objects that get() and
     * put() operations allocate and relinquish when done.
     */
    static class ValueCache {
        private final static int INITIAL_SIZE = 256;

        /**
         * handle -> Object
         */
        Object[] _array = new Object[INITIAL_SIZE];
        /**
         * hashCode -> handle
         */
        int[] _hashTable = new int[INITIAL_SIZE];
        /**
         * handle -> handle of next object with same hash index
         */
        int[] _hashLink = new int[INITIAL_SIZE];

        boolean _isClear;

        boolean _hashed;

        ValueCache() {
            clear();
        }

        /**
         * Look up the handle for an Object that has already been stored in this
         * Value.
         * 
         * @param object
         * @return The handle, or -1 if the object has not been stored yet.
         */
        int lookup(Object object) {
            int index = hashIndex(object);
            if (index != -1) {
                for (int handle = _hashTable[index]; handle != -1; handle = _hashLink[handle]) {
                    if (_array[handle] == object)
                        return handle;
                }
            }
            return -1;
        }

        /**
         * Get the object stored with the supplied handle value.
         * 
         * @param handle
         * @return The object
         */
        Object get(int handle) {
            return _array[handle];
        }

        /**
         * Record the association between the supplied handle and object values.
         * Subsequent lookup and get operations will then be able to find this
         * object or fetch it by handle.
         * 
         * @param handle
         * @param object
         * @return previous handle, or -1 if none
         */
        void store(int handle, Object object) {
            if (handle >= _array.length)
                grow(1 + handle * 2);
            _array[handle] = object;
            _isClear = false;
        }

        int put(int handle, Object object) {
            int previous = lookup(object);
            if (previous != -1)
                return previous;
            if (handle >= _array.length)
                grow(1 + handle * 2);
            _array[handle] = object;
            int index = hashIndex(object);
            _hashLink[handle] = _hashTable[index];
            _hashTable[index] = handle;
            _isClear = false;
            _hashed = true;
            return -1;
        }

        /**
         * Enlarges the backing arrays to support more objects
         */
        void grow(int newSize) {
            Object[] temp = _array;
            _array = new Object[newSize];
            _hashTable = new int[newSize];
            _hashLink = new int[newSize];
            boolean hashed = _hashed;
            _isClear = false;
            clear();
            for (int index = 0; index < temp.length; index++) {
                if (hashed) {
                    put(index, temp[index]);
                } else {
                    store(index, temp[index]);
                }
            }
        }

        /**
         * Clears all object/handle associations.
         */
        void clear() {
            if (!_isClear) {
                for (int index = 0; index < _array.length; index++) {
                    _array[index] = null;
                }
                for (int index = 0; index < _hashTable.length; index++) {
                    _hashTable[index] = -1;
                }
                for (int index = 0; index < _hashLink.length; index++) {
                    _hashLink[index] = -1;
                }
                _isClear = true;
                _hashed = false;
            }
        }

        private int hashIndex(Object object) {
            return (System.identityHashCode(object) & 0x7FFFFFFF)
                    % _hashTable.length;
        }
    }

    private static class DisplayMarker {
        int _start;

        DisplayMarker(int start) {
            _start = start;
        }

        public String toString() {
            return "@" + Integer.toString(_start);
        }
    }
}
