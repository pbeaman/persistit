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

package com.persistit.encoding;

import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * Interface for specialized encoding and decoding of <tt>Object</tt>s of
 * arbitrary type in a {@link Value}. Persistit contains built-in logic to
 * encode objects of certain classes. For other classes, if there is no
 * registered <tt>ValueCoder</tt>, Persistit uses standard object serializaion
 * to generate a byte array that represents the state of that object in the
 * database.
 * </p>
 * <p>
 * By implementing and registering a <tt>ValueCoder</tt> with the current
 * {@link CoderManager} you can override this default encoding. Typically the
 * customized encoding of an object will be much shorter than that produced by
 * serialization because the customized encoding classes by their Persistit
 * class handles (see {@link com.persistit.ClassIndex}) rather than their names.
 * In addition, standard serialization serializes the entire graph of other
 * objects that may be directly or indirectly referred to by the object being
 * made persistent. This behavior may result in storing much more data than is
 * required, and more importantly, may result in incorrect semantics. A
 * customized <tt>ValueEncoder</tt> can handle persistence of fields that refer
 * to other objects in a manner that is semantically appropriate for the
 * specific object. Note that if a class has a <tt>ValueCoder</tt>, it can be
 * stored in Persistit even if it does not implement
 * <tt>java.io.Serializable</tt>.
 * </p>
 * <p>
 * A <tt>ValueCoder</tt> implements methods to convert an object of some class
 * to an array of bytes and back again. Typically the implementation of a
 * <tt>ValueCoder</tt> will simply invoke the <tt>Value</tt>'s <tt>put</tt>
 * methods to write the fields into the <tt>Value</tt>. For example, the
 * following code defines a class, and a <tt>ValueCoder</tt> to store and
 * retrieve it.
 * </p>
 * <blockquote>
 * 
 * <pre>
 * class MyClass {
 *     int id;
 *     int anotherInt;
 *     String someString;
 *     MyClass anotherMyClass;
 * }
 * 
 * class MyClassValueCoder implements ValueCoder {
 *     public void put(Value value, Object object, CoderContext context) {
 *         MyClass mc = (MyClass) object;
 *         value.put(mc.id);
 *         value.put(mc.anotherInt);
 *         value.put(mc.someString);
 *         value.put(mc.anotherMyClass == null ? -1 : mc.anotherMyClass.id);
 *     }
 * 
 *     public Object get(Value value, Class clazz, CoderContext context) {
 *         MyClass mc = new MyClass();
 *         mc.id = value.getInt();
 *         mc.anotherInt = value.getInt();
 *         mc.someString = value.getString();
 *         mc.anotherMyClass = lookupMyClass(value.getInt());
 *         return mc;
 *     }
 * }
 * </pre>
 * 
 * </blockquote>
 * <p>
 * The application should register this <tt>ValueCoder</tt> before attempting to
 * invoke {@link Value#put(Object)} on an object of class <tt>MyClass</tt> or to
 * retrieve an object value using {@link Value#get()} that will resolve to a
 * <tt>MyClass</tt>. Generally this means that applications should register all
 * <tt>ValueCoder</tt>s and <tt>KeyCoder</tt>s during application startup,
 * immediately after invoking {@link com.persistit.Persistit#initialize}. For
 * example: <blockquote>
 * 
 * <pre>
 *      ...
 *  try
 *  {
 *      Persistit.initialize();
 *      CoderManager cm = Persistit.getInstance().getCoderManager();
 *      cm.registerValueCoder(new MyClassValueCoder());
 *      cm.registerValueCoder(new MyClassValueCoder2());
 *      ...
 *      cm.registerKeyCoder(new MyClassKeyCoder());
 *      ...
 *  }
 *  catch (PersistitException e)
 *  {
 *      ...
 *  }
 * </pre>
 * 
 * </blockquote>
 * </p>
 * 
 * @version 1.0
 */
public interface ValueCoder {
    /**
     * <p>
     * Encodes the supplied <tt>Object</tt> into the supplied <tt>Value</tt>.
     * This method will be called only if this <tt>ValueCoder</tt> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the class of the supplied object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the
     * <tt>Value</tt> and its size should be updated to reflect the serialized
     * object. Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to
     * manipulate the byte array directly. More commonly, the implementation of
     * this method will simply call the appropriate <tt>put</tt> methods to
     * write the interior field values into the <tt>Value</tt> object.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> to which the interior data of the supplied
     *            <tt>Object</tt> should be encoded
     * @param object
     *            The object value to encode. This parameter will never be
     *            <tt>null</tt> because Persistit encodes nulls with a built-in
     *            encoding.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     */
    public void put(Value value, Object object, CoderContext context)
            throws ConversionException;

    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <tt>Value</tt>, and returns it. This method will be
     * called only if this <tt>ValueCoder</tt> has been registered with the
     * current {@link CoderManager} to encode objects having supplied
     * <tt>Class</tt> value. Persistit will never call this method to decode a
     * value that was <tt>null</tt> when written because null values are handled
     * by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> from which interior fields of the object
     *            are to be retrieved
     * @param clazz
     *            The class of the object to be returned.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     * @return An <tt>Object</tt> having the same class as the suppled
     *         <tt>clazz</tt> parameter.
     * @throws ConversionException
     */
    public Object get(Value value, Class clazz, CoderContext context)
            throws ConversionException;

}
