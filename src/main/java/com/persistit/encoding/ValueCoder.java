/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.encoding;

import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * Interface for specialized encoding and decoding of <code>Object</code>s of
 * arbitrary type in a {@link Value}. Persistit contains built-in logic to
 * encode objects of certain classes. For other classes, if there is no
 * registered <code>ValueCoder</code>, Persistit uses standard object
 * serialization to generate a byte array that represents the state of that
 * object in the database.
 * </p>
 * <p>
 * By implementing and registering a <code>ValueCoder</code> with the current
 * {@link CoderManager} you can override this default encoding. Typically the
 * customized encoding of an object will be much shorter than that produced by
 * serialization because the customized encoding classes by their Persistit
 * class handles (see {@link com.persistit.ClassIndex}) rather than their names.
 * In addition, standard serialization serializes the entire graph of other
 * objects that may be directly or indirectly referred to by the object being
 * made persistent. This behavior may result in storing much more data than is
 * required, and more importantly, may result in incorrect semantics. A
 * customized <code>ValueEncoder</code> can handle persistence of fields that
 * refer to other objects in a manner that is semantically appropriate for the
 * specific object. Note that if a class has a <code>ValueCoder</code>, it can
 * be stored in Persistit even if it does not implement
 * <code>java.io.Serializable</code>.
 * </p>
 * <p>
 * A <code>ValueCoder</code> implements methods to convert an object of some
 * class to an array of bytes and back again. Typically the implementation of a
 * <code>ValueCoder</code> will simply invoke the <code>Value</code>'s
 * <code>put</code> methods to write the fields into the <code>Value</code>. For
 * example, the following code defines a class, and a <code>ValueCoder</code> to
 * store and retrieve it.
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
 * The application should register this <code>ValueCoder</code> before
 * attempting to invoke {@link Value#put(Object)} on an object of class
 * <code>MyClass</code> or to retrieve an object value using {@link Value#get()}
 * that will resolve to a <code>MyClass</code>. Generally this means that
 * applications should register all <code>ValueCoder</code>s and
 * <code>KeyCoder</code>s during application startup, immediately after invoking
 * {@link com.persistit.Persistit#initialize}. For example: <blockquote>
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
     * Encodes the supplied <code>Object</code> into the supplied
     * <code>Value</code>. This method will be called only if this
     * <code>ValueCoder</code> has been registered with the current
     * {@link CoderManager} to encode objects having the class of the supplied
     * object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the
     * <code>Value</code> and its size should be updated to reflect the
     * serialized object. Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to
     * manipulate the byte array directly. More commonly, the implementation of
     * this method will simply call the appropriate <code>put</code> methods to
     * write the interior field values into the <code>Value</code> object.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> to which the interior data of the
     *            supplied <code>Object</code> should be encoded
     * @param object
     *            The object value to encode. This parameter will never be
     *            <code>null</code> because Persistit encodes nulls with a
     *            built-in encoding.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     */
    public void put(Value value, Object object, CoderContext context) throws ConversionException;

    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <code>Value</code>, and returns it. This method
     * will be called only if this <code>ValueCoder</code> has been registered
     * with the current {@link CoderManager} to encode objects having supplied
     * <code>Class</code> value. Persistit will never call this method to decode
     * a value that was <code>null</code> when written because null values are
     * handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> from which interior fields of the
     *            object are to be retrieved
     * @param clazz
     *            The class of the object to be returned.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     * @return An <code>Object</code> having the same class as the suppled
     *         <code>clazz</code> parameter.
     * @throws ConversionException
     */
    public Object get(Value value, Class<?> clazz, CoderContext context) throws ConversionException;

}
