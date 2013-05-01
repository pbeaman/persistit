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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import com.persistit.DefaultValueCoder;
import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * A {@link ValueCoder} that uses standard Java serialization to store and
 * retrieve object values. This class is related to {@link DefaultValueCoder}.
 * When Persistit serializes or deserializes an object for which there is no
 * registered <code>ValueCoder</code>, it implicitly constructs either a
 * <code>DefaultValueCoder</code> or a <code>SerialValueCoder</code>, depending
 * on the value of the <code>serialOverride</code> property. See <a
 * href="../../../Object_Serialization_Notes.html"> Persistit Object
 * Serialization</a> for details.
 * </p>
 * <p>
 * This class creates a new <code>java.io.ObjectInputStream</code> each time it
 * is called upon to deserialize an instance of its client class, or a
 * <code>java.io.ObjectOutputStream</code> to serialize an instance. The format
 * of the stored data is specified by the <a href=
 * "http://java.sun.com/j2se/1.4.2/docs/guide/serialization/spec/serialTOC.html"
 * > Java Object Serialization Specification</a> with one exception: the class
 * descriptor is replaced by a handle; an association between handles and class
 * descriptors is maintained in a separate metadata tree. This avoids
 * redundantly storing class descriptors for multiple instance of the same
 * class.
 * </p>
 * 
 * @since 1.1
 * @version 1.1
 */
public final class SerialValueCoder implements ValueCoder {
    private final ObjectStreamClass _classDescriptor;

    public SerialValueCoder(final Class<?> clazz) {
        _classDescriptor = ObjectStreamClass.lookup(clazz);
        if (_classDescriptor == null) {
            throw new ConversionException("Not Serializable: " + clazz.getName());
        }
    }

    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <code>Value</code> using standard Java
     * serialization, and returns it. This method will be called only if this
     * <code>ValueCoder</code> has been registered with the current
     * {@link CoderManager} to encode objects having supplied <code>Class</code>
     * value. Persistit will never call this method to decode a value that was
     * <code>null</code> when written because null values are handled by
     * built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> from which interior fields of the
     *            object are to be retrieved
     * 
     * @param clazz
     *            The class of the object to be returned.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>. For <code>SerialValueCoder</code> this
     *            parameter is always ignored.
     * 
     * @return An <code>Object</code> having the same class as the suppled
     *         <code>clazz</code> parameter.
     * 
     * @throws ConversionException
     */
    @Override
    public Object get(final Value value, final Class<?> clazz, final CoderContext context) {
        try {
            final ObjectInputStream stream = value.oldValueInputStream(_classDescriptor);
            final Object object = stream.readObject();
            stream.close();
            return object;
        } catch (final Exception e) {
            throw new ConversionException(e);
        }
    }

    /**
     * <p>
     * Encodes the supplied <code>Object</code> into the supplied
     * <code>Value</code> using standard Java serialization. This method will be
     * called only if this <code>ValueCoder</code> has been registered with the
     * current {@link CoderManager} to encode objects having the class of the
     * supplied object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the
     * <code>Value</code> and its size should be updated to reflect the appended
     * key segment. Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to
     * manipulate the byte array directly. More commonly, the implementation of
     * this method will simply call the appropriate <code>put</code> methods to
     * write the interior field values into the <code>Value</code> object.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> to which the interior data of the
     *            supplied <code>Object</code> should be encoded
     * 
     * @param object
     *            The object value to encode. This parameter will never be
     *            <code>null</code> because Persistit encodes nulls with a
     *            built-in encoding.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>. For <code>SerialValueCoder</code> this
     *            parameter is always ignored.
     */
    @Override
    public void put(final Value value, final Object object, final CoderContext context) {
        try {
            final ObjectOutputStream stream = value.oldValueOutputStream(_classDescriptor);
            stream.writeObject(object);
            stream.close();
        } catch (final Exception e) {
            throw new ConversionException(e);
        }
    }
}
