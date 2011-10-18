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
    private ObjectStreamClass _classDescriptor;

    public SerialValueCoder(Class<?> clazz) {
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
    public Object get(Value value, Class<?> clazz, CoderContext context) {
        try {
            ObjectInputStream stream = value.oldValueInputStream(_classDescriptor);
            Object object = stream.readObject();
            stream.close();
            return object;
        } catch (Exception e) {
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
    public void put(Value value, Object object, CoderContext context) {
        try {
            ObjectOutputStream stream = value.oldValueOutputStream(_classDescriptor);
            stream.writeObject(object);
            stream.close();
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }
}
