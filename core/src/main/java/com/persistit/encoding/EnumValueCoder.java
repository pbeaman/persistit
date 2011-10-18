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

public class EnumValueCoder implements ValueCoder {

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
    @Override
    public void put(Value value, Object object, CoderContext context) throws ConversionException {
        String name = ((Enum) object).name();
        value.put(name);
    }

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
    @Override
    public Object get(Value value, Class clazz, CoderContext context) throws ConversionException {
        String name = value.getString();
        while (clazz.getSuperclass() != Enum.class) {
            clazz = clazz.getSuperclass();
        }
        return Enum.valueOf(clazz, name);
    }
}
