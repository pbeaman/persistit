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
    public void put(final Value value, final Object object, final CoderContext context) throws ConversionException {
        final String name = ((Enum) object).name();
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
    public Object get(final Value value, Class clazz, final CoderContext context) throws ConversionException {
        final String name = value.getString();
        while (clazz.getSuperclass() != Enum.class) {
            clazz = clazz.getSuperclass();
        }
        return Enum.valueOf(clazz, name);
    }
}
