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
 * An extension of {@link ValueCoder} that adds the {@link #display} method.
 * This method populates a supplied <tt>StringBuilder</tt> with a displayable,
 * human-readable representation of the Object value that was formerly written
 * to a {@link Value} by this <tt>ValueDisplayer</tt>.
 * </p>
 * <p>
 * The {@link Value#toString toString} method and
 * {@link Value#decodeDisplayable(boolean, StringBuilder) decodeDisplayable}
 * methods of <tt>Value</tt> preferentially use the registered
 * <tt>ValueDisplayer</tt>, if present, to generate a String representation of
 * an object value encoded in a <tt>Value</tt>.
 * </p>
 * 
 * @version 1.0
 */
public interface ValueDisplayer extends ValueCoder {
    /**
     * <p>
     * Writes a String representation of the value into a supplied
     * <tt>StringBuilder</tt>. This is used in utility programs to display
     * stored content without actually deserialized Objects represented by the
     * value.
     * </p>
     * <p>
     * This method will be called only if this <tt>ValueDisplayer</tt> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the supplied <tt>Class</tt> value. Persistit will never call this method
     * to decode a value that was <tt>null</tt> when written because null values
     * are handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> from which interior fields of the object
     *            are to be retrieved
     * 
     * @param target
     *            The <tt>StringBuilder</tt> into which the decoded value is to
     *            be written
     * 
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     * 
     * @throws ConversionException
     */
    public void display(Value value, StringBuilder target, Class clazz,
            CoderContext context) throws ConversionException;
}
