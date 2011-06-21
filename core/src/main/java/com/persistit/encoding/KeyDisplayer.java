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

import com.persistit.Key;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * An extension of {@link KeyCoder} that adds the {@link #displayKeySegment}
 * method. This method populates a supplied <code>StringBuilder</code> with a
 * displayable, human-readable representation of the Object value that was
 * formerly written to a {@link Key} by this <code>KeyDisplayer</code>.
 * </p>
 * <p>
 * The {@link Key#toString toString} method and
 * {@link Key#decodeDisplayable(boolean, StringBuilder) decodeDisplayable}
 * methods of <code>Value</code> preferentially use the registered
 * <code>KeyDisplayer</code>, if present, to generate a String representation of
 * an object value encoded in a <code>Key</code>.
 * </p>
 * 
 * @version 1.0
 */
public interface KeyDisplayer extends KeyCoder {
    /**
     * <p>
     * Populates the state of the supplied target <code>Object</code> by
     * decoding the next key segment of the supplied <code>Key</code>. This
     * method will be called only if this <code>KeyRenderer</code> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the supplied <code>Class</code> value. In addition, Persistit will never
     * call this method to decode a value that was <code>null</code> when
     * written because null values are handled by built-in encoding logic.
     * </p>
     * 
     * @param key
     *            The <code>Key</code> from which interior fields of the object
     *            are to be retrieved
     * 
     * @param target
     *            An object into which the key segment is to be written
     * 
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     * 
     * @throws ConversionException
     */
    public void displayKeySegment(Key key, StringBuilder target, Class clazz, CoderContext context)
            throws ConversionException;

}
