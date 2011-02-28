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
 * An extended {@link KeyCoder} that can populate a supplied <code>Object</code>
 * with data that was formerly written to a {@link Key} by the
 * <code>KeyCoder</code> .
 * </p>
 * <p>
 * A <code>KeyRenderer</code> implements an additional method called
 * {@link #renderKeySegment} that populates a supplied target object rather than
 * creating a new object instance. For example, a {@link KeyStringCoder}, which
 * extends this interface, accepts a <code>StringBuilder</code> as a target.
 * Instead of decoding and creating a new <code>String</code> object,
 * <code>renderKeySegment</code> modifies the <code>StringBuilder</code> target
 * to contain the decoded String.
 * </p>
 * 
 * 
 * @version 1.0
 */
public interface KeyRenderer extends KeyCoder {

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
    public void renderKeySegment(Key key, Object target, Class clazz,
            CoderContext context) throws ConversionException;

}
