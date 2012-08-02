/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.encoding;

import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * An extension of {@link ValueCoder} that adds the {@link #display} method.
 * This method populates a supplied <code>StringBuilder</code> with a
 * displayable, human-readable representation of the Object value that was
 * formerly written to a {@link Value} by this <code>ValueDisplayer</code>.
 * </p>
 * <p>
 * The {@link Value#toString toString} method and
 * {@link Value#decodeDisplayable(boolean, StringBuilder) decodeDisplayable}
 * methods of <code>Value</code> preferentially use the registered
 * <code>ValueDisplayer</code>, if present, to generate a String representation
 * of an object value encoded in a <code>Value</code>.
 * </p>
 * 
 * @version 1.0
 */
public interface ValueDisplayer extends ValueCoder {
    /**
     * <p>
     * Writes a String representation of the value into a supplied
     * <code>StringBuilder</code>. This is used in utility programs to display
     * stored content without actually deserialized Objects represented by the
     * value.
     * </p>
     * <p>
     * This method will be called only if this <code>ValueDisplayer</code> has
     * been registered with the current {@link CoderManager} to encode objects
     * having the supplied <code>Class</code> value. Persistit will never call
     * this method to decode a value that was <code>null</code> when written
     * because null values are handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> from which interior fields of the
     *            object are to be retrieved
     * 
     * @param target
     *            The <code>StringBuilder</code> into which the decoded value is
     *            to be written
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
    public void display(Value value, StringBuilder target, Class<?> clazz, CoderContext context)
            throws ConversionException;
}
