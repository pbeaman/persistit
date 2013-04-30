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
