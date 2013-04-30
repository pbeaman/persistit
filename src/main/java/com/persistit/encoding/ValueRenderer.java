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
 * An extended {@link ValueCoder} that can populate a supplied
 * <code>Object</code> with data that was formerly written to a {@link Value} by
 * the <code>ValueCoder</code>.
 * </p>
 * <p>
 * A <code>ValueRenderer</code> implements an additional method called
 * {@link #render} that populates a supplied target object rather than creating
 * a new object instance. The application provides the mutable target object to
 * {@link Value#get(Object)}.
 * </p>
 * <p>
 * A <code>ValueRenderer</code> can implement an application-level contract to
 * recover a limited set of fields from an encoded <code>Value</code> or to
 * populate a generic target object that might be suitable for utility display
 * purposes. For example, suppose there is a complex <code>Person</code> class
 * with many fields that refer to other objects. Deserializing and populating
 * all the fields of such a <code>Person</code> object might not be appropriate
 * in an area of the application that merely needs to display a list of names.
 * To avoid completely deserializing numerous <code>Person</code> objects, the
 * application could register a <code>ValueRenderer</code> implementation that
 * knows how to decode a serialized <code>Person</code> into a
 * <code>PersonListData</code> object. To do so it would {@link Value#get} the
 * desired interior fields of the encoded <code>Person</code> and
 * {@link Value#skip} the other fields.
 * </p>
 * 
 * 
 * @version 1.0
 */
public interface ValueRenderer extends ValueCoder {
    /**
     * <p>
     * Populates the state of the supplied (mutable) target <code>Object</code>
     * by decoding the supplied <code>Value</code>. This method will be called
     * only if this <code>ValueRenderer</code> has been registered with the
     * current {@link CoderManager} to encode objects having the supplied
     * <code>Class</code> value. Persistit will never call this method to decode
     * a value that was <code>null</code> when written because null values are
     * handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> from which interior fields of the
     *            object are to be retrieved
     * 
     * @param target
     *            The object into which the decoded value is to be written
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
    public void render(Value value, Object target, Class<?> clazz, CoderContext context) throws ConversionException;
}
