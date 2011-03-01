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
 * A specialized KeyCoder for String encoding/decoding implementations. An
 * implementation of this class may provide specialized logic for translating a
 * <code>String</code>s or various other string representation classes (e.g.,
 * <code>StringBuilder</code>, <code>StringBuilder</code> and
 * <code>CharSequence</code>) into a subarray of bytes and back again. The
 * default key encoding mechanism converts a string to UTF-8.
 * </p>
 * <p>
 * Custom <code>KeyStringEncoder</code>s can implement a specialized collation
 * sequence for strings; in particular, a locale-specific
 * <code>KeyStringEncoder</code> should be used to implement correct
 * language-specific alphabetical collation of strings. </b>
 * <p>
 * By default, {@link Key} uses the registered <code>KeyStringCoder</code> to
 * encode and decode <code>String</code> and <code>StringBuilder</code> objects.
 * Because <code>KeyStringCoder</code> extends <code>KeyCoder</code>, you may
 * register a <code>KeyStringEncoder</code> to handle encoding and decoding for
 * any other type of object provided that the <code>KeyStringCoder</code>'s
 * implementation handles that type. Recommended practice is to implement and
 * register <code>StringBuilder</code> as an additional registered type for JDK
 * 1.5 systems.
 * </p>
 * <p>
 * In Persistit, all strings are represented internally as a single type.
 * Therefore if you append a <code>StringBuilder</code> to a <code>Key</code>,
 * you can decode the value as a <code>Sting</code> rather than as a
 * <code>StringBuilder</code>. This conversion is similar to "autoboxing", where
 * primitive values are automatically interchangeable with their wrapper types.
 * By registering additional classes that reasonably implement string values,
 * the application can extend this interchangeability among types.
 * </p>
 * 
 * @version 1.0
 */
public interface KeyStringCoder extends KeyRenderer {
    /**
     * <p>
     * Append the encoded form of the <code>String</code> into the
     * <code>Key</code>. Typically the implementation will get and manipulate
     * the backing byte array of the <code>Key</code> using
     * {@link Key#getEncodedBytes}. The implementation must append bytes
     * starting at the location returned by {@link Key#getEncodedSize} and must
     * modify the raw size by calling {@link Key#setEncodedSize} when done. The
     * implementation must not modify bytes a offsets smaller than the starting
     * location.
     * </p>
     * <p>
     * The resulting key will be collated with respect to other keys by
     * performing a byte-wise, <i>unsigned</i> comparison. Thus the byte value
     * -1 (0xFF) collates <i>after</i> the byte values 1 (0x01) and -2 (0xFE).
     * </p>
     * <p>
     * After calling this method to encode a String, Persistit will internally
     * modify all byte values of 0 or 1 to the two-byte sequences (0x01, 0x20)
     * and (0x01, 0x21), and will then append a zero byte as the end marker for
     * the key segment. Prior to calling {@link #decodeKeySegment}, Persistit
     * will convert the array back to its original form. This transformation
     * preserves correct collation semantics of keys written by this key coder
     * with respect to other Encoded objects. See {@link CoderManager} for
     * information on key ordering among objects of different classes.
     * </p>
     * 
     * @param key
     *            The <code>Key</code> to which the encoded string should be
     *            appended.
     * @param string
     *            The string to encode, provided by any of several classes that
     *            reasonably represent a String. At minimum, the
     *            <code>KeyStringEncoder</code> must handle object of type
     *            <code>String</code>, <code>StringBuilder</code>, and for JDK
     *            1.5 installations, <code>StringBuiler</code>. (If you are
     *            using a JVM compatible with JDK 1.4 or above, you can
     *            accomplish this by casting the object as a
     *            <code>CharSequence</code>.)
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     */
    public void appendKeySegment(Key key, Object string, CoderContext context)
            throws ConversionException;

    /**
     * <p>
     * Decode the encoded form of a String from the backing byte array of a
     * <code>Key</code> into an object whose class is specified by the supplied
     * <code>Class</code> parameter.
     * </p>
     * <p>
     * This method is not called by the current implementation of
     * {@link com.persistit.Key} to decode <code>String</code>s. It is defined
     * for compatibility with {@link KeyCoder}.
     * </p>
     * 
     * @param key
     *            The <code>Key</code>
     * @param clazz
     *            The class of the object to be returned.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     */
    public Object decodeKeySegment(Key key, Class clazz, CoderContext context)
            throws ConversionException;

    /**
     * <p>
     * Decode the encoded form of a String from the backing byte array of a
     * <code>Key</code> into an instance of a supplied class that reasonably
     * represents string values, such as a <code>StringBuilder</code> or
     * <code>StringBuilder</code> (for JDK 1.5 and above).
     * </p>
     * 
     * @param key
     *            The <code>Key</code>
     * @param target
     *            A mutable object that accepts a string value. Implementations
     *            of this interface must accept a <code>StringBuilder</code> as
     *            a target.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value. For <code>KeyStringCoder</code>, this will always be
     *            <code>String.class</code>.
     */
    public void renderKeySegment(Key key, Object target, Class clazz,
            CoderContext context) throws ConversionException;
}
